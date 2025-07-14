import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime as dt

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from loguru import logger
from redis import RedisError
from redis.asyncio import Redis

from api.v1.router import router as v1_router
from settings import settings
from utils.auth_utils import renew_token, store_new_token
from utils.redis_utils import cleanup_dlq, get_available_handlers

if settings.GP_SINGLE_CONNECTION:
    from utils.single_connection_gp_utils import run_query
else:
    from utils.gp_utils import run_query

FRONTEND_URL = f'http://{settings.HOST}:{settings.FRONTEND_PORT}'

logger.add('backend.log',
           level=settings.LOGLEVEL,
           format='{time} | {level} | {name}:{function}:{line} - {message}',
           rotation='10 MB')


async def scan_redis(filename: str, interval: float, pattern: str):
    # TODO: review logic
    # TODO: use fastapi state connection and schemas
    # TODO: move to gp_utils or cold_store_utils
    # TODO: add async fs support?
    r = Redis(host=settings.HOST,
              port=settings.REDIS_PORT,
              db=settings.REDIS_DB)

    while True:
        start_time = dt.now()
        data = {}

        async for key in r.scan_iter(pattern):
            # TODO: have to get decoded values, remove overhead
            key_str = key.decode('utf-8')
            value = await r.get(key)

            try:
                try:
                    # TODO: have to get decoded values, remove overhead
                    value_str = value.decode('utf-8')
                    try:
                        value_data = json.loads(value_str)
                    except json.JSONDecodeError:
                        value_data = value_str
                except (AttributeError, UnicodeDecodeError):
                    value_data = str(value)

                data[key_str] = value_data
            except Exception as e:
                logger.error(f'‼️ Error processing key {key_str}: {e}')
                continue

        existing_data = {}

        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Ошибка при чтении файла: {e}")

        merged_data = {**existing_data, **data}
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)
            f.flush()

        # Ждём до следующего обновления
        elapsed = (dt.now() - start_time).total_seconds()
        await asyncio.sleep(max(0, interval - elapsed))


async def scan_redis_to_greenplum(table_name: str, interval: float, pattern: str):
    # TODO: review logic
    # TODO: use fastapi state connection and schemas
    r = Redis(
        host=settings.HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB
    )
    # FIXME handler_id and version
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {settings.GP_SCHEMA}.{table_name} (
        task_id TEXT,
        prompt TEXT,
        status TEXT,
        task_type TEXT,
        user_id TEXT,
        short_task_id TEXT,
        queued_at TIMESTAMP WITH TIME ZONE,
        finished_at TIMESTAMP WITH TIME ZONE,
        context TEXT,
        retries INTEGER,
        start_position INTEGER,
        current_position INTEGER,
        result_text TEXT,
        result_relevant_docs JSONB,
        error_text TEXT,
        error_relevant_docs JSONB,
        feedback TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    ) DISTRIBUTED RANDOMLY;
    """

    try:
        # Создаем таблицу
        await run_query(create_table_query)
        logger.info(f'ℹ️ Table {settings.GP_SCHEMA}.{table_name} '
            f'created or already exists')

        # Создаем индексы
        try:
            await run_query(f'CREATE INDEX idx_{table_name}_task_id ON {settings.GP_SCHEMA}.{table_name} (task_id)')
        except Exception as e:
            logger.warning(f'⚠️ Possible duplicate index: {e}')

        try:
            await run_query(f'CREATE INDEX idx_{table_name}_status ON {settings.GP_SCHEMA}.{table_name} (status)')
        except Exception as e:
            logger.warning(f'⚠️ Possible duplicate index: {e}')

    except Exception as e:
        logger.error(f'‼️ Error creating table: {e}')
        raise

    def parse_datetime(dt_str):
        """Преобразует строку даты в объект datetime"""
        if not dt_str:
            return None
        try:
            return dt.fromisoformat(dt_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                return dt.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                logger.warning(f'⚠️ Failed to parse datetime "{dt_str}"')
                return None

    def extract_json_fields(data: dict):
        """Извлекает данные из JSON-структур"""
        result = data.get('result', {})
        error = data.get('error', {})
        feedback_data = data.get('feedback', {})

        return (
            result.get('text', ''),
            json.dumps(result.get('relevant_docs', [])),
            error.get('text', ''),
            json.dumps(error.get('relevant_docs', [])),
            feedback_data.get('feedback', '')
        )

    async def should_update_record(task_id: str, new_status: str) -> bool:
        """Проверяет, нужно ли обновлять запись"""
        try:
            check_query = f"""
            SELECT status FROM {settings.GP_SCHEMA}.{table_name}
            WHERE task_id = $1
            LIMIT 1;
            """
            result = await run_query(check_query, (task_id,))
            return not result or result[0].get('status') != 'completed' or result[0].get('feedback') != 'neutral'
        except Exception as e:
            logger.error(f'‼️ Error checking record: {e}')
            return True

    while True:
        start_time = dt.now()
        stats = {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        try:
            async for key in r.scan_iter(match=pattern):
                try:
                    key_str = key.decode('utf-8')
                    if not key_str.startswith('task:'):
                        continue

                    value = await r.get(key)
                    if not value:
                        continue

                    try:
                        task_data = json.loads(value.decode('utf-8'))
                    except Exception as e:
                        logger.error(f'‼️ Invalid JSON for key {key_str}: {e}')
                        stats['errors'] += 1
                        continue

                    task_id = task_data.get('task_id')
                    if not task_id:
                        logger.warning(f'⚠️ Missing task_id for key {key_str}')
                        stats['errors'] += 1
                        continue

                    current_status = task_data.get('status')
                    if not await should_update_record(task_id, current_status):
                        stats['skipped'] += 1
                        continue

                    # Подготовка данных
                    queued_at = parse_datetime(task_data.get('queued_at'))
                    finished_at = parse_datetime(task_data.get('finished_at'))
                    result_fields = extract_json_fields(task_data)

                    # Удаляем старую версию если существует
                    await run_query(
                        f'DELETE FROM {settings.GP_SCHEMA}.{table_name} WHERE task_id = $1;',
                        (task_id,)
                    )

                    # Вставляем новую запись
                    await run_query( # FIXME handler_id, version
                        f"""
                        INSERT INTO {settings.GP_SCHEMA}.{table_name} (
                            task_id, prompt, status, task_type, user_id,
                            short_task_id, queued_at, finished_at, context,
                            retries, start_position, current_position,
                            result_text, result_relevant_docs,
                            error_text, error_relevant_docs, feedback
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);
                        """,
                        (
                            task_id,
                            task_data.get('prompt'),
                            current_status,
                            task_data.get('task_type'), # FIXME handler_id, version
                            task_data.get('user_id'),
                            task_data.get('short_task_id'),
                            queued_at,
                            finished_at,
                            task_data.get('context'),
                            task_data.get('retries', 0),
                            task_data.get('start_position', 0),
                            task_data.get('current_position', 0),
                            *result_fields
                        )
                    )

                    stats['updated' if await should_update_record(task_id, current_status) else 'new'] += 1

                except Exception as e:
                    logger.error(f'‼️ Error processing key {key_str}: {e}')
                    stats['errors'] += 1
                    continue

            logger.info(
                f'ℹ️ Processed stats: {stats["new"]} new, '
                f'{stats["updated"]} updated, '
                f'{stats["skipped"]} skipped, {stats["errors"]} errors'
            )

        except Exception as e:
            logger.error(f'‼️ Redis scan error: {e}')
            await asyncio.sleep(5)
            continue

        elapsed = (dt.now() - start_time).total_seconds()
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        fastapi_app.state.redis = Redis(host=settings.HOST,
                                        port=settings.REDIS_PORT,
                                        db=settings.REDIS_DB,
                                        decode_responses=True)

        if settings.USE_GP_COLD_STORE:
            cool_save_to_gp = asyncio.create_task(scan_redis_to_greenplum(
                table_name=settings.GP_TABLE, interval=60, pattern='task:*'))
        write_redis_log = asyncio.create_task(scan_redis(
            filename='redis_log.json', interval=5.0, pattern='task:*'))

    except RedisError as e:
        logger.error(f'‼️ Redis error: {e}')
        raise
    fastapi_app.state.available_handlers = {}
    fastapi_app.state.handlers_configs = {}
    asyncio.create_task(get_available_handlers(fastapi_app))
    asyncio.create_task(cleanup_dlq(fastapi_app.state.redis))

    yield
    await fastapi_app.state.redis.delete('available_handlers')
    if settings.USE_GP_COLD_STORE:
        await cool_save_to_gp
    await write_redis_log
    await fastapi_app.state.redis.aclose()


app = FastAPI(debug=settings.DEBUG, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def refresh_jwt_token(request: Request, call_next):
    secret_key = settings.SECRET_KEY
    algorithm = settings.JWT_ALGORITHM
    redis: Redis = app.state.redis
    token = request.cookies.get('access_token')
    if token:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[algorithm])
            user_id = payload.get('sub')
            if user_id and await redis.get(f'token:{token}') == user_id:
                await redis.expire(f'token:{token}', 90 * 24 * 3600)
        except JWTError:
            pass

    response = await call_next(request)
    return response


@app.middleware('http')
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000

    log_message = (
        f'ℹ️ {request.client.host}:{request.client.port} - '
        f'{request.method} {request.url.path} '
        f'HTTP/{request.scope["http_version"]} '
        f'{response.status_code}'
        f' | {process_time:.2f} ms'
    )
    logger.info(log_message)
    return response


app.include_router(v1_router)


@app.get('/')
async def root(request: Request, response: Response):
    redis: Redis = app.state.redis
    token = request.cookies.get('access_token')

    if not token:
        token = await store_new_token(redis)
        new_user = True
    else:
        try:
            await renew_token(token, redis)
            new_user = False
        except HTTPException:
            new_user = True
            token = await store_new_token(redis)

    response.set_cookie(
        key='access_token',
        value=token,
        httponly=True,
        secure=False,
        samesite='lax',
        max_age=90 * 24 * 3600,
    )
    if new_user:
        response.status_code = 303
        response.headers['location'] = '/'


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,
                host=settings.HOST,
                port=settings.BACKEND_PORT,
                reload=settings.DEBUG,
                log_config=None,
                access_log=False)
