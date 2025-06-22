import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from loguru import logger
from redis import RedisError
from redis.asyncio import Redis

from api.v1.router import router as v1_router
from settings import settings
from utils.auth_utils import renew_token, store_new_token
from utils.redis_utils import update_handlers, cleanup_dlq

FRONTEND_URL = f'http://{settings.HOST}:{settings.FRONTEND_PORT}'

logger.add('backend.log',
           level=settings.LOGLEVEL,
           format='{time} | {level} | {name}:{function}:{line} - {message}',
           rotation='10 MB')


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        fastapi_app.state.redis = Redis(host=settings.HOST,
                                        port=settings.REDIS_PORT,
                                        db=settings.REDIS_DB,
                                        decode_responses=True)
    except RedisError as e:
        logger.error(f'Ошибка redis: {e}')
        raise
    fastapi_app.state.handlers = []
    asyncio.create_task(update_handlers(fastapi_app))
    asyncio.create_task(cleanup_dlq(fastapi_app.state.redis))

    yield
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
        f'{request.client.host}:{request.client.port} - '
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,
                host=settings.HOST,
                port=settings.BACKEND_PORT,
                reload=settings.DEBUG,
                log_config=None,
                access_log=False)
