import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from loguru import logger
from pydantic import ValidationError
from redis.asyncio import Redis

from schemas.task import Task, TaskCreate, TaskStatus
from settings import settings


# TODO: add redis pool init here for Depends


async def update_task_position(task_id: str, redis: Redis):
    all_tasks = await redis.lrange('task_queue', 0, -1)
    all_tasks.reverse()
    try:
        current_pos = all_tasks.index(task_id) + 1
    except ValueError:
        pending_tasks = await redis.lrange('pending_task_queue', 0, -1)
        if task_id in pending_tasks:
            current_pos = -1
        else:
            current_pos = 0
    task_data = await redis.get(f'task:{task_id}')
    if task_data:
        task = Task.model_validate_json(task_data)
        task.current_position = current_pos
        await redis.setex(
            f'task:{task_id}',
            settings.redis_store_seconds,
            task.model_dump_json())


async def set_task_to_queue(user_id: str,
                            task: TaskCreate,
                            fastapi_app: FastAPI) -> tuple[str, str]:
    redis: Redis = fastapi_app.state.redis
    task_id = str(uuid.uuid4())
    short_id = generate_short_id(task_id, user_id)
    task_to_enqueue = Task(
        task_id=task_id,
        prompt=task.prompt.strip(),
        handler_id=task.handler_id,
        user_id=user_id,
        short_task_id=short_id,
        queued_at=datetime.now(timezone.utc).isoformat(),
    )

    available_handlers = fastapi_app.state.available_handlers
    if task.handler_id not in available_handlers:
        task_to_enqueue.start_position = -1
        task_to_enqueue.status = TaskStatus.PENDING
        async with redis.pipeline() as pipe:
            await redis.setex(
                f'task:{task_id}',
                settings.redis_store_seconds,
                task_to_enqueue.model_dump_json())
            await pipe.lpush('pending_task_queue', task_id)
            await pipe.lpush(f'pending_task_queue:{task.handler_id}', task_id)
            await pipe.execute()

    else:
        # can set wrong task start_position
        # we can`t get position and set task with one transaction
        current_queue_length = await redis.llen('task_queue')
        start_position = current_queue_length + 1
        task_to_enqueue.start_position = start_position
        task_to_enqueue.status = TaskStatus.QUEUED
        async with redis.pipeline() as pipe:
            await redis.setex(
                f'task:{task_id}',
                settings.redis_store_seconds,
                task_to_enqueue.model_dump_json())
            await pipe.lpush('task_queue', task_id)
            await pipe.lpush(f'task_queue:{task.handler_id}', task_id)
            await pipe.execute()

    return task_id, short_id


def generate_short_id(
        _task_id: str, _user_id: str, length: int = 3) -> str:
    combined = f'{_task_id}:{_user_id}'.encode()
    hash_bytes = hashlib.blake2b(combined, digest_size=4).digest()
    hash_num = int.from_bytes(hash_bytes, byteorder='big')

    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    for _ in range(length):
        hash_num, remainder = divmod(hash_num, 36)
        result.append(chars[remainder])
    return ''.join(reversed(result))


async def get_available_handlers(fastapi_app: FastAPI):
    redis: Redis = fastapi_app.state.redis
    # TODO: возвращать список доступных обработчиков и описания всех
    #  обработчиков из redis разными функциями - функцию по получению
    #  описаний вызывать в случае если есть новые обработчики,
    #  иначе возвращать существующий список или ничего
    try:
        while True:
            workers_ids = await redis.lrange('workers', 0, -1)
            available_handlers: dict[str, int] = {}
            for worker_id in workers_ids:
                try:
                    worker_handlers: list[str] = json.loads(
                        await redis.get(worker_id))
                except TypeError:
                    worker_handlers = []
                for handler_id in worker_handlers:
                    if handler_id in available_handlers:
                        available_handlers[handler_id] += 1
                    else:
                        available_handlers[handler_id] = 1

            current_handlers_ids = set(
                fastapi_app.state.available_handlers.keys())
            available_handlers_ids = set(available_handlers.keys())
            handlers_ids_added = (
                    available_handlers_ids - current_handlers_ids)
            handlers_ids_removed = (
                    current_handlers_ids - available_handlers_ids)

            if handlers_ids_added or handlers_ids_removed:
                if handlers_ids_added:
                    logger.debug(
                        f'ℹ️ Handlers added: {handlers_ids_added}')
                if handlers_ids_removed:
                    logger.debug(
                        f'ℹ️ Handlers removed: {handlers_ids_removed}')
                await update_queues(
                    fastapi_app, handlers_ids_removed, handlers_ids_added)

                handlers_configs = json.loads(
                    await redis.get('handlers_configs'))
                fastapi_app.state.handlers_configs = handlers_configs

            if available_handlers != fastapi_app.state.available_handlers:
                logger.debug(
                    f'ℹ️ Available handlers quantity updated: '
                    f'{available_handlers}')
                fastapi_app.state.available_handlers = available_handlers
                await redis.set(
                    'available_handlers', json.dumps(available_handlers))

            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.error('‼️ Error during updating available handlers')
        fastapi_app.state.available_handlers = {}
        fastapi_app.state.handlers_configs = {}


async def update_queues(fastapi_app: FastAPI,
                        handlers_ids_removed: set[str],
                        handlers_ids_added: set[str]):
    redis: Redis = fastapi_app.state.redis

    if handlers_ids_removed:
        logger.info('♻️ Moving unactual tasks to pending queue...')
        for handler_id in handlers_ids_removed:
            while True:
                task_id = await redis.brpoplpush(
                    f'task_queue:{handler_id}',
                    f'pending_task_queue:{handler_id}',
                    1)
                if not task_id:
                    break
                try:
                    task = Task.model_validate_json(
                        await redis.get(f'task:{task_id}'))
                except ValidationError as e:
                    logger.warning(
                        f'⚠️ Unable to load task: {task_id}, {e}')
                    continue
                task.status = TaskStatus.PENDING
                task.current_position = -1
                async with redis.pipeline() as pipe:
                    await pipe.lrem('task_queue', 0, task_id)
                    await pipe.lpush('pending_task_queue', task_id)
                    await pipe.setex(
                        f'task:{task_id}',
                        settings.redis_store_seconds,
                        task.model_dump_json())
                    await pipe.execute()
                logger.info(
                    f'♻️ Task is pending now: {task_id}, type: {handler_id}')

    logger.info('♻️ Moving processing tasks to pending queue...')
    processing_tasks = await redis.lrange('processing_queue', 0, -1)
    for task_id in processing_tasks:
        try:
            task = Task.model_validate_json(
                await redis.get(f'task:{task_id}'))
        except ValidationError as e:
            logger.warning(
                f'⚠️ Unable to load task: {task_id}, {e}')
            continue
        handler_id = task.handler_id
        if handler_id not in handlers_ids_removed:
            continue
        task.status = TaskStatus.PENDING
        task.current_position = -1
        async with redis.pipeline() as pipe:
            await pipe.lrem('processing_queue', 0, task_id)
            await pipe.lpush('pending_task_queue', task_id)
            await pipe.lpush(f'pending_task_queue:{handler_id}', task_id)
            await pipe.setex(
                f'task:{task_id}',
                settings.redis_store_seconds,
                task.model_dump_json())
            await pipe.execute()
        logger.info(
            f'♻️ Task is pending now: {task_id}, type: {handler_id} '
            f'(was in processing)')

    if handlers_ids_added:
        logger.info('♻️ Pending tasks recovery...')
        pending_tasks = await redis.lrange('pending_task_queue', 0, -1)
        for task_id in pending_tasks:
            try:
                task = Task.model_validate_json(
                    await redis.get(f'task:{task_id}'))
            except ValidationError as e:
                logger.warning(
                    f'⚠️ Unable to load task: {task_id}, {e}')
                continue
            handler_id = task.handler_id
            if handler_id not in handlers_ids_added:
                continue

            task.status = TaskStatus.QUEUED
            async with redis.pipeline() as pipe:
                await pipe.lrem('pending_task_queue', 0, task_id)
                await pipe.lrem(f'pending_task_queue:{handler_id}', 0, task_id)
                await pipe.lpush('task_queue', task_id)
                await pipe.lpush(f'task_queue:{handler_id}', task_id)
                await pipe.setex(
                    f'task:{task_id}',
                    settings.redis_store_seconds,
                    task.model_dump_json())
                await pipe.execute()
            logger.info(f'♻️ Task recovered: {task_id}, type: {handler_id}')

    logger.info('✅️ Queues update finished')


async def cleanup_dlq(redis: Redis):
    while True:
        await asyncio.sleep(3600)
        logger.info('🧹 Cleaning dead_letters...')
        dlq_length = await redis.llen('dead_letters')
        if dlq_length > 50:
            tasks = await redis.lrange('dead_letters', 0, -1)
            for task_id in tasks:
                await redis.delete(f'task:{task_id}')
            await redis.delete('dead_letters')
