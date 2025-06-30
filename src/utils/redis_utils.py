import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from loguru import logger
from redis.asyncio import Redis

from schemas.task import Task, TaskCreate, TaskStatus


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
        await redis.setex(f'task:{task_id}', 3600, task.model_dump_json())


async def set_task_to_queue(user_id: str,
                            task: TaskCreate,
                            fastapi_app: FastAPI) -> tuple[str, str]:
    redis: Redis = fastapi_app.state.redis
    task_id = str(uuid.uuid4())
    short_id = generate_short_id(task_id, user_id)
    task_to_enqueue = Task(
        task_id=task_id,
        prompt=task.prompt.strip(),
        task_type=task.task_type,
        user_id=user_id,
        short_task_id=short_id,
        queued_at=datetime.now(timezone.utc).isoformat(),
    )

    handlers = fastapi_app.state.handlers
    available_handlers = [
        h['task_type'] for h in handlers if h['available_workers'] > 0]
    if task.task_type not in available_handlers:
        task_to_enqueue.start_position = -1
        task_to_enqueue.status = TaskStatus.PENDING
        async with redis.pipeline() as pipe:
            await redis.setex(
                f'task:{task_id}', 3600, task_to_enqueue.model_dump_json())
            await pipe.lpush('pending_task_queue', task_id)
            await pipe.lpush(f'pending_task_queue:{task.task_type}', task_id)
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
                f'task:{task_id}', 3600, task_to_enqueue.model_dump_json())
            await pipe.lpush('task_queue', task_id)
            await pipe.lpush(f'task_queue:{task.task_type}', task_id)
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


async def update_handlers(fastapi_app: FastAPI):
    redis: Redis = fastapi_app.state.redis
    try:
        while True:
            raw_new_handlers = await redis.get('handlers')
            new_handlers = (
                json.loads(raw_new_handlers) if raw_new_handlers else [])
            current_handlers = fastapi_app.state.handlers

            if current_handlers != new_handlers:

                new_task_types = {h['task_type'] for h in new_handlers
                                  if h['available_workers'] > 0}
                old_task_types = {h['task_type'] for h in current_handlers
                                  if h['available_workers'] > 0}
                types_removed = old_task_types - new_task_types
                types_added = new_task_types - old_task_types

                fastapi_app.state.handlers = new_handlers
                logger.debug(f'ℹ️ Handlers updated: {new_handlers}')
                if types_added:
                    logger.debug(f'ℹ️ Handlers added: {types_added}')
                if types_removed:
                    logger.debug(f'ℹ️ Handlers removed: {types_removed}')
                await update_queues(
                    fastapi_app, types_removed, types_added)

            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.error('‼️ Error during updating handlers')
        fastapi_app.state.handlers = []


async def update_queues(fastapi_app: FastAPI,
                        types_removed: set[str],
                        types_added: set[str]):
    redis: Redis = fastapi_app.state.redis

    if types_removed:
        logger.info('🔍 Moving unactual tasks to pending queue...')
        for task_type in types_removed:
            while True:
                task_id = await redis.brpoplpush(
                    f'task_queue:{task_type}',
                    f'pending_task_queue:{task_type}',
                    1)
                if not task_id:
                    break
                task = Task.model_validate_json(
                    await redis.get(f'task:{task_id}'))
                task.status = TaskStatus.PENDING
                task.current_position = -1
                async with redis.pipeline() as pipe:
                    await pipe.lrem('task_queue', 0, task_id)
                    await pipe.lpush('pending_task_queue', task_id)
                    await pipe.setex(
                        f'task:{task_id}', 3600, task.model_dump_json())
                    await pipe.execute()
                logger.info(
                    f'♻️ Task is pending now: {task_id}, type: {task_type}')

    logger.info('🔍 Moving processing tasks to pending queue...')
    processing_tasks = await redis.lrange('processing_queue', 0, -1)
    for task_id in processing_tasks:
        task = Task.model_validate_json(await redis.get(f'task:{task_id}'))
        task_type = task.task_type
        if task_type not in types_removed:
            continue
        task.status = TaskStatus.PENDING
        task.current_position = -1
        async with redis.pipeline() as pipe:
            await pipe.lrem('processing_queue', 0, task_id)
            await pipe.lpush('pending_task_queue', task_id)
            await pipe.lpush(f'pending_task_queue:{task_type}', task_id)
            await pipe.setex(
                f'task:{task_id}', 3600, task.model_dump_json())
            await pipe.execute()
        logger.info(
            f'♻️ Task is pending now: {task_id}, type: {task_type} '
            f'(was in processing)')

    if types_added:
        logger.info('🔍 Pending tasks recovery...')
        pending_tasks = await redis.lrange('pending_task_queue', 0, -1)
        for task_id in pending_tasks:
            task = Task.model_validate_json(await redis.get(f'task:{task_id}'))
            task_type = task.task_type
            if task_type not in types_added:
                continue

            task.status = TaskStatus.QUEUED
            async with redis.pipeline() as pipe:
                await pipe.lrem('pending_task_queue', 0, task_id)
                await pipe.lrem(f'pending_task_queue:{task_type}', 0, task_id)
                await pipe.lpush('task_queue', task_id)
                await pipe.lpush(f'task_queue:{task_type}', task_id)
                await pipe.setex(
                    f'task:{task_id}', 3600, task.model_dump_json())
                await pipe.execute()
            logger.info(f'♻️ Task recovered: {task_id}, type: {task_type}')


async def cleanup_dlq(redis: Redis):
    while True:
        await asyncio.sleep(3600)
        logger.info('🧹 Очистка dead_letters...')
        dlq_length = await redis.llen('dead_letters')
        if dlq_length > 50:
            tasks = await redis.lrange('dead_letters', 0, -1)
            for task_id in tasks:
                await redis.delete(f'task:{task_id}')
            await redis.delete('dead_letters')
