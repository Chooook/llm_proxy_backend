import asyncio
import json
import uuid

from aioredis import Redis
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from utils.auth_utils import get_current_user
from utils.task_short_id import generate_short_id
from schemas.task import TaskCreate

router = APIRouter(prefix='/api/v1')


@router.post('/enqueue')
async def enqueue_task(request: Request, task: TaskCreate):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    task_id = str(uuid.uuid4())
    short_id = generate_short_id(task_id, user_id)

    await redis.setex(
        f'task:{task_id}',
        3600,
        json.dumps({
            'status': 'queued',
            'prompt': task.prompt.strip(),
            'type': task.task_type,
            'user_id': user_id,
            'short_task_id': short_id
        })
    )
    await redis.rpush('task_queue', task_id)
    return JSONResponse({'task_id': task_id, 'short_id': short_id})


@router.get('/subscribe/{task_id}')
async def subscribe_stream_status(request: Request, task_id: str):
    redis: Redis = request.app.state.redis
    async def event_generator():
        last_status = ''
        while True:
            raw_task = await redis.get(f'task:{task_id}')
            if not raw_task:
                in_dead_letters = await redis.lpos("dead_letters", task_id)
                if in_dead_letters is not None:
                    yield json.dumps(in_dead_letters)
                break
            task = json.loads(raw_task)
            if task['status'] != last_status:
                yield json.dumps(task)
                last_status = task['status']
            if task['status'] in ['completed', 'failed']:
                break
            await asyncio.sleep(1)
    return EventSourceResponse(event_generator())


@router.get('/tasks/')
async def list_queued_tasks_by_user(request: Request):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    tasks: list[dict] = []
    if not user_id:
        return JSONResponse(tasks)
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor, match='task:*', count=100)
        for task_id in keys:
            try:
                raw_task = await redis.get(task_id)
                if not raw_task:
                    continue
                task = json.loads(raw_task)
                if not task:
                    continue
            except json.JSONDecodeError:
                continue
            if task['user_id'] == user_id:
                tasks.append({
                    'task_id': task_id.split(':')[1],
                    'status': task['status'],
                    'prompt': task['prompt'],
                    'result': str(task.get('result')),
                    'type': task['type'],
                    'user_id': task['user_id'],
                    'short_task_id': task['short_task_id']
                    })
        if cursor == 0:
            break
    return JSONResponse(tasks)
