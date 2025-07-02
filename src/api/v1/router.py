import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import ValidationError
from redis.asyncio import Redis
from sse_starlette.sse import EventSourceResponse

from settings import settings
from schemas.feedback import FeedbackItem, TaskFeedback
from schemas.task import Task, TaskCreate, TaskStatus
from utils.auth_utils import get_current_user
from utils.redis_utils import set_task_to_queue, update_task_position
from utils.gp_utils import run_query

FEEDBACK_FILE = Path(__file__).parent / 'feedback.json'

router = APIRouter(prefix='/api/v1')


@router.post('/enqueue')
async def enqueue_task(request: Request, task: TaskCreate):
    user_id = await get_current_user(request, request.app.state.redis)
    if not task.handler_id or task.handler_id == 'default':
        raise HTTPException(status_code=405, detail='Invalid handler_id')
    task_id, short_id = await set_task_to_queue(user_id, task, request.app)
    return {'task_id': task_id, 'short_task_id': short_id}


@router.get('/subscribe/{task_id}')
async def subscribe_stream_status(request: Request, task_id: str):
    redis: Redis = request.app.state.redis
    async def event_generator():
        last_status = ''
        last_position = -1
        while True:
            await update_task_position(task_id, redis)
            raw_task = await redis.get(f'task:{task_id}')
            if not raw_task:
                break
            task = Task.model_validate_json(raw_task)
            status = task.status
            position = task.current_position
            if status != last_status or position != last_position:
                yield task.model_dump_json(indent=2)
                last_status = task.status
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                task.finished_at = datetime.now(timezone.utc).isoformat()
                await redis.setex(
                    f'task:{task_id}', 86400, task.model_dump_json())
                break
            await asyncio.sleep(1)
    return EventSourceResponse(event_generator())


@router.post('/feedback/{task_id}')
async def submit_task_feedback(
        request: Request, task_id: str, feedback: TaskFeedback):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    task = Task.model_validate_json(await redis.get(f'task:{task_id}'))
    if task.user_id != user_id:
        raise HTTPException(status_code=403, detail='Forbidden')
    task.feedback = feedback
    await redis.setex(f'task:{task_id}', 3600, task.model_dump_json())


@router.get('/tasks')
async def list_queued_tasks_by_user(request: Request):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    tasks: list[Task] = []
    if not user_id:
        return tasks
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor, match='task:*', count=100)
        for task_id in keys:
            try:
                raw_task = await redis.get(task_id)
                if not raw_task:
                    continue
                task = Task.model_validate_json(raw_task)
            except ValidationError as e:
                logger.warning(f'Ошибка валидации задачи {task_id}: {e}')
                continue
            if task.user_id == user_id:
                tasks.append(task)
        if cursor == 0:
            break
    tasks.sort(key=lambda t: datetime.fromisoformat(t.queued_at))
    tasks_as_json = [task.model_dump_json(indent=2) for task in tasks]
    return tasks_as_json


@router.post('/feedback')
async def submit_feedback(feedback: FeedbackItem):
    """Endpoint для сохранения обратной связи"""

    def ensure_feedback_file_exists():
        """Создать файл для хранения отзывов, если он не существует"""
        if not FEEDBACK_FILE.exists():
            with open(FEEDBACK_FILE, 'w', encoding='utf-8') as fb_file:
                json.dump([], fb_file, ensure_ascii=False, indent=2)

    ensure_feedback_file_exists()

    try:
        with open(FEEDBACK_FILE, 'r', encoding='utf-8') as f:
            feedbacks = json.load(f)

        new_feedback = {
            'text': feedback.text,
            'contact': feedback.contact,
            'timestamp': datetime.now().isoformat()
        }
        feedbacks.append(new_feedback)

        with open(FEEDBACK_FILE, 'w', encoding='utf-8') as f:
            json.dump(feedbacks, f, ensure_ascii=False, indent=2)

        return {'status': 'success', 'message': 'Feedback received'}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Failed to save feedback: {str(e)}'
        )


@router.get('/handlers/stream')
async def available_handlers_stream(request: Request):
    # FIXME: если сервер остановить - frontend зависнет со старыми данными,
    #  доработать обработку ошибок на фронте
    async def event_generator():
        last_data = None
        while True:
            handlers = request.app.state.available_handlers
            configs = request.app.state.handlers_configs
            handlers_with_configs = {
                'available_handlers': handlers, 'configs': configs}
            if handlers_with_configs != last_data:
                last_data = handlers_with_configs
                logger.debug(
                    f'ℹ️ Available handlers quantity updated: {handlers}')
                yield json.dumps(handlers_with_configs)
            await asyncio.sleep(3)

    return EventSourceResponse(event_generator())


@router.get('/test_gp')
async def test_gp_query(schema=settings.GP_SCHEMA,
                        table='kmaus_user_data',
                        limit=3):
    try:

        query = f'select task_id, prompt, status from {schema}.{table} limit {limit};'
        result = await run_query(query)

        if result:
            return JSONResponse({
                'status': 'success',
                'data': [dict(row) for row in result],
                'message': 'GreenPlum connection test successful'
            })

        return JSONResponse({
            'status': 'success',
            'message': 'Query executed but returned no data'
        })

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'GreenPlum connection test failed: {str(e)}'
        )
