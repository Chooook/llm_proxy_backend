import asyncio
import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from redis.asyncio import Redis
from sse_starlette.sse import EventSourceResponse

from schemas.feedback import FeedbackItem
from schemas.task import Task, TaskCreate
from utils.auth_utils import get_current_user
from utils.redis_utils import set_task_to_queue, update_task_position

FEEDBACK_FILE = Path(__file__).parent / 'feedback.json'

router = APIRouter(prefix='/api/v1')


@router.post('/enqueue')
async def enqueue_task(request: Request, task: TaskCreate):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    task_id, short_id = await set_task_to_queue(user_id, task, redis)
    return JSONResponse({'task_id': task_id, 'short_task_id': short_id})


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
            #     dead_letters = await redis.lrange('dead_letters', 0, -1)
            #     if task_id in dead_letters:
            #         yield json.dumps(dead_letters)
                break
            task = Task.model_validate_json(raw_task)
            status = task.status
            position = task.current_position
            if status != last_status or position != last_position:
                yield task.model_dump_json()
                last_status = task.status
            if task.status in ['completed', 'failed']:
                break
            await asyncio.sleep(1)
    return EventSourceResponse(event_generator())


@router.get('/tasks')
async def list_queued_tasks_by_user(request: Request):
    redis: Redis = request.app.state.redis
    user_id = await get_current_user(request, redis)
    tasks: list[Task] = []
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
                task = Task.model_validate_json(raw_task)
            except ValidationError as e:
                print(e, flush=True)
                continue
            if task.user_id == user_id:
                tasks.append(task)
        if cursor == 0:
            break
    tasks.sort(key=lambda t: datetime.fromisoformat(t.queued_at))
    tasks_as_json = [task.model_dump_json() for task in tasks]
    return JSONResponse(tasks_as_json)


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

        return JSONResponse({
            'status': 'success', 'message': 'Feedback received'})

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Failed to save feedback: {str(e)}'
        )
