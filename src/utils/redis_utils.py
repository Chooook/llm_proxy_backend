from redis.asyncio import Redis

from schemas.task import Task


async def update_task_position(task_id: str, redis: Redis):
    all_tasks = await redis.lrange('task_queue', 0, -1)
    all_tasks.reverse()
    try:
        current_pos = all_tasks.index(task_id) + 1
    except ValueError:
        current_pos = 0
    task_data = await redis.get(f'task:{task_id}')
    if task_data:
        task = Task.model_validate_json(task_data)
        task.current_position = current_pos
        await redis.setex(f'task:{task_id}', 3600, task.model_dump_json())
