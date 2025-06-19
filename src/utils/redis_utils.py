import hashlib
import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from schemas.task import Task, TaskCreate


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


async def set_task_to_queue(
        user_id: str, task: TaskCreate, redis: Redis) -> tuple[str, str]:
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

    task_id = str(uuid.uuid4())
    short_id = generate_short_id(task_id, user_id)

    async with redis.pipeline() as pipe:
        current_length = await pipe.llen('task_queue').execute()
        start_position = current_length[0] + 1
        task_to_enqueue = Task(
            task_id=task_id,
            status='queued',
            prompt=task.prompt.strip(),
            task_type=task.task_type,
            user_id=user_id,
            short_task_id=short_id,
            queued_at=datetime.now(timezone.utc).isoformat(),
            start_position=start_position,
        )
        await redis.setex(
            f'task:{task_id}', 3600, task_to_enqueue.model_dump_json())
        await pipe.lpush('task_queue', task_id).execute()

    return task_id, short_id
