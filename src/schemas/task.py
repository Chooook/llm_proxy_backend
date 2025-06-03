from enum import Enum

from pydantic import BaseModel


class TaskType(str, Enum):
    GENERATE_WITH_API = 'generate_api'
    GENERATE_WITH_LOCAL = 'generate_local'
    GENERATE_WITH_GP = 'generate_gp'
    SEARCH_IN_KNOWLEDGE_BASE = 'search_in_kb'

class TaskCreate(BaseModel):
    prompt: str
    # TODO: replace with real user id when authentication is implemented:
    user_id: str = 'localuser'
    task_type: TaskType = TaskType.GENERATE_WITH_LOCAL
