from enum import Enum

from pydantic import BaseModel


class TaskType(str, Enum):
    DUMMY = 'dummy'
    GENERATE_WITH_LOCAL = 'generate_local'
    SEARCH_IN_KNOWLEDGE_BASE = 'search'
    GENERATE_WITH_PM = 'generate_pm'
    GENERATE_WITH_SPC = 'generate_spc'
    GENERATE_WITH_OAPSO = 'generate_oapso'

class TaskCreate(BaseModel):
    prompt: str
    task_type: TaskType = TaskType.GENERATE_WITH_LOCAL
