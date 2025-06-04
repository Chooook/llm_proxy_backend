from typing import Optional

from pydantic import BaseModel

class FeedbackItem(BaseModel):
    text: str
    contact: Optional[str] = None
