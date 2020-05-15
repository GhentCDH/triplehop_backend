from datetime import datetime
from pydantic import BaseModel, UUID4
from typing import Optional


class JobId(BaseModel):
    id: UUID4


class JobToDisplay(BaseModel):
    id: UUID4
    user_name: str
    project_system_name: Optional[str]
    project_display_name: Optional[str]
    entity_type_system_name: Optional[str]
    entity_type_display_name: Optional[str]
    relation_type_system_name: Optional[str]
    relation_type_display_name: Optional[str]
    type: str
    status: str
    counter: Optional[int]
    total: Optional[int]
    created: datetime
    started: Optional[datetime]
    ended: Optional[datetime]
