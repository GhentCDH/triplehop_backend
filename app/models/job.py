from datetime import datetime
from pydantic import BaseModel, UUID4


class JobId(BaseModel):
    id: UUID4


class JobToDisplay(BaseModel):
    id: UUID4
    user_name: str
    project_system_name: str = None
    project_display_name: str = None
    entity_type_system_name: str = None
    entity_type_display_name: str = None
    relation_type_system_name: str = None
    relation_type_display_name: str = None
    type: str
    status: str
    counter: int = None
    total: int = None
    created: datetime
    started: datetime = None
    ended: datetime = None
