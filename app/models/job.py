from pydantic import BaseModel, UUID4


class Job(BaseModel):
    id: UUID4
    project_name: str
    entity_name: str
    type: str
    status: str
