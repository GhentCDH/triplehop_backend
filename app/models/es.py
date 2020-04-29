from pydantic import BaseModel


class ElasticSearchRequest(BaseModel):
    project_name: str
    entity_name: str
