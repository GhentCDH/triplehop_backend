from pydantic import BaseModel


class ElasticSearchRequest(BaseModel):
    project_name: str
    entity_type_name: str
