from fastapi import APIRouter, Depends

from app.db.core import get_repository
from app.db.entity import EntityRepository

router = APIRouter()

@router.get('/{project_name}/{entity_type_name}/{entity_id}')
async def entity(
    project_name: str,
    entity_type_name: str,
    entity_id: int,
    entity_repo: EntityRepository = Depends(get_repository(EntityRepository))
):
    return await entity_repo.get_entity(project_name, entity_type_name, entity_id)
