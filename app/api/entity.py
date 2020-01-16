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
    # project_id = await get_project_id_by_name(project_name)
    # entity_type_id = await get_entity_type_id_by_name(project_name, entity_type_name)
    # async with pool.acquire() as conn:
    #     await conn.execute(
    #         '''
    #             SET graph_path = g{project_id};
    #         '''.format(project_id=project_id)
    #     )
    #     return await conn.fetchrow(
    #         '''
    #             MATCH (ve:v{entity_type_id} {{id: $1}}) RETURN ve;
    #         '''.format(entity_type_id=entity_type_id),
    #         str(entity_id)
    #     )
