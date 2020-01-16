from asyncpg.pool import Pool
from fastapi import APIRouter, Depends

from app.db import get_pool

router = APIRouter()

@router.get('/{project_name}/{entity_type_name}/{entity_id}')
async def entity(
    project_name: str,
    entity_type_name: str,
    entity_id: int,
    pool:Pool = Depends(get_pool)
):
    async with pool.acquire() as conn:
        await conn.execute('''
            SET graph_path = g1;
        ''')
        return await conn.fetchrow('''
            MATCH (ve:v1 {id: 1913}) RETURN ve;
        ''')
