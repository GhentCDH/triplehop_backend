from asyncpg.connection import Connection

from app.db.base import BaseRepository
from app.db.config import ConfigRepository

class EntityRepository(BaseRepository):
    def __init__(self, conn: Connection) -> None:
        super().__init__(conn)
        self._conf_repo = ConfigRepository(conn)

    async def get_entity(
        self,
        project_name: str,
        entity_type_name: str,
        entity_id: int
    ):
        project_id = await self._conf_repo.get_project_id_by_name(project_name)
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)
        await self._conn.execute(
            '''
                SET graph_path = g{project_id};
            '''.format(project_id=project_id)
        )
        return await self.fetchrow(
            '''
                MATCH (ve:v{entity_type_id} {{id: :id}}) RETURN ve;
            '''.format(entity_type_id=entity_type_id),
            id=str(entity_id)
        )
