from asyncpg.connection import Connection
from fastapi import HTTPException
from json import loads as json_load
from re import compile as re_compile

from app.db.base import BaseRepository
from app.db.config import ConfigRepository

RE_NODE = re_compile('({[^}]*})')


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
        async with self.connection.transaction():
            project_id = await self._conf_repo.get_project_id_by_name(project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)

            await self._conn.execute(
                '''
                    SET graph_path = g{project_id};
                '''.format(project_id=project_id)
            )
            record = await self.fetchrow(
                '''
                    MATCH (ve:v{entity_type_id} {{id: :id}}) RETURN ve;
                '''.format(entity_type_id=entity_type_id),
                id=str(entity_id),
            )

            if record is None:
                raise HTTPException(
                    status_code=404,
                    detail=f'Entity of type "{entity_type_name}" with id {entity_id} not found',
                )

            raw_entity = json_load(RE_NODE.search(record['ve']).group(1))

            property_mapping = await self._conf_repo.get_entity_type_property_mapping(project_name, entity_type_name)
            return {property_mapping[k]: v for k, v in raw_entity.items() if k in property_mapping}
