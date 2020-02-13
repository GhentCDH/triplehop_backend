from asyncpg.connection import Connection
from fastapi import HTTPException
from json import loads as json_load
from re import compile as re_compile

from app.db.base import BaseRepository
from app.db.config import ConfigRepository

RE_RECORD = re_compile('^[ev]([0-9]+)[^{]*({[^}]*})$')


class DataRepository(BaseRepository):
    def __init__(self, conn: Connection) -> None:
        super().__init__(conn)
        self._conf_repo = ConfigRepository(conn)

    async def get_entity(
        self,
        project_name: str,
        entity_type_name: str,
        entity_id: int,
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
                # TODO: figure out why id can't be an int
                id=str(entity_id),
            )

            if record is None:
                raise HTTPException(
                    status_code=404,
                    detail=f'Entity of type "{entity_type_name}" with id {entity_id} not found',
                )

            raw_entity = json_load(RE_RECORD.match(record['ve']).group(2))

            etpm = await self._conf_repo.get_entity_type_property_mapping(project_name, entity_type_name)
            return {etpm[k]: v for k, v in raw_entity.items() if k in etpm}

    async def get_relations_with_entity(
        self,
        project_name: str,
        entity_type_name: str,
        entity_id: int,
        relation_type_name: str,
        inverse: bool = False,
    ):
        async with self.connection.transaction():
            project_id = await self._conf_repo.get_project_id_by_name(project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)
            relation_type_id = await self._conf_repo.get_relation_type_id_by_name(project_name, relation_type_name)

            await self._conn.execute(
                '''
                    SET graph_path = g{project_id};
                '''.format(project_id=project_id)
            )

            # TODO: figure out why the dummy d is required
            if inverse:
                query = '''
                    MATCH (ve) -[e:e{relation_type_id}]-> (d:v{entity_type_id} {{id: :id}}) RETURN e, ve;
                '''.format(entity_type_id=entity_type_id, relation_type_id=relation_type_id)
            else:
                query = '''
                    MATCH (d:v{entity_type_id} {{id: :id}}) -[e:e{relation_type_id}]-> (ve) RETURN e, ve;
                '''.format(entity_type_id=entity_type_id, relation_type_id=relation_type_id)

            records = await self.fetch(
                query,
                # TODO: figure out why id can't be an int
                id=str(entity_id),
            )

            rtpm = await self._conf_repo.get_relation_type_property_mapping(project_name, relation_type_name)
            results = []
            for record in records:
                result = {}

                # relation properties
                raw_relation = json_load(RE_RECORD.match(record['e']).group(2))
                result['relation'] = {rtpm[k]: v for k, v in raw_relation.items() if k in rtpm}

                # entity properties
                entity_match = RE_RECORD.match(record['ve'])
                entity_type_id = int(entity_match.group(1))
                etn = await self._conf_repo.get_entity_type_name_by_id(project_name, entity_type_id)
                e_property_mapping = await self._conf_repo.get_entity_type_property_mapping(
                    project_name,
                    etn
                )
                raw_entity = json_load(entity_match.group(2))
                result['entity'] = {e_property_mapping[k]: v for k, v in raw_entity.items() if k in e_property_mapping}
                result['entity_type_name'] = etn

                results.append(result)

            return results
