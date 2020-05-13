from typing import Any, Dict, List

from asyncpg.connection import Connection
from fastapi import HTTPException
from json import loads as json_load

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import RE_RECORD, dtu, utd


class DataRepository(BaseRepository):
    def __init__(self, conn: Connection) -> None:
        super().__init__(conn)
        self._conf_repo = ConfigRepository(conn)

    async def _set_graph(self, project_name: str):
        project_id = await self._conf_repo.get_project_id_by_name(project_name)
        await self._conn.execute(
            '''
                SET graph_path = g_{project_id};
            '''.format(
                project_id=dtu(project_id),
            )
        )

    async def get_entity(
        self,
        project_name: str,
        entity_type_name: str,
        entity_id: int,
    ):
        async with self.connection.transaction():
            await self._set_graph(project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)

            record = await self.fetchrow(
                '''
                    MATCH (ve:v_{entity_type_id} {{id: :id}}) RETURN ve;
                '''.format(
                    entity_type_id=dtu(entity_type_id),
                ),
                # TODO: figure out why id can't be an int
                {
                    'id': str(entity_id),
                }
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
            await self._set_graph(project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)
            relation_type_id = await self._conf_repo.get_relation_type_id_by_name(project_name, relation_type_name)

            # TODO: figure out why the dummy d is required
            if inverse:
                query = '''
                    MATCH (ve) -[e:e_{relation_type_id}]-> (d:v_{entity_type_id} {{id: :id}}) RETURN e, ve;
                '''.format(
                    entity_type_id=dtu(entity_type_id),
                    relation_type_id=dtu(relation_type_id),
                )
            else:
                query = '''
                    MATCH (d:v_{entity_type_id} {{id: :id}}) -[e:e_{relation_type_id}]-> (ve) RETURN e, ve;
                '''.format(
                    entity_type_id=dtu(entity_type_id),
                    relation_type_id=dtu(relation_type_id),
                )

            records = await self.fetch(
                query,
                # TODO: figure out why id can't be an int
                {
                    'id': str(entity_id),
                }
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
                entity_type_id = utd(entity_match.group(1))
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

    async def get_entity_ids_by_type_name(
        self,
        project_name: str,
        entity_type_name: str,
    ) -> List[int]:
        async with self.connection.transaction():
            await self._set_graph(project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)

            records = await self.fetch(
                '''
                    MATCH (ve:v_{entity_type_id}) RETURN ve.id;
                '''.format(
                    entity_type_id=dtu(entity_type_id),
                )
            )

            return [int(r['id']) for r in records]

    @staticmethod
    def convert_from_jsonb(jsonb: str) -> Any:
        if jsonb is None:
            return None
        return json_load(jsonb)

    async def get_entity_data(
        self,
        project_name: str,
        entity_type_name: str,
        entity_ids: List[int],
        query: Dict,
    ) -> Dict[int, Dict]:
        if not entity_ids:
            return {}

        if not query:
            return {}

        entity_type_ids = {}
        mappings = {}
        results = {}
        async with self.connection.transaction():
            await self._set_graph(project_name)
            entity_type_ids[entity_type_name] = \
                await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)

            if query['props']:
                # TODO: find a way to use placeholders to pass list of ids
                db_query = \
                    '''
                        MATCH (ve:v_{entity_type_id})
                        WHERE ve.id IN [{entity_ids}]
                    '''.format(
                        entity_type_id=dtu(entity_type_ids[entity_type_name]),
                        entity_ids=','.join([str(id) for id in entity_ids])
                    )

                mappings[entity_type_name] = \
                    await self._conf_repo.get_entity_type_i_property_mapping(project_name, entity_type_name)
                query_return_parts = ['ve.id']
                for prop in query['props']:
                    if prop != 'id':
                        query_return_parts.append(
                            f've.{mappings[entity_type_name][prop]} as {prop}'
                        )
                db_query += \
                    '''
                        RETURN {return_query}
                    '''.format(
                        return_query=', '.join(query_return_parts)
                    )

                records = await self.fetch(db_query)
                for record in records:
                    results[record['id']] = {
                        p: self.convert_from_jsonb(record[p])
                        for p in query['props']
                    }

            if query['relations']:
                relation_types_config = await self._conf_repo.get_relation_types_config(project_name)
                for relation in query['relations']:
                    # TODO: inverse relations
                    if relation[:2] == 'r_':
                        for etn in relation_types_config[relation[2:]]['range_names']:
                            if etn not in entity_type_ids:
                                entity_type_ids[etn] = \
                                    await self._conf_repo.get_entity_type_id_by_name(project_name, etn)

                            # TODO: find a way to use placeholders to pass list of ids
                            db_query = \
                                '''
                                    MATCH (ve:v_{entity_type_id})
                                    WHERE ve.id IN [{entity_ids}]
                                    MATCH (ve)-[e:e_{relation_type_id}]->(ver:v_{range_entity_type_id})
                                '''.format(
                                    entity_type_id=dtu(entity_type_ids[entity_type_name]),
                                    entity_ids=','.join([str(id) for id in entity_ids]),
                                    relation_type_id=dtu(relation_types_config[relation[2:]]['id']),
                                    range_entity_type_id=dtu(entity_type_ids[etn]),
                                )

                            if etn not in mappings:
                                mappings[etn] = \
                                    await self._conf_repo.get_entity_type_i_property_mapping(project_name, etn)

                            query_return_parts = ['ve.id']
                            # TODO: props on relation itself
                            for prop in query['relations'][relation]['e_props']:
                                if prop in mappings[etn]:
                                    query_return_parts.append(
                                        f'ver.{mappings[etn][prop]} as {relation}_e_{prop}'
                                    )

                            db_query += \
                                '''
                                    RETURN {return_query}
                                '''.format(
                                    return_query=', '.join(query_return_parts)
                                )

                            records = await self.fetch(db_query)
                            for record in records:
                                if record['id'] not in results:
                                    results[record['id']] = {}
                                # TODO: relations with cardinatlity 1
                                if relation not in results[record['id']]:
                                    results[record['id']][relation] = []
                                results[record['id']][relation].append(
                                    {
                                        'e_props': {
                                            p: self.convert_from_jsonb(record[f'{relation}_e_{p}'])
                                            for p in query['relations'][relation]['e_props']
                                        }
                                    }
                                )

            return results
