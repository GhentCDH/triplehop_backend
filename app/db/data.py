from __future__ import annotations
from typing import Any, Dict, List

from databases import Database
import json

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import RE_RECORD, dtu, utd


class DataRepository(BaseRepository):
    def __init__(self, db: Database, project_name: str) -> None:
        super().__init__(db)
        self._conf_repo = ConfigRepository(db)
        self._project_name = project_name
        self._project_id = None

    async def get_entity(
        self,
        entity_type_name: str,
        entity_id: int,
    ):
        async with self._db.transaction():
            await self._init_age()
            if self._project_id is None:
                self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

            query = (
                f'SELECT * FROM cypher('
                f'\'{self._project_id}\', '
                f'$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) return n$$, :params'
                f') as (n agtype);'
            )

            record = await self._db.fetch_one(
                query,
                {
                    'params': json.dumps({
                        'entity_id': entity_id
                    })
                }
            )

            if record is None:
                return None

            etpm = await self._conf_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

            # strip ::vertex from record data
            properties = json.loads(record['n'][:-8])['properties']

            return {etpm[k]: v for k, v in properties.items() if k in etpm}

    async def get_relations(
        self,
        entity_type_name: str,
        entity_id: int,
        relation_type_name: str,
        inverse: bool = False,
    ):
        async with self._db.transaction():
            await self._init_age()
            if self._project_id is None:
                self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
            entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)
            relation_type_id = await self._conf_repo.get_relation_type_id_by_name(self._project_name, relation_type_name)

            # TODO: figure out why the dummy is required
            if inverse:
                query = (
                    f'SELECT * FROM cypher('
                    f'\'{self._project_id}\', '
                    f'$$MATCH () -[e:e_{dtu(relation_type_id)}] -> (\\:n_{dtu(entity_type_id)} {{id: $entity_id}})'
                    f'return e$$, :params'
                    f') as (e agtype);'
                )
            else:
                query = (
                    f'SELECT * FROM cypher('
                    f'\'{self._project_id}\', '
                    f'$$MATCH (\\:n_{dtu(entity_type_id)} {{id: $entity_id}}) -[e:e_{dtu(relation_type_id)}] -> ()'
                    f'return e$$, :params'
                    f') as (e agtype);'
                )

            records = await self._db.fetch_all(
                query,
                {
                    'params': json.dumps({
                        'entity_id': entity_id
                    })
                }
            )

            rtpm = await self._conf_repo.get_relation_type_property_mapping(self._project_name, relation_type_name)
            results = []

            for record in records:
                # strip ::edge from record data
                properties = json.loads(record['e'][:-6])['properties']
                results.append({rtpm[k]: v for k, v in properties.items() if k in rtpm})

            return results

    async def get_relations_with_entity(
        self,
        entity_type_name: str,
        entity_id: int,
        relation_type_name: str,
        inverse: bool = False,
    ):
        # async with self.connection.transaction():
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)
        relation_type_id = await self._conf_repo.get_relation_type_id_by_name(self._project_name, relation_type_name)

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

        rtpm = await self._conf_repo.get_relation_type_property_mapping(self._project_name, relation_type_name)
        results = []
        for record in records:
            result = {}

            # relation properties
            raw_relation = json.loads(RE_RECORD.match(record['e']).group(2))
            result['relation'] = {rtpm[k]: v for k, v in raw_relation.items() if k in rtpm}

            # entity properties
            entity_match = RE_RECORD.match(record['ve'])
            entity_type_id = utd(entity_match.group(1))
            etn = await self._conf_repo.get_entity_type_name_by_id(self._project_name, entity_type_id)
            e_property_mapping = await self._conf_repo.get_entity_type_property_mapping(
                self._project_name,
                etn
            )
            raw_entity = json.loads(entity_match.group(2))
            result['entity'] = {e_property_mapping[k]: v for k, v in raw_entity.items() if k in e_property_mapping}
            result['entity_type_name'] = etn

            results.append(result)

        return results

    async def get_entity_ids_by_type_name(
        self,
        entity_type_name: str,
    ) -> List[int]:
        # async with self.connection.transaction():
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

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
        return json.loads(jsonb)

    async def get_entity_data(
        self,
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
        # async with self.connection.transaction():
        entity_type_ids[entity_type_name] = \
            await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

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
                await self._conf_repo.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
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
            relation_types_config = await self._conf_repo.get_relation_types_config(self._project_name)
            for relation in query['relations']:
                if relation[:2] == 'r_':
                    relation_name = relation[2:]
                    entity_type_names = relation_types_config[relation_name]['range_names']
                    raw_db_query = '''
                        MATCH (ve:v_{entity_type_id})
                        WHERE ve.id IN [{entity_ids}]
                        MATCH (ve)-[e:e_{relation_type_id}]->(ver:v_{range_entity_type_id})
                    '''
                elif relation[:3] == 'ri_':
                    relation_name = relation[3:]
                    entity_type_names = relation_types_config[relation_name]['domain_names']
                    raw_db_query = '''
                        MATCH (ve:v_{entity_type_id})
                        WHERE ve.id IN [{entity_ids}]
                        MATCH (ver:v_{range_entity_type_id})-[e:e_{relation_type_id}]->(ve)
                    '''
                else:
                    continue

                for etn in entity_type_names:
                    if etn not in entity_type_ids:
                        entity_type_ids[etn] = \
                            await self._conf_repo.get_entity_type_id_by_name(self._project_name, etn)

                    # TODO: find a way to use placeholders to pass list of ids
                    db_query = raw_db_query.format(
                        entity_type_id=dtu(entity_type_ids[entity_type_name]),
                        entity_ids=','.join([str(id) for id in entity_ids]),
                        relation_type_id=dtu(relation_types_config[relation_name]['id']),
                        range_entity_type_id=dtu(entity_type_ids[etn]),
                    )

                    if etn not in mappings:
                        mappings[etn] = \
                            await self._conf_repo.get_entity_type_i_property_mapping(self._project_name, etn)

                    query_return_parts = ['ve.id', 'label(ver) as __type__']
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
                                },
                                'entity_type_name': await self._conf_repo.get_entity_type_name_by_id(
                                    self._project_name,
                                    utd(self.convert_from_jsonb(record['__type__'])[2:])
                                )
                            }
                        )

            return results
