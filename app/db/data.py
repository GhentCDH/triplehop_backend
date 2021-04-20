from __future__ import annotations

import asyncpg
import json
import typing

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import dtu, utd


class DataRepository(BaseRepository):
    def __init__(self, pool: asyncpg.pool.Pool, project_name: str) -> None:
        super().__init__(pool)
        self._conf_repo = ConfigRepository(pool)
        self._project_name = project_name

    async def get_entities(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH (n:n_{dtu(entity_type_id)}) '
            f'WHERE n.id IN $entity_ids '
            f'return n$$, :params'
            f') as (n agtype);'
        )
        records = await self.fetch(
            query,
            {
                'params': json.dumps({
                    'entity_ids': entity_ids
                })
            },
            age=True
        )

        if not records:
            return records

        etpm = await self._conf_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

        results = {}
        for record in records:
            # strip ::vertex from record data
            properties = json.loads(record['n'][:-8])['properties']

            results[properties['id']] = {etpm[k]: v for k, v in properties.items() if k in etpm}
        return results

    async def get_relations(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        relation_type_name: str,
        inverse: bool = False,
    ) -> typing.Dict:
        project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)
        relation_type_id = await self._conf_repo.get_relation_type_id_by_name(self._project_name, relation_type_name)

        if inverse:
            query = (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$MATCH (d) -[e:e_{dtu(relation_type_id)}] -> (r:n_{dtu(entity_type_id)}) '
                f'WHERE r.id IN $entity_ids '
                f'return r.id, e, d$$, :params'
                f') as (id agtype, e agtype, n agtype);'
            )
        else:
            query = (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$MATCH (d:n_{dtu(entity_type_id)}) -[e:e_{dtu(relation_type_id)}] -> (r)'
                f'WHERE d.id IN $entity_ids '
                f'return d.id, e, r$$, :params'
                f') as (id agtype, e agtype, n agtype);'
            )

        records = await self.fetch(
            query,
            {
                'params': json.dumps({
                    'entity_ids': entity_ids
                })
            },
            age=True
        )

        # group records on entity type of domain (inverse) or range
        grouped_records = {}
        for record in records:
            label = json.loads(record['n'][:-8])['label']
            # strip n_ from label, convert underscores to dashes
            etid = utd(label[2:])
            if etid not in grouped_records:
                grouped_records[etid] = []
            grouped_records[etid].append(record)

        rtpm = await self._conf_repo.get_relation_type_property_mapping(self._project_name, relation_type_name)
        results = {}

        for etid, records in grouped_records.items():
            etn = await self._conf_repo.get_entity_type_name_by_id(self._project_name, etid)
            etpm = await self._conf_repo.get_entity_type_property_mapping(self._project_name, etn)
            for record in records:
                entity_id = int(record['id'])
                if entity_id not in results:
                    results[entity_id] = []

                result = {}

                # relation properties
                # strip ::edge from record data
                relation_properties = json.loads(record['e'][:-6])['properties']
                result['relation'] = {rtpm[k]: v for k, v in relation_properties.items() if k in rtpm}

                # entity properties
                # strip ::vertex from record data
                entity_properties = json.loads(record['n'][:-8])['properties']
                result['entity'] = {etpm[k]: v for k, v in entity_properties.items() if k in etpm}
                result['entity_type_name'] = etn

                results[entity_id].append(result)

        return results

    async def get_entity_ids_by_type_name(
        self,
        entity_type_name,
    ) -> typing.List:
        project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH (n:n_{dtu(entity_type_id)}) '
            f'WITH n.id as id '
            f'ORDER BY n.id '
            f'return id$$'
            f') as (id agtype);'
        )
        records = await self.fetch(
            query,
            age=True
        )
        return [r['id'] for r in records]

    @staticmethod
    def convert_from_jsonb(jsonb: str) -> typing.Any:
        if jsonb is None:
            return None
        return json.loads(jsonb)

    async def get_entity_data(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        query: typing.Dict,
    ) -> typing.Dict[int, typing.Dict]:
        if not entity_ids:
            return {}

        if not query:
            return {}

        entity_type_ids = {}
        mappings = {}
        results = {}

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
