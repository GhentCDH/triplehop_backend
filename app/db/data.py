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
        self._project_id = None

    async def get_entities_graphql(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        # TODO: set _project_id on init
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        raw_results = await self.get_entities_raw(entity_type_id, entity_ids)

        if not raw_results:
            return raw_results

        etpm = await self._conf_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {
            entity_id: {etpm[k]: v for k, v in raw_result['e_props'].items() if k in etpm}
            for entity_id, raw_result in raw_results.items()
        }

    async def get_entities_raw(
        self,
        entity_type_id: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        # TODO: set _project_id on init
        # TODO: only retrieve requested properties
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)

        query = (
            f'SELECT * FROM cypher('
            f'\'{self._project_id}\', '
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

        results = {}
        for record in records:
            # strip ::vertex from record data
            properties = json.loads(record['n'][:-8])['properties']

            results[properties['id']] = {'e_props': properties}
        return results

    async def get_relations_graphql(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        relation_type_name: str,
        inverse: bool = False,
    ) -> typing.Dict:
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)
        relation_type_id = await self._conf_repo.get_relation_type_id_by_name(self._project_name, relation_type_name)

        raw_results = await self.get_relations_raw(entity_type_id, entity_ids, relation_type_id, inverse)

        results = {}
        rtpm = await self._conf_repo.get_relation_type_property_mapping(self._project_name, relation_type_name)
        etd = {}
        for entity_id, raw_result in raw_results.items():
            results[entity_id] = []
            for raw_relation_result in raw_result.values():
                etid = raw_relation_result['entity_type_id']
                if etid not in etd:
                    etn = await self._conf_repo.get_entity_type_name_by_id(self._project_name, etid)
                    etd[etid] = {
                        'etn': etn,
                        'etpm': await self._conf_repo.get_entity_type_property_mapping(self._project_name, etn)
                    }
                etpm = etd[etid]['etpm']
                results[entity_id].append(
                    {
                        'relation': {rtpm[k]: v for k, v in raw_relation_result['r_props'].items() if k in rtpm},
                        'entity': {etpm[k]: v for k, v in raw_relation_result['e_props'].items() if k in etpm},
                        'entity_type_name': etd[etid]['etn'],
                    }
                )

        return results

    async def get_relations_raw(
        self,
        entity_type_id: str,
        entity_ids: typing.List[int],
        relation_type_id: str,
        inverse: bool = False,
    ) -> typing.Dict:
        '''
        Get relations and linked entity information starting from an entity type, an entity id and a relation type.

        Return: Dict = {
            entity_id: {
                relation_id: {
                    r_props: Dict, # relation properties
                    e_props: Dict, # linked entity properties
                    entity_type_id: str, # linked entity type
                }
            }
        }
        '''
        # TODO: set _project_id on init
        # TODO: only retrieve requested properties
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)

        if inverse:
            query = (
                f'SELECT * FROM cypher('
                f'\'{self._project_id}\', '
                f'$$MATCH (n) -[e:e_{dtu(relation_type_id)}] -> (r:n_{dtu(entity_type_id)}) '
                f'WHERE r.id IN $entity_ids '
                f'return r.id, e, n$$, :params'
                f') as (id agtype, e agtype, n agtype);'
            )
        else:
            query = (
                f'SELECT * FROM cypher('
                f'\'{self._project_id}\', '
                f'$$MATCH (d:n_{dtu(entity_type_id)}) -[e:e_{dtu(relation_type_id)}] -> (n)'
                f'WHERE d.id IN $entity_ids '
                f'return d.id, e, n$$, :params'
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

        results = {}

        for etid, records in grouped_records.items():
            for record in records:
                entity_id = int(record['id'])
                if entity_id not in results:
                    results[entity_id] = {}

                # relation properties
                # strip ::edge from record data
                relation_properties = json.loads(record['e'][:-6])['properties']
                if relation_properties['id'] not in results[entity_id]:
                    results[entity_id][relation_properties['id']] = {}
                results[entity_id][relation_properties['id']]['r_props'] = relation_properties

                # entity properties
                # strip ::vertex from record data
                entity_properties = json.loads(record['n'][:-8])['properties']
                results[entity_id][relation_properties['id']]['e_props'] = entity_properties
                results[entity_id][relation_properties['id']]['entity_type_id'] = etid

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

    async def get_entity_data(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        crdb_query: typing.Dict,
    ) -> typing.Dict[int, typing.Dict]:
        # TODO: document crdb_query format
        if not entity_ids:
            return {}

        if not crdb_query:
            raise Exception('Empty query')

        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        return await self.get_entity_data_raw(
            entity_type_id,
            entity_ids,
            crdb_query,
        )

    async def get_entity_data_raw(
        self,
        entity_type_id: str,
        entity_ids: typing.List[int],
        crdb_query: typing.Dict,
        first_iteration: bool = True
    ) -> typing.Dict:
        results = {}
        # start entity
        if first_iteration:
            # check if entity props are requested
            if crdb_query['e_props']:
                results = await self.get_entities_raw(entity_type_id, entity_ids)

        for relation in crdb_query['relations']:
            # get relation data
            raw_results = await self.get_relations_raw(
                entity_type_id,
                entity_ids,
                relation.split('_')[1],
                relation.split('_')[0] == 'ri'
            )
            for entity_id, raw_result in raw_results.items():
                if entity_id not in results:
                    results[entity_id] = {}
                if 'relations' not in results[entity_id]:
                    results[entity_id]['relations'] = {}
                results[entity_id]['relations'][relation] = raw_result

            # recursively obtain further relation data
            if crdb_query['relations'][relation]['relations']:
                related_entities = {}
                # mapping so results (identified by entity_type_name, entity_id)
                # can be added in the right place (identified by relation_id)
                mapping = {}
                for raw_relation_results in raw_results.values():
                    for relation_id, raw_result in raw_relation_results.items():
                        if raw_result['entity_type_id'] not in related_entities:
                            related_entities[raw_result['entity_type_id']] = set()
                        related_entities[raw_result['entity_type_id']].add(raw_result['e_props']['id'])

                        mapping_key = f'{raw_result["entity_type_id"]}|{raw_result["e_props"]["id"]}'
                        if mapping_key not in mapping:
                            mapping[mapping_key] = []
                        mapping[mapping_key].append(relation_id)

                    for related_entity_type_id, related_entity_ids in related_entities.items():
                        raw_related_results = await self.get_entity_data_raw(
                            related_entity_type_id,
                            list(related_entity_ids),
                            crdb_query['relations'][relation],
                            False,
                        )
                        for related_entity_id, raw_related_result in raw_related_results.items():
                            mapping_key = f'{related_entity_type_id}|{related_entity_id}'
                            for relation_id in mapping[mapping_key]:
                                results[entity_id]['relations'][relation][relation_id].update(raw_related_result)

        return results
