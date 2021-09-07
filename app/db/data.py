from __future__ import annotations

import aiocache
# import asyncio
import asyncpg
import json
import re
import typing

from app.cache.core import key_builder
from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import dtu, relation_label, utd

RE_LABEL_DOES_NOT_EXIST = re.compile(
    r'^label[ ][en]_[a-f0-9]{8}_[a-f0-9]{4}_4[a-f0-9]{3}_[89ab][a-f0-9]{3}_[a-f0-9]{12}[ ]does not exists$'
)


class DataRepository(BaseRepository):
    def __init__(self, pool: asyncpg.pool.Pool, project_name: str) -> None:
        super().__init__(pool)
        self._conf_repo = ConfigRepository(pool)
        self._project_name = project_name
        self._project_id = None

    async def get_entity_type_id_from_vertex_graph_id(
        self,
        vertex_graph_id: str,
    ) -> str:
        '''
        Get the entity type id from a graph id.
        This data can be retrieved from the name column in the ag_catalog.ag_label table by using the id column.
        The value from this id column can be retrieved from the graph_id by doing a right bitshift by (32+16) places.
        The actual lookup is performed in get_entity_type_id_by_label_id so it can be cached.
        '''
        return await self.get_entity_type_id_by_label_id(int(vertex_graph_id) >> (32+16))

    @aiocache.cached(key_builder=key_builder)
    async def get_entity_type_id_by_label_id(
        self,
        label_id: int,
    ) -> str:
        graph_id = await self.get_graph_id()
        n_etid_with_underscores = await self.fetchval(
            (
                'SELECT name '
                'FROM ag_label '
                'WHERE graph = :graph_id AND id = :label_id;'
            ),
            {
                'graph_id': graph_id,
                'label_id': label_id,
            },
            age=True,
        )
        return utd(n_etid_with_underscores[2:])

    @aiocache.cached(key_builder=key_builder)
    async def get_graph_id(
        self,
    ) -> str:
        # TODO: set _project_id on init
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        return await self.fetchval(
            (
                'SELECT graph '
                'FROM ag_label '
                'WHERE relation = :relation::regclass;'
            ),
            {
                'relation': f'"{self._project_id}"._ag_label_vertex'
            },
            age=True,
        )

    async def get_entities_graphql(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        entity_type_id = await self._conf_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        raw_results = await self.get_entities_raw(entity_type_id, entity_ids)

        if not raw_results:
            return raw_results

        etpm = await self._conf_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {
            entity_id: {etpm[k]: v for k, v in raw_result['e_props'].items() if k in etpm}
            for entity_id, raw_result in raw_results.items()
        }

    # async def get_entity_raw(
    #     self,
    #     entity_type_id: str,
    #     entity_id: int,
    # ) -> typing.Dict:
    #     # TODO: set _project_id on init
    #     # TODO: only retrieve requested properties
    #     if not self._project_id:
    #         self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)

    #     query = (
    #         f'SELECT * FROM cypher('
    #         f'\'{self._project_id}\', '
    #         f'$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) '
    #         f'return n$$, :params'
    #         f') as (n agtype);'
    #     )
    #     try:
    #         record = await self.fetchrow(
    #             query,
    #             {
    #                 'params': json.dumps({
    #                     'entity_id': entity_id
    #                 })
    #             },
    #             age=True
    #         )
    #     # If no items have been added, the label does not exist
    #     except asyncpg.exceptions.FeatureNotSupportedError as e:
    #         if RE_LABEL_DOES_NOT_EXIST.match(e.message):
    #             return None

    #     properties = json.loads(record['n'][:-8])['properties']
    #     return {'e_props': properties}

    async def get_entities_raw(
        self,
        entity_type_id: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        # TODO: set _project_id on init
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        query = (
            f'SELECT i.id, n.properties '
            f'FROM "{self._project_id}".n_{dtu(entity_type_id)} n '
            f'INNER JOIN "{self._project_id}"._i_n_{dtu(entity_type_id)} i '
            f'ON n.id = i.nid '
            f'WHERE i.id = ANY(:entity_ids);'
        )
        try:
            records = await self.fetch(
                query,
                {
                    'entity_ids': entity_ids,
                },
                age=True,
            )
        # If no entities have been added, the entity table doesn't exist
        except asyncpg.exceptions.UndefinedTableError:
            return {}
        results = {
            record['id']: {
                'e_props': json.loads(record['properties'])
            }
            for record in records
        }
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
        etpma = await self._conf_repo.get_entity_type_property_mapping(self._project_name, '__all__')
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
                result = {
                    'relation': {
                        rtpm[k]: v
                        for k, v in raw_relation_result['r_props'].items()
                        if k in rtpm
                    },
                    'entity': {
                        etpm[k]: v
                        for k, v in raw_relation_result['e_props'].items()
                        if k in etpm
                    },
                    'entity_type_name': etd[etid]['etn'],
                }
                if relation_type_id == '_source_':
                    if 'properties' in result['relation']:
                        result['relation']['properties'] = [
                            etpma[f'p_{dtu(p)}']
                            for p in result['relation']['properties']
                            if f'p_{dtu(p)}' in etpma
                        ]
                results[entity_id].append(result)

        return results

    # async def get_relation_raw(
    #     self,
    #     entity_type_id: str,
    #     entity_id: int,
    #     relation_type_id: str,
    #     inverse: bool = False,
    # ) -> typing.Dict:
    #     '''
    #     Get relations and linked entity information starting from an entity type, entity id and a relation type.

    #     Return: Dict = {
    #         relation_id: {
    #             r_props: Dict, # relation properties
    #             e_props: Dict, # linked entity properties
    #             entity_type_id: str, # linked entity type
    #         }
    #     }
    #     '''
    #     # TODO: set _project_id on init
    #     # TODO: only retrieve requested properties
    #     if not self._project_id:
    #         self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)

    #     if inverse:
    #         query = (
    #             f'SELECT * FROM cypher('
    #             f'\'{self._project_id}\', '
    #             f'$$MATCH (n) -[e:e_{dtu(relation_type_id)}] -> (r:n_{dtu(entity_type_id)}) '
    #             f'WHERE r.id = $entity_id '
    #             f'return e, n$$, :params'
    #             f') as (e agtype, n agtype);'
    #         )
    #     else:
    #         query = (
    #             f'SELECT * FROM cypher('
    #             f'\'{self._project_id}\', '
    #             f'$$MATCH (d:n_{dtu(entity_type_id)}) -[e:e_{dtu(relation_type_id)}] -> (n) '
    #             f'WHERE d.id = $entity_id '
    #             f'return e, n$$, :params'
    #             f') as (e agtype, n agtype);'
    #         )
    #     try:
    #         records = await self.fetch(
    #             query,
    #             {
    #                 'params': json.dumps({
    #                     'entity_id': entity_id
    #                 })
    #             },
    #             age=True
    #         )
    #     # If no items have been added, the label does not exist
    #     except asyncpg.exceptions.FeatureNotSupportedError as e:
    #         if RE_LABEL_DOES_NOT_EXIST.match(e.message):
    #             return {}

    #     results = {}

    #     for record in records:
    #         # relation properties
    #         # strip ::edge from record data
    #         relation_properties = json.loads(record['e'][:-6])['properties']

    #         # entity properties
    #         # strip ::vertex from record data
    #         entity_properties = json.loads(record['n'][:-8])['properties']

    #         label = json.loads(record['n'][:-8])['label']
    #         # strip n_ from label, convert underscores to dashes
    #         etid = utd(label[2:])

    #         results[relation_properties['id']] = {
    #             'r_props': relation_properties,
    #             'e_props': entity_properties,
    #             'entity_type_id': etid
    #         }

    #     return results

    async def get_relations_raw(
        self,
        entity_type_id: str,
        entity_ids: typing.List[int],
        relation_type_id: str,
        inverse: bool = False,
    ) -> typing.Dict:
        '''
        Get relations and linked entity information starting from an entity type, entity ids and a relation type.

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
        if not self._project_id:
            self._project_id = await self._conf_repo.get_project_id_by_name(self._project_name)
        if inverse:
            query = (
                f'SELECT ri.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties '
                f'FROM "{self._project_id}".n_{dtu(entity_type_id)} r '
                f'INNER JOIN "{self._project_id}"._i_n_{dtu(entity_type_id)} ri '
                f'ON r.id = ri.nid '
                f'INNER JOIN "{self._project_id}".{relation_label(relation_type_id)} e '
                f'ON r.id = e.end_id '
                f'INNER JOIN "{self._project_id}"._ag_label_vertex n '
                f'ON e.start_id = n.id '
                f'WHERE ri.id = ANY(:entity_ids);'
            )
        else:
            query = (
                f'SELECT di.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties '
                f'FROM "{self._project_id}".n_{dtu(entity_type_id)} d '
                f'INNER JOIN "{self._project_id}"._i_n_{dtu(entity_type_id)} di '
                f'ON d.id = di.nid '
                f'INNER JOIN "{self._project_id}".{relation_label(relation_type_id)} e '
                f'ON d.id = e.start_id '
                f'INNER JOIN "{self._project_id}"._ag_label_vertex n '
                f'ON e.end_id = n.id '
                f'WHERE di.id = ANY(:entity_ids);'
            )
        try:
            records = await self.fetch(
                query,
                {
                    'entity_ids': entity_ids,
                },
                age=True,
            )
        # If no relations have been added, the relation table doesn't exist
        except asyncpg.exceptions.UndefinedTableError:
            return {}

        results = {}
        for record in records:
            id = record['id']
            relation_properties = json.loads(record['e_properties'])
            entity_properties = json.loads(record['n_properties'])
            etid = await self.get_entity_type_id_from_vertex_graph_id(record['n_id'])

            if id not in results:
                results[id] = {}

            results[id][relation_properties['id']] = {
                'r_props': relation_properties,
                'e_props': entity_properties,
                'entity_type_id': etid
            }

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
        try:
            records = await self.fetch(
                query,
                age=True
            )
        # If no items have been added, the label does not exist
        except asyncpg.exceptions.FeatureNotSupportedError as e:
            if RE_LABEL_DOES_NOT_EXIST.match(e.message):
                return []
        return [int(r['id']) for r in records]

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

        for relation_type_id in crdb_query['relations']:
            # get relation data
            raw_results = await self.get_relations_raw(
                entity_type_id,
                entity_ids,
                relation_type_id.split('_')[1],
                relation_type_id.split('_')[0] == 'ri'
            )
            for entity_id, raw_result in raw_results.items():
                if entity_id not in results:
                    results[entity_id] = {}
                if 'relations' not in results[entity_id]:
                    results[entity_id]['relations'] = {}
                results[entity_id]['relations'][relation_type_id] = raw_result

            # gather what further information is required
            rel_entities = {}
            raw_rel_results_per_entity_type_id = {}
            # mapping so results (identified by entity_type_name, entity_id)
            # can be added in the right place (identified by relation_type_id, relation_id)
            mapping = {}
            if crdb_query['relations'][relation_type_id]['relations']:
                for entity_id, raw_relation_results in raw_results.items():
                    for relation_id, raw_result in raw_relation_results.items():
                        rel_entity_type_id = raw_result['entity_type_id']
                        rel_entity_id = raw_result['e_props']['id']

                        if rel_entity_type_id not in rel_entities:
                            rel_entities[rel_entity_type_id] = set()
                        rel_entities[rel_entity_type_id].add(rel_entity_id)

                        if relation_type_id not in mapping:
                            mapping[relation_type_id] = {}
                        mapping[relation_type_id][relation_id] = [
                            rel_entity_type_id,
                            rel_entity_id,
                        ]

                # recursively obtain further relation data
                for rel_entity_type_id, rel_entity_ids in rel_entities.items():
                    raw_rel_results_per_entity_type_id[rel_entity_type_id] = await self.get_entity_data_raw(
                        rel_entity_type_id,
                        list(rel_entity_ids),
                        crdb_query['relations'][relation_type_id],
                        False,
                    )

                # add the additional relation data to the result
                for entity_id in results:
                    if 'relations' in results[entity_id] and relation_type_id in results[entity_id]['relations']:
                        for relation_id in results[entity_id]['relations'][relation_type_id]:
                            (rel_entity_type_id, rel_entity_id) = mapping[relation_type_id][relation_id]
                            if rel_entity_id in raw_rel_results_per_entity_type_id[rel_entity_type_id]:
                                results[entity_id]['relations'][relation_type_id][relation_id]['relations'] = \
                                    raw_rel_results_per_entity_type_id[rel_entity_type_id][rel_entity_id]['relations']

        return results
