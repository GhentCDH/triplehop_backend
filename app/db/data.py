from __future__ import annotations

import aiocache
import asyncpg
import json
import typing

from app.cache.core import skip_first_arg_key_builder
from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import dtu, relation_label, utd


class DataRepository(BaseRepository):
    def __init__(
        self,
        pool: asyncpg.pool.Pool,
    ) -> None:
        super().__init__(pool)
        self._conf_repo = ConfigRepository(pool)

    async def get_entity_type_id_from_vertex_graph_id(
        self,
        project_id: str,
        vertex_graph_id: str,
    ) -> str:
        '''
        Get the entity type id from a graph id.
        This data can be retrieved from the name column in the ag_catalog.ag_label table by using the id column.
        The value from this id column can be retrieved from the graph_id by doing a right bitshift by (32+16) places.
        The actual lookup is performed in get_entity_type_id_by_label_id so it can be cached.
        '''
        return await self._get_entity_type_id_by_label_id(
            project_id,
            int(vertex_graph_id) >> (32+16),
        )

    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def _get_entity_type_id_by_label_id(
        self,
        project_id: str,
        label_id: int,
        connection: asyncpg.connection.Connection = None,
    ) -> str:
        graph_id = await self._get_graph_id(project_id)
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
            connection=connection,
        )
        return utd(n_etid_with_underscores[2:])

    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def _get_graph_id(
        self,
        project_id: str,
        connection: asyncpg.connection.Connection = None,
    ) -> str:
        return await self.fetchval(
            (
                'SELECT graph '
                'FROM ag_label '
                'WHERE relation = :relation::regclass;'
            ),
            {
                'relation': f'"{project_id}"._ag_label_vertex'
            },
            age=True,
            connection=connection,
        )

    async def get_entities(
        self,
        project_id: str,
        entity_type_id: str,
        entity_ids: typing.List[int],
        connection: asyncpg.connection.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        query = (
            f'SELECT i.id, n.properties '
            f'FROM "{project_id}".n_{dtu(entity_type_id)} n '
            f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} i '
            f'ON n.id = i.nid '
            f'WHERE i.id = ANY(:entity_ids);'
        )

        records = await self.fetch(
            query,
            {
                'entity_ids': entity_ids,
            },
            age=True,
            connection=connection,
        )


        return records

    async def put_entity(
        self,
        project_id: str,
        entity_type_id: str,
        entity_id: int,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        set_clause = ', '.join([f'n.{k} = ${k}' for k in input.keys()])

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) '
            f'SET {set_clause} '
            f'return n$$, :params'
            f') as (n agtype);'
        )

        record = await self.fetchrow(
            query,
            {
                'params': json.dumps({
                    'entity_id': entity_id,
                    **input,
                })
            },
            age=True,
            connection=connection,
        )

        return record

    async def get_relations_from_start_entities(
        self,
        project_id: str,
        entity_type_id: str,
        entity_ids: typing.List[int],
        relation_type_id: str,
        inverse: bool = False,
        connection: asyncpg.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)
        self.__class__._check_valid_label(relation_type_id)
        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        if inverse:
            query = (
                f'SELECT ri.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties '
                f'FROM "{project_id}".n_{dtu(entity_type_id)} r '
                f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} ri '
                f'ON r.id = ri.nid '
                f'INNER JOIN "{project_id}".{relation_label(relation_type_id)} e '
                f'ON r.id = e.end_id '
                f'INNER JOIN "{project_id}"._ag_label_vertex n '
                f'ON e.start_id = n.id '
                f'WHERE ri.id = ANY(:entity_ids);'
            )
        else:
            query = (
                f'SELECT di.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties '
                f'FROM "{project_id}".n_{dtu(entity_type_id)} d '
                f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} di '
                f'ON d.id = di.nid '
                f'INNER JOIN "{project_id}".{relation_label(relation_type_id)} e '
                f'ON d.id = e.start_id '
                f'INNER JOIN "{project_id}"._ag_label_vertex n '
                f'ON e.end_id = n.id '
                f'WHERE di.id = ANY(:entity_ids);'
            )
        records = await self.fetch(
            query,
            {
                'entity_ids': entity_ids,
            },
            age=True,
            connection=connection,
        )

        return records

    # Return {id: int, properties: {})}
    async def get_relation(
        self,
        project_id: str,
        relation_type_id: str,
        relation_id: int,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)
        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH ()-[e:e_{dtu(relation_type_id)} {{id: $relation_id}}]->() '
            f'return e$$, :params'
            f') as (e agtype);'
        )
        record = await self.fetchval(
            query,
            {
                'params': json.dumps({
                    'relation_id': relation_id,
                })
            },
            age=True,
            connection=connection,
        )

        # strip off ::edge
        properties = json.loads(record[:-6])['properties']
        return {
            'id': properties['id'],
            'properties': properties,
        }

    async def put_relation(
        self,
        project_id: str,
        relation_type_id: str,
        relation_id: int,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)

        set_clause = ', '.join([f'e.{k} = ${k}' for k in input.keys()])

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH ()-[e:e_{dtu(relation_type_id)} {{id: $relation_id}}]->() '
            f'SET {set_clause} '
            f'return e$$, :params'
            f') as (e agtype);'
        )

        record = await self.fetchrow(
            query,
            {
                'params': json.dumps({
                    'relation_id': relation_id,
                    **input,
                })
            },
            age=True,
            connection=connection,
        )

        return record

    async def get_relation_sources(
        self,
        project_id: str,
        relation_type_id: str,
        relation_ids: typing.List[str],
        connection: asyncpg.connection.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        query = (
            f'SELECT di.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties '
            f'FROM "{project_id}".en_{dtu(relation_type_id)} d '
            f'INNER JOIN "{project_id}"._i_en_{dtu(relation_type_id)} di '
            f'ON d.id = di.nid '
            f'INNER JOIN "{project_id}"._source_ e '
            f'ON d.id = e.start_id '
            f'INNER JOIN "{project_id}"._ag_label_vertex n '
            f'ON e.end_id = n.id '
            f'WHERE di.id = ANY(:relation_ids);'
        )

        records = await self.fetch(
            query,
            {
                'relation_ids': relation_ids,
            },
            age=True,
            connection=connection,
        )

        return records

    async def get_entity_ids(
        self,
        project_id: str,
        entity_type_id: str,
        connection: asyncpg.Connection = None,
    ) -> typing.List[int]:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

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
            age=True,
            connection=connection,
        )

        return [int(r['id']) for r in records]

    async def find_entities_linked_to_entity(
        self,
        project_id: str,
        start_entity_type_id: str,
        entity_type_id: str,
        entity_id: int,
        path_parts: typing.List[str],
        connection: asyncpg.Connection = None,
    ) -> typing.List:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(start_entity_type_id)
        self.__class__._check_valid_label(entity_type_id)

        cypher_path = ''
        last_index = len(path_parts) - 1
        for index, part in enumerate(path_parts):
            [direction, relation_type_id] = part.split('_')
            self.__class__._check_valid_label(relation_type_id)
            if direction == '$r':
                rel_start = '-'
                rel_end = '->'
            else:
                rel_start = '<-'
                rel_end = '-'

            if index == 0:
                node = f'(n:n_{dtu(start_entity_type_id)})'
            else:
                node = '()'

            if index == last_index:
                end_node = f'(\\:n_{dtu(entity_type_id)} {{id: $entity_id}})'
            else:
                end_node = ''

            cypher_path = f'{cypher_path}{node}{rel_start}[\\:e_{dtu(relation_type_id)}]{rel_end}{end_node}'

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH {cypher_path} '
            f'return n.id$$, :params'
            f') as (id agtype);'
        )

        records = await self.fetch(
            query,
            {
                'params': json.dumps({
                    'entity_id': entity_id,
                })
            },
            age=True,
            connection=connection,
        )

        return [int(r['id']) for r in records]

    async def find_entities_linked_to_relation(
        self,
        project_id: str,
        start_entity_type_id: str,
        end_relation_type_id: str,
        relation_id: int,
        path_parts: typing.List[str],
        connection: asyncpg.Connection = None,
    ) -> typing.List:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(start_entity_type_id)
        self.__class__._check_valid_label(end_relation_type_id)

        cypher_path = ''
        last_index = len(path_parts) - 1
        for index, part in enumerate(path_parts):
            [direction, relation_type_id] = part.split('_')
            self.__class__._check_valid_label(relation_type_id)
            if direction == '$r':
                rel_start = '-'
                rel_end = '->'
            else:
                rel_start = '<-'
                rel_end = '-'

            if index == 0:
                node = f'(n:n_{dtu(start_entity_type_id)})'
            else:
                node = '()'

            if index == last_index:
                relation = f'[\\:e_{dtu(relation_type_id)} {{id: $relation_id}}]'
                end_node = '()'
            else:
                relation = f'[\\:e_{dtu(relation_type_id)}]'
                end_node = ''

            cypher_path = f'{cypher_path}{node}{rel_start}{relation}{rel_end}{end_node}'

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH {cypher_path} '
            f'return n.id$$, :params'
            f') as (id agtype);'
        )

        records = await self.fetch(
            query,
            {
                'params': json.dumps({
                    'relation_id': relation_id,
                })
            },
            age=True,
            connection=connection,
        )

        return [int(r['id']) for r in records]
