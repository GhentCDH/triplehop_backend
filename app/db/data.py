from __future__ import annotations

import aiocache
import asyncpg
import json
import re
import typing

from app.cache.core import self_project_name_key_builder, skip_first_arg_key_builder
from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.utils import dtu, relation_label, utd

RE_LABEL_DOES_NOT_EXIST = re.compile(
    r'^label[ ][en]_[a-f0-9]{8}_[a-f0-9]{4}_4[a-f0-9]{3}_[89ab][a-f0-9]{3}_[a-f0-9]{12}[ ]does not exists$'
)


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
        )
        return utd(n_etid_with_underscores[2:])

    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def _get_graph_id(
        self,
        project_id: str,
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
        )

    async def get_entities(
        self,
        project_id: str,
        entity_type_id: str,
        entity_ids: typing.List[int],
    ) -> typing.List[asyncpg.Record]:
        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        query = (
            f'SELECT i.id, n.properties '
            f'FROM "{project_id}".n_{dtu(entity_type_id)} n '
            f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} i '
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
            return []

        return records

    async def put_entity(
        self,
        project_id: str,
        entity_type_id: str,
        entity_id: int,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        # TODO: log revision

        set_clause = ', '.join([f'n.{k} = ${k}' for k in input.keys()])

        query = (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) '
            f'SET {set_clause} '
            f'return n$$, :params'
            f') as (n agtype);'
        )
        try:
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

        # If no items have been added, the label does not exist
        except asyncpg.exceptions.FeatureNotSupportedError as e:
            if RE_LABEL_DOES_NOT_EXIST.match(e.message):
                return None
            raise e

        if record is None:
            return None

        # strip off ::vertex
        return json.loads(record['n'][:-8])['properties']

    async def get_relations(
        self,
        project_id: str,
        entity_type_id: str,
        entity_ids: typing.List[int],
        relation_type_id: str,
        inverse: bool = False,
    ) -> typing.List[asyncpg.Record]:
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
            return []

        return records

    async def get_relation_sources(
        self,
        project_id: str,
        relation_type_id: str,
        relation_ids: typing.List[str],
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
        try:
            records = await self.fetch(
                query,
                {
                    'relation_ids': relation_ids,
                },
                age=True,
            )
        # If no relations have been added, the relation table doesn't exist
        except asyncpg.exceptions.UndefinedTableError:
            return []

        return records

    async def get_entity_ids(
        self,
        project_id: str,
        entity_type_id: str,
    ) -> typing.List:
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
