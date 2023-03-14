from __future__ import annotations

import json
import typing

import aiocache
import asyncpg

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

    async def get_type_id_from_graph_id(
        self,
        project_id: str,
        graph_id: str,
        connection: asyncpg.connection.Connection = None,
    ) -> str:
        """
        Get the entity or relation type id from a graph id.
        This data can be retrieved from the name column in the ag_catalog.ag_label table by using the id column.
        The value from this id column can be retrieved from the graph_id by doing a right bitshift by (32+16) places.
        The actual lookup is performed in get_type_id_by_label_id so it can be cached.
        """
        return await self._get_type_id_by_label_id(
            project_id,
            int(graph_id) >> (32 + 16),
            connection,
        )

    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def _get_type_id_by_label_id(
        self,
        project_id: str,
        label_id: int,
        connection: asyncpg.connection.Connection = None,
    ) -> str:
        graph_id = await self._get_graph_id(project_id, connection)
        raw_type_id = await self.fetchval(
            (
                "SELECT name "
                "FROM ag_label "
                "WHERE graph = :graph_id AND id = :label_id;"
            ),
            {
                "graph_id": graph_id,
                "label_id": label_id,
            },
            age=True,
            connection=connection,
        )
        if raw_type_id == "_source_":
            return raw_type_id
        # n_uuid or e_uuid
        return utd(raw_type_id[2:])

    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def _get_graph_id(
        self,
        project_id: str,
        connection: asyncpg.connection.Connection = None,
    ) -> str:
        self.__class__._check_valid_label(project_id)
        return await self.fetchval(
            ("SELECT graph " "FROM ag_label " "WHERE relation = :relation::regclass;"),
            {"relation": f'"{project_id}"._ag_label_vertex'},
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
            f"SELECT i.id, n.properties "
            f'FROM "{project_id}".n_{dtu(entity_type_id)} n '
            f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} i '
            f"ON n.id = i.nid "
            f"WHERE i.id = ANY(:entity_ids);"
        )

        records = await self.fetch(
            query,
            {
                "entity_ids": entity_ids,
            },
            age=True,
            connection=connection,
        )

        return records

    @staticmethod
    def nullable(value) -> bool:
        if value is None:
            return True
        if isinstance(value, (str, list, dict)):
            return len(value) == 0
        if isinstance(value, int):
            return False
        print(value)
        raise Exception("Instance type not yet implemented.")

    async def post_entity(
        self,
        project_id: str,
        entity_type_id: str,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        async def execute_in_transaction(
            inner_connection: asyncpg.connection.Connection,
        ):
            clean_input = {
                k: v for k, v in input.items() if not DataRepository.nullable(v)
            }
            async with inner_connection.transaction():
                entity_id = await self.fetchval(
                    (
                        "UPDATE app.entity_count "
                        "SET current_id = current_id + 1 "
                        "WHERE id = :entity_type_id "
                        "RETURNING current_id;"
                    ),
                    {
                        "entity_type_id": entity_type_id,
                    },
                    connection=inner_connection,
                )

                clean_input["id"] = entity_id
                create_clause = ", ".join([f"{k}:${k}" for k in clean_input.keys()])

                query = (
                    f"SELECT * FROM cypher("
                    f"'{project_id}', "
                    f"$$CREATE (n:n_{dtu(entity_type_id)} {{{create_clause}}}) "
                    f"RETURN n$$, :params"
                    f") as (n agtype);"
                )

                record = await self.fetchval(
                    query,
                    {"params": json.dumps(clean_input)},
                    age=True,
                    connection=inner_connection,
                )

                # TODO: remove additional index when property indices are available (https://github.com/apache/incubator-age/issues/45)
                # strip off ::vertex
                parsed_record = json.loads(record[:-8])
                await self.execute(
                    (
                        f'INSERT INTO "{project_id}"._i_n_{dtu(entity_type_id)} '
                        f"(id, nid) "
                        f"VALUES (:id, :nid);"
                    ),
                    {
                        "id": parsed_record["properties"]["id"],
                        "nid": str(parsed_record["id"]),
                    },
                    connection=inner_connection,
                )

                return record

        # Make sure getting a new relation_id and inserting the relation
        # with this new id are executed in a single transation.
        if connection:
            return await execute_in_transaction(connection)
        else:
            async with self.connection() as new_connection:
                return await execute_in_transaction(new_connection)

    async def put_entity(
        self,
        project_id: str,
        entity_type_id: str,
        entity_id: int,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> asyncpg.Record:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        set = {k: v for k, v in input.items() if not DataRepository.nullable(v)}
        remove = [k for k, v in input.items() if DataRepository.nullable(v)]

        set_clause = ""
        if set:
            set_content = ", ".join([f"n.{k} = ${k}" for k in set.keys()])
            set_clause = f"SET {set_content} "

        remove_clause = ""
        if remove:
            remove_clause = "".join(f"REMOVE n.{k} " for k in remove)

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) "
            f"{set_clause}"
            f"{remove_clause}"
            f"RETURN n$$, :params"
            f") as (n agtype);"
        )

        record = await self.fetchrow(
            query,
            {
                "params": json.dumps(
                    {
                        "entity_id": entity_id,
                        **input,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        return record

    async def delete_raw_relations(
        self,
        project_id: str,
        relation_type_id: str,
        nids: typing.List[int],
        ids: typing.List[int],
        connection: asyncpg.connection.Connection = None,
    ) -> None:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)

        async def execute_in_transaction(
            inner_connection: asyncpg.connection.Connection,
        ):
            async with inner_connection.transaction():
                if relation_type_id == "_source_":
                    await self.execute(
                        (
                            f"DELETE "
                            f'FROM "{project_id}"._source_ '
                            f"WHERE id = ANY(:nids);"
                        ),
                        {
                            "nids": nids,
                        },
                        age=True,
                        connection=connection,
                    )
                else:
                    await self.execute(
                        (
                            f"DELETE "
                            f'FROM "{project_id}".e_{dtu(relation_type_id)} '
                            f"WHERE id = ANY(:nids);"
                        ),
                        {
                            "nids": nids,
                        },
                        age=True,
                        connection=connection,
                    )

                    # Delete relation entity to enable source relations
                    await self.execute(
                        (
                            f"DELETE "
                            f'FROM "{project_id}".en_{dtu(relation_type_id)} '
                            f"WHERE id = ANY(:nids);"
                        ),
                        {"nids": nids},
                        age=True,
                        connection=connection,
                    )

                    # TODO: remove additional index when property indices are available (https://github.com/apache/incubator-age/issues/45)
                    await self.execute(
                        (
                            f"DELETE "
                            f'FROM "{project_id}"._i_en_{dtu(relation_type_id)} '
                            f"WHERE id = ANY(:ids);"
                        ),
                        {"ids": ids},
                        age=True,
                        connection=connection,
                    )

        # Make sure all statements to delete a relation are executed in a single transation.
        if connection:
            return await execute_in_transaction(connection)
        else:
            async with self.connection() as new_connection:
                return await execute_in_transaction(new_connection)

    async def delete_entity(
        self,
        project_id: str,
        entity_type_id: str,
        entity_id: int,
        connection: asyncpg.connection.Connection = None,
    ) -> asyncpg.Record:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        async def execute_in_transaction(
            inner_connection: asyncpg.connection.Connection,
        ):
            async with inner_connection.transaction():
                query = (
                    f"SELECT * FROM cypher("
                    f"'{project_id}', "
                    f"$$MATCH (n:n_{dtu(entity_type_id)} {{id: $entity_id}}) "
                    f"DELETE n "
                    f"RETURN n$$, :params"
                    f") as (n agtype);"
                )

                # Delete relation entity to enable source relations
                record = await self.fetchrow(
                    query,
                    {
                        "params": json.dumps(
                            {
                                "entity_id": entity_id,
                            }
                        )
                    },
                    age=True,
                    connection=connection,
                )

                # TODO: remove additional index when property indices are available (https://github.com/apache/incubator-age/issues/45)
                await self.execute(
                    (
                        f'DELETE FROM "{project_id}"._i_n_{dtu(entity_type_id)} '
                        f"WHERE id = :id;"
                    ),
                    {
                        "id": entity_id,
                    },
                    connection=inner_connection,
                )

                return record

        # Make sure getting a new relation_id and inserting the relation
        # with this new id are executed in a single transation.
        if connection:
            return await execute_in_transaction(connection)
        else:
            async with self.connection() as new_connection:
                return await execute_in_transaction(new_connection)

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
                f"SELECT ri.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties "
                f'FROM "{project_id}".n_{dtu(entity_type_id)} r '
                f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} ri '
                f"ON r.id = ri.nid "
                f'INNER JOIN "{project_id}".{relation_label(relation_type_id)} e '
                f"ON r.id = e.end_id "
                f'INNER JOIN "{project_id}"._ag_label_vertex n '
                f"ON e.start_id = n.id "
                f"WHERE ri.id = ANY(:entity_ids);"
            )
        else:
            query = (
                f"SELECT di.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties "
                f'FROM "{project_id}".n_{dtu(entity_type_id)} d '
                f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} di '
                f"ON d.id = di.nid "
                f'INNER JOIN "{project_id}".{relation_label(relation_type_id)} e '
                f"ON d.id = e.start_id "
                f'INNER JOIN "{project_id}"._ag_label_vertex n '
                f"ON e.end_id = n.id "
                f"WHERE di.id = ANY(:entity_ids);"
            )
        records = await self.fetch(
            query,
            {
                "entity_ids": entity_ids,
            },
            age=True,
            connection=connection,
        )

        return records

    async def get_all_entity_relations(
        self,
        project_id: str,
        entity_type_id: str,
        entity_id: int,
        connection: asyncpg.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(entity_type_id)

        query = (
            f"SELECT e.start_id, d.properties as start_properties, e.id, e.properties, e.end_id, r.properties as end_properties "
            f'FROM "{project_id}".n_{dtu(entity_type_id)} d '
            f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} di '
            f"ON d.id = di.nid "
            f'INNER JOIN "{project_id}"._ag_label_edge e '
            f"ON d.id = e.start_id "
            f'INNER JOIN "{project_id}"._ag_label_vertex r '
            f"ON e.end_id = r.id "
            f"WHERE di.id = :entity_id;"
        )

        records = await self.fetch(
            query,
            {
                "entity_id": entity_id,
            },
            age=True,
            connection=connection,
        )

        query = (
            f"SELECT e.start_id, d.properties as start_properties, e.id, e.properties, e.end_id, r.properties as end_properties "
            f'FROM "{project_id}".n_{dtu(entity_type_id)} r '
            f'INNER JOIN "{project_id}"._i_n_{dtu(entity_type_id)} ri '
            f"ON r.id = ri.nid "
            f'INNER JOIN "{project_id}"._ag_label_edge e '
            f"ON r.id = e.end_id "
            f'INNER JOIN "{project_id}"._ag_label_vertex d '
            f"ON e.start_id = d.id "
            f"WHERE ri.id = :entity_id;"
        )

        range_records = await self.fetch(
            query,
            {
                "entity_id": entity_id,
            },
            age=True,
            connection=connection,
        )

        # prevent duplicates (relation from a node to itself)
        relation_ids = set([domain_record["id"] for domain_record in records])
        for range_record in range_records:
            if range_record["id"] not in relation_ids:
                records.append(range_record)

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

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH ()-[e:e_{dtu(relation_type_id)} {{id: $relation_id}}]->() "
            f"RETURN e$$, :params"
            f") as (e agtype);"
        )
        record = await self.fetchval(
            query,
            {
                "params": json.dumps(
                    {
                        "relation_id": relation_id,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        if record is None:
            return None

        # strip off ::edge
        properties = json.loads(record[:-6])["properties"]
        return {
            "id": properties["id"],
            "properties": properties,
        }

    async def delete_relation_sources(
        self,
        project_id: str,
        relation_type_id: str,
        relation_id: int,
        connection: asyncpg.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH (en:en_{dtu(relation_type_id)} {{id: $relation_id}})-[e:_source_]->(s) "
            f"DELETE e "
            f"RETURN e, s$$, :params"
            f") as (e agtype, s agtype);"
        )
        records = await self.fetch(
            query,
            {
                "params": json.dumps(
                    {
                        "relation_id": relation_id,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        return records

    async def post_relation(
        self,
        project_id: str,
        relation_type_id: str,
        start_entity_type_id: str,
        start_entity_id: int,
        end_entity_type_id: str,
        end_entity_id: int,
        input: typing.Dict,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)

        async def execute_in_transaction(
            inner_connection: asyncpg.connection.Connection,
        ):
            clean_input = {
                k: v for k, v in input.items() if not DataRepository.nullable(v)
            }
            async with inner_connection.transaction():
                relation_id = await self.fetchval(
                    (
                        "UPDATE app.relation_count "
                        "SET current_id = current_id + 1 "
                        "WHERE id = :relation_type_id "
                        "RETURNING current_id;"
                    ),
                    {
                        "relation_type_id": relation_type_id,
                    },
                    connection=inner_connection,
                )

                clean_input["id"] = relation_id
                create_clause = ", ".join([f"{k}:${k}" for k in clean_input.keys()])

                query = (
                    f"SELECT * FROM cypher("
                    f"'{project_id}', "
                    f"$$MATCH (d:n_{dtu(start_entity_type_id)} {{id: $start_entity_id}}), "
                    f"(r:n_{dtu(end_entity_type_id)} {{id: $end_entity_id}}) "
                    f"CREATE (d)-[e:e_{dtu(relation_type_id)} {{{create_clause}}}]->(r) "
                    f"RETURN d, e, r$$, :params"
                    f") as (d agtype, e agtype, r agtype);"
                )

                record = await self.fetchrow(
                    query,
                    {
                        "params": json.dumps(
                            {
                                "start_entity_id": start_entity_id,
                                "end_entity_id": end_entity_id,
                                **clean_input,
                            }
                        )
                    },
                    age=True,
                    connection=inner_connection,
                )

                # Create relation entity to enable source relations
                # strip off ::edge
                parsed_record = json.loads(record["e"][:-6])
                relation_entity_record = await self.fetchval(
                    (
                        f"SELECT * FROM cypher("
                        f"'{project_id}', "
                        f"$$CREATE (en:en_{dtu(relation_type_id)} {{id: $id}}) "
                        f"RETURN en$$, :params"
                        f") as (en agtype);"
                    ),
                    {
                        "params": json.dumps(
                            {
                                "id": parsed_record["properties"]["id"],
                            }
                        )
                    },
                    age=True,
                    connection=inner_connection,
                )

                # TODO: remove additional index when property indices are available (https://github.com/apache/incubator-age/issues/45)
                # strip off ::vertex
                parsed_relation_entity_record = json.loads(relation_entity_record[:-8])
                await self.execute(
                    (
                        f'INSERT INTO "{project_id}"._i_en_{dtu(relation_type_id)} '
                        f"(id, nid) "
                        f"VALUES (:id, :nid);"
                    ),
                    {
                        "id": parsed_relation_entity_record["properties"]["id"],
                        "nid": str(parsed_relation_entity_record["id"]),
                    },
                    connection=inner_connection,
                )

                return record

        # Make sure getting a new relation_id and inserting the relation
        # with this new id are executed in a single transation.
        if connection:
            return await execute_in_transaction(connection)
        else:
            async with self.connection() as new_connection:
                return await execute_in_transaction(new_connection)

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

        set = {k: v for k, v in input.items() if not DataRepository.nullable(v)}
        remove = [k for k, v in input.items() if DataRepository.nullable(v)]

        set_clause = ""
        if set:
            set_content = ", ".join([f"e.{k} = ${k}" for k in set.keys()])
            set_clause = f"SET {set_content} "

        remove_clause = ""
        if remove:
            remove_clause = "".join(f"REMOVE e.{k} " for k in remove)

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH (d)-[e:e_{dtu(relation_type_id)} {{id: $relation_id}}]->(r) "
            f"{set_clause}"
            f"{remove_clause} "
            f"RETURN d, e, r$$, :params"
            f") as (d agtype, e agtype, r agtype);"
        )

        record = await self.fetchrow(
            query,
            {
                "params": json.dumps(
                    {
                        "relation_id": relation_id,
                        **input,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        return record

    async def delete_relation(
        self,
        project_id: str,
        relation_type_id: str,
        relation_id: int,
        connection: asyncpg.connection.Connection = None,
    ) -> typing.Dict:
        self.__class__._check_valid_label(project_id)
        self.__class__._check_valid_label(relation_type_id)

        async def execute_in_transaction(
            inner_connection: asyncpg.connection.Connection,
        ):
            async with inner_connection.transaction():
                query = (
                    f"SELECT * FROM cypher("
                    f"'{project_id}', "
                    f"$$MATCH (d)-[e:e_{dtu(relation_type_id)} {{id: $relation_id}}]->(r) "
                    f"DELETE e "
                    f"RETURN d, e, r$$, :params"
                    f") as (d agtype, e agtype, r agtype);"
                )

                record = await self.fetchrow(
                    query,
                    {
                        "params": json.dumps(
                            {
                                "relation_id": relation_id,
                            }
                        )
                    },
                    age=True,
                    connection=inner_connection,
                )

                # Delete relation entity to enable source relations
                await self.execute(
                    (
                        f"SELECT * FROM cypher("
                        f"'{project_id}', "
                        f"$$MATCH (en:en_{dtu(relation_type_id)} {{id: $id}}) "
                        f"DELETE en$$, :params"
                        f") as (en agtype);"
                    ),
                    {
                        "params": json.dumps(
                            {
                                "id": relation_id,
                            }
                        )
                    },
                    age=True,
                    connection=inner_connection,
                )

                # TODO: remove additional index when property indices are available (https://github.com/apache/incubator-age/issues/45)
                await self.execute(
                    (
                        f'DELETE FROM "{project_id}"._i_en_{dtu(relation_type_id)} '
                        f"WHERE id = :id;"
                    ),
                    {
                        "id": relation_id,
                    },
                    connection=inner_connection,
                )

                return record

        # Make sure all statements to delete a relation are executed in a single transation.
        if connection:
            return await execute_in_transaction(connection)
        else:
            async with self.connection() as new_connection:
                return await execute_in_transaction(new_connection)

    async def get_relations_sources(
        self,
        project_id: str,
        relation_type_id: str,
        relation_ids: typing.List[str],
        connection: asyncpg.connection.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        # TODO: use cypher query when property indices are available (https://github.com/apache/incubator-age/issues/45)
        query = (
            f"SELECT di.id, e.properties as e_properties, n.id as n_id, n.properties as n_properties "
            f'FROM "{project_id}".en_{dtu(relation_type_id)} d '
            f'INNER JOIN "{project_id}"._i_en_{dtu(relation_type_id)} di '
            f"ON d.id = di.nid "
            f'INNER JOIN "{project_id}"._source_ e '
            f"ON d.id = e.start_id "
            f'INNER JOIN "{project_id}"._ag_label_vertex n '
            f"ON e.end_id = n.id "
            f"WHERE di.id = ANY(:relation_ids);"
        )

        records = await self.fetch(
            query,
            {
                "relation_ids": relation_ids,
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
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH (n:n_{dtu(entity_type_id)}) "
            f"WITH n.id as id "
            f"ORDER BY n.id "
            f"RETURN id$$"
            f") as (id agtype);"
        )

        records = await self.fetch(
            query,
            age=True,
            connection=connection,
        )

        return [int(r["id"]) for r in records]

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

        cypher_path = ""
        last_index = len(path_parts) - 1
        for index, part in enumerate(path_parts):
            [direction, relation_type_id] = part.split("_")
            self.__class__._check_valid_label(relation_type_id)
            if direction == "$r":
                rel_start = "-"
                rel_end = "->"
            else:
                rel_start = "<-"
                rel_end = "-"

            if index == 0:
                node = f"(n:n_{dtu(start_entity_type_id)})"
            else:
                node = "()"

            if index == last_index:
                end_node = f"(\\:n_{dtu(entity_type_id)} {{id: $entity_id}})"
            else:
                end_node = ""

            cypher_path = f"{cypher_path}{node}{rel_start}[\\:e_{dtu(relation_type_id)}]{rel_end}{end_node}"

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH {cypher_path} "
            f"RETURN n.id$$, :params"
            f") as (id agtype);"
        )

        records = await self.fetch(
            query,
            {
                "params": json.dumps(
                    {
                        "entity_id": entity_id,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        return [int(r["id"]) for r in records]

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

        cypher_path = ""
        last_index = len(path_parts) - 1
        for index, part in enumerate(path_parts):
            [direction, relation_type_id] = part.split("_")
            self.__class__._check_valid_label(relation_type_id)
            if direction == "$r":
                rel_start = "-"
                rel_end = "->"
            else:
                rel_start = "<-"
                rel_end = "-"

            if index == 0:
                node = f"(n:n_{dtu(start_entity_type_id)})"
            else:
                node = "()"

            if index == last_index:
                relation = f"[\\:e_{dtu(relation_type_id)} {{id: $relation_id}}]"
                end_node = "()"
            else:
                relation = f"[\\:e_{dtu(relation_type_id)}]"
                end_node = ""

            cypher_path = f"{cypher_path}{node}{rel_start}{relation}{rel_end}{end_node}"

        query = (
            f"SELECT * FROM cypher("
            f"'{project_id}', "
            f"$$MATCH {cypher_path} "
            f"RETURN n.id$$, :params"
            f") as (id agtype);"
        )

        records = await self.fetch(
            query,
            {
                "params": json.dumps(
                    {
                        "relation_id": relation_id,
                    }
                )
            },
            age=True,
            connection=connection,
        )

        return [int(r["id"]) for r in records]
