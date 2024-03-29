from __future__ import annotations

import typing

import asyncpg

from app.db.base import BaseRepository


class RevisionRepository(BaseRepository):
    async def get_new_revision_count(
        self,
        project_id: str,
        connection: asyncpg.connection.Connection,
    ) -> typing.Dict:
        return await self.fetchval(
            (
                "UPDATE revision.count "
                "SET current_id = current_id + 1 "
                "WHERE project_id = :project_id "
                "RETURNING current_id;"
            ),
            {
                "project_id": project_id,
            },
            connection=connection,
        )

    async def post_entities_revision(
        self,
        project_id: str,
        data: typing.List[typing.List],
        connection: asyncpg.connection.Connection,
    ) -> typing.Dict:
        await self.executemany(
            (
                f'INSERT INTO revision."{project_id}_entities" '
                f"(revision_id, user_id, entity_type_revision_id, entity_type_id, entity_id, old_value, new_value) "
                f"VALUES "
                f"(:revision_id, :user_id, :entity_type_revision_id, :entity_type_id, :entity_id, :old_value, :new_value)"
            ),
            data,
            connection=connection,
        )

    async def post_relations_revision(
        self,
        project_id: str,
        data: typing.List[typing.List],
        connection: asyncpg.connection.Connection,
    ) -> typing.Dict:
        await self.executemany(
            (
                f'INSERT INTO revision."{project_id}_relations" '
                f"("
                f"    revision_id,"
                f"    user_id,"
                f"    relation_type_revision_id,"
                f"    relation_type_id,"
                f"    relation_id,"
                f"    start_entity_type_revision_id,"
                f"    start_entity_type_id,"
                f"    start_entity_id,"
                f"    end_entity_type_revision_id,"
                f"    end_entity_type_id,"
                f"    end_entity_id,"
                f"    old_value,"
                f"    new_value"
                f") "
                f"VALUES "
                f"("
                f"    :revision_id,"
                f"    :user_id,"
                f"    :relation_type_revision_id,"
                f"    :relation_type_id,"
                f"    :relation_id,"
                f"    :start_entity_type_revision_id,"
                f"    :start_entity_type_id,"
                f"    :start_entity_id,"
                f"    :end_entity_type_revision_id,"
                f"    :end_entity_type_id,"
                f"    :end_entity_id,"
                f"    :old_value,"
                f"    :new_value"
                f")"
            ),
            data,
            connection=connection,
        )

    async def post_relation_sources_revision(
        self,
        project_id: str,
        data: typing.List[typing.List],
        connection: asyncpg.connection.Connection,
    ) -> typing.Dict:
        await self.executemany(
            (
                f'INSERT INTO revision."{project_id}_relation_sources" '
                f"("
                f"    revision_id,"
                f"    user_id,"
                f"    source_relation_type_revision_id,"
                f"    source_relation_type_id,"
                f"    source_relation_id,"
                f"    start_relation_type_revision_id,"
                f"    start_relation_type_id,"
                f"    start_relation_id,"
                f"    end_entity_type_revision_id,"
                f"    end_entity_type_id,"
                f"    end_entity_id,"
                f"    old_value,"
                f"    new_value"
                f") "
                f"VALUES "
                f"("
                f"    :revision_id,"
                f"    :user_id,"
                f"    :source_relation_type_revision_id,"
                f"    :source_relation_type_id,"
                f"    :source_relation_id,"
                f"    :start_relation_type_revision_id,"
                f"    :start_relation_type_id,"
                f"    :start_relation_id,"
                f"    :end_entity_type_revision_id,"
                f"    :end_entity_type_id,"
                f"    :end_entity_id,"
                f"    :old_value,"
                f"    :new_value"
                f")"
            ),
            data,
            connection=connection,
        )
