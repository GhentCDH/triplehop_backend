from __future__ import annotations

import asyncpg
import typing

from app.db.base import BaseRepository


class RevisionRepository(BaseRepository):
    async def get_new_revision_count(
        self,
        project_id: str,
        connection: asyncpg.connection.Connection,
    ) -> typing.Dict:
        return await self.fetchval(
            (
                'UPDATE revision.count '
                'SET current_id = current_id + 1 '
                'WHERE project_id = :project_id '
                'RETURNING current_id;'
            ),
            {
                'project_id': project_id,
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
                f'(revision_id, user_id, entity_type_revision_id, entity_type_id, entity_id, old_value, new_value) '
                f'VALUES '
                f'(:revision_id, :user_id, :entity_type_revision_id, :entity_type_id, :entity_id, :old_value, :new_value)'
            ),
            data,
            connection=connection,
        )
