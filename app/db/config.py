import asyncpg
import typing

from app.db.base import BaseRepository


class ConfigRepository(BaseRepository):
    async def get_projects_config(self, connection: asyncpg.Connection = None) -> typing.List[asyncpg.Record]:
        return await self.fetch(
            '''
                SELECT
                    project.id::text,
                    project.system_name,
                    project.display_name
                FROM app.project;
            ''',
            connection=connection,
        )

    async def get_entity_types_config(
        self,
        project_name: str,
        connection: asyncpg.Connection = None,
    ) -> typing.List[asyncpg.Record]:
        return await self.fetch(
            '''
                SELECT
                    entity.id::text,
                    entity.system_name,
                    entity.display_name,
                    entity.config
                FROM app.entity
                INNER JOIN app.project ON entity.project_id = project.id
                WHERE project.system_name = :project_name
                AND entity.system_name != '__all__';
            ''',
            {
                'project_name': project_name,
            },
            connection=connection,
        )

    async def get_current_entity_type_revision_id(
        self,
        entity_type_id: str,
        connection: asyncpg.Connection = None,
    ) -> str:
        return await self.fetchval(
            '''
                SELECT id::text
                FROM app.entity_revision
                WHERE entity_id = :entity_type_id
                ORDER BY created DESC
                LIMIT 1;
            ''',
            {
                'entity_type_id': entity_type_id,
            },
            connection=connection,
        )

    async def get_relation_types_config(
        self,
        project_name: str,
        connection: asyncpg.Connection = None
    ) -> typing.List[asyncpg.Record]:
        return await self.fetch(
            '''
                SELECT
                    relation.id::text,
                    relation.system_name,
                    relation.display_name,
                    relation.config,
                    array_agg(domain.system_name) as domain_names,
                    array_agg(range.system_name) as range_names
                FROM app.relation
                INNER JOIN app.project ON relation.project_id = project.id
                INNER JOIN app.relation_domain ON relation.id = relation_domain.relation_id
                INNER JOIN app.entity domain ON relation_domain.entity_id = domain.id
                INNER JOIN app.relation_range ON relation.id = relation_range.relation_id
                INNER JOIN app.entity range ON relation_range.entity_id = range.id
                WHERE project.system_name = :project_name
                GROUP BY (relation.id);
            ''',
            {
                'project_name': project_name,
            },
            connection=connection,
        )

    async def get_current_relation_type_revision_id(
        self,
        relation_type_id: str,
        connection: asyncpg.Connection = None,
    ) -> str:
        return await self.fetchval(
            '''
                SELECT id::text
                FROM app.relation_revision
                WHERE relation_id = :relation_type_id
                ORDER BY created DESC
                LIMIT 1;
            ''',
            {
                'relation_type_id': relation_type_id,
            },
            connection=connection,
        )
