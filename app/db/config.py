from typing import Dict

from aiocache import cached
from fastapi import HTTPException
from numbers import Integral

from app.db.base import BaseRepository
from app.cache.core import key_builder


class ConfigRepository(BaseRepository):
    @cached(key_builder=key_builder)
    async def _get_project_config(self) -> Dict:
        records = await self.fetch(
            '''
                SELECT
                    project.id,
                    project.system_name,
                    project.display_name
                FROM app.project;
            ''',
        )

        result = {}
        for record in records:
            result[record['system_name']] = {
                'id': record['id'],
                'system_name': record['system_name'],
                'display_name': record['display_name'],
            }

        return result

    @cached(key_builder=key_builder)
    async def get_project_id_by_name(self, project_name: str) -> int:
        project_config = await self._get_project_config()

        if (
            project_name not in project_config or
            'id' not in project_config[project_name] or
            not isinstance(project_config[project_name]['id'], Integral)
        ):
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

        return project_config[project_name]['id']

    @cached(key_builder=key_builder)
    async def get_project_by_system_name(self, project_name: str) -> int:
        project_config = await self._get_project_config()

        if (
            project_name not in project_config or
            'id' not in project_config[project_name] or
            not isinstance(project_config[project_name]['id'], Integral)
        ):
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

        return project_config[project_name]

    @cached(key_builder=key_builder)
    async def _get_entity_type_config(self, project_name: str) -> Dict:
        # TODO use underscores for database columns
        records = await self.fetch(
            '''
                SELECT
                    entity.id,
                    entity.system_name,
                    entity.display_name,
                    entity.config
                FROM app.entity
                INNER JOIN app.project ON entity.project_id = project.id
                WHERE project.system_name = :project_name;
            ''',
            project_name=project_name,
        )

        result = {}
        for record in records:
            result[record['system_name']] = {
                'id': record['id'],
                'display_name': record['display_name'],
                'config': record['config'],
            }

        return result

    @cached(key_builder=key_builder)
    async def get_entity_type_id_by_name(self, project_name: str, entity_type_name: str) -> int:
        entity_type_config = await self._get_entity_type_config(project_name)

        if (
            entity_type_name not in entity_type_config or
            'id' not in entity_type_config[entity_type_name] or
            not isinstance(entity_type_config[entity_type_name]['id'], Integral)
        ):
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found',
            )

        return entity_type_config[entity_type_name]['id']
