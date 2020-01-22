from typing import Dict

from fastapi import HTTPException
from functools import lru_cache
from numbers import Integral

from app.db.base import BaseRepository

class ConfigRepository(BaseRepository):
    async def _get_project_config(self) -> Dict:
        if not 'projects' in self._config:
            print('get project config from db')
            records = await self.fetch(
                '''
                    SELECT
                        project.id,
                        project.systemName,
                        project.displayName
                    FROM app.project;
                ''',
            )
            self._config['projects'] = {}
            for record in records:
                self._config['projects'][record['systemname']] = {
                    'id': record['id'],
                    'display_name': record['displayname'],
                }

        return self._config['projects']

    async def get_project_id_by_name(self, project_name: str) -> int:
        project_config = await self._get_project_config()

        if (
            not project_name in project_config or
            not 'id' in project_config[project_name] or
            not isinstance(project_config[project_name]['id'], Integral)
        ):
            # TODO log message
            raise HTTPException(status_code=404, detail=f'Project "{project_name}" not found')

        return project_config[project_name]['id']

    def clear_project_config(self) -> None:
        if 'projects' in self._config:
            del self._config['projects']

    async def _get_entity_type_config(self, project_name: str) -> Dict:
        # TODO use self._config to cache entity config
        # TODO use underscores for database columns
        records = await self.fetch(
            '''
                SELECT
                    entity.id,
                    entity.systemName,
                    entity.displayName,
                    entity.config
                FROM app.entity
                INNER JOIN app.project ON entity.projectId = project.id
                WHERE project.systemName = :project_name;
            ''',
            project_name=project_name,
        )
        result = {}
        for record in records:
            result[record['systemname']] = {
                'id': record['id'],
                'display_name': record['displayname'],
                'config': record['config'],
            }
        return result;

    async def get_entity_type_id_by_name(self, project_name: str, entity_type_name: str) -> int:
        entity_type_config = await self._get_entity_type_config(project_name)

        if (
            not entity_type_name in entity_type_config or
            not 'id' in entity_type_config[entity_type_name] or
            not isinstance(entity_type_config[entity_type_name]['id'], Integral)
        ):
            # TODO log message
            raise HTTPException(status_code=404, detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found')

        return entity_type_config[entity_type_name]['id']

    def clear_cache_entity_type_config(self) -> None:
        self._get_entity_type_config.cache_clear()
        self.get_entity_type_id_by_name.cache_clear()
