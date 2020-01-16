from fastapi import HTTPException
from functools import lru_cache
from typing import Dict
from numbers import Integral

from app.db.base import BaseRepository

class ConfigRepository(BaseRepository):
    @lru_cache()
    async def _get_project_config(self) -> Dict:
        # TODO actually request database
        return {
            'cinecos': {
                'id': 1,
                'display_name': 'Cinecos',
            }
        }

    @lru_cache()
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

    def clear_cache_project_config(self) -> None:
        self._get_project_config.cache_clear()
        self.get_project_id_by_name.cache_clear()

    @lru_cache()
    async def _get_entity_type_config(self, project_name: str) -> Dict:
        # TODO actually request database
        return {
            'film': {
                'id': 1,
                'display_name': 'Film',
                'config': {
                    0: {
                        "systemName": "title",
                        "displayName": "Title",
                    },
                    1: {
                        "systemName": "year",
                        "displayName": "Year",
                    },
                },
            },
        }

    @lru_cache()
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
