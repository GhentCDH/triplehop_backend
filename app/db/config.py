from typing import Dict

from aiocache import cached
from fastapi import HTTPException
from json import loads as json_load

from app.cache.core import key_builder
from app.db.base import BaseRepository
from app.db.core import dtu


class ConfigRepository(BaseRepository):
    # TODO: delete cache on project config update
    @cached(key_builder=key_builder)
    async def _get_projects_config(self) -> Dict:
        records = await self.fetch(
            '''
                SELECT
                    project.id::text,
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

    # TODO: delete cache on project config update
    @cached(key_builder=key_builder)
    async def get_project_id_by_name(self, project_name: str) -> int:
        project_config = await self._get_projects_config()

        try:
            return project_config[project_name]['id']
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

    # TODO: delete cache on project config update
    @cached(key_builder=key_builder)
    async def get_project_config(self, project_name: str) -> int:
        project_config = await self._get_projects_config()

        try:
            return project_config[project_name]
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

    # TODO: delete cache on entity config update
    @cached(key_builder=key_builder)
    async def get_entity_types_config(self, project_name: str) -> Dict:
        records = await self.fetch(
            '''
                SELECT
                    entity.id::text,
                    entity.system_name,
                    entity.display_name,
                    entity.config
                FROM app.entity
                INNER JOIN app.project ON entity.project_id = project.id
                WHERE project.system_name = :project_name;
            ''',
            {
                'project_name': project_name,
            }
        )

        result = {}
        for record in records:
            result[record['system_name']] = {
                'id': record['id'],
                'display_name': record['display_name'],
                'config': json_load(record['config']),
            }

        return result

    # TODO: delete cache on entity config update
    @cached(key_builder=key_builder)
    async def get_entity_type_property_mapping(self, project_name: str, entity_type_name: str) -> Dict:
        entity_types_config = await self.get_entity_types_config(project_name)

        try:
            entity_type_config = entity_types_config[entity_type_name]
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found',
            )

        properties_config = entity_type_config['config']['data']

        # leave the id property intact
        result = {'id': 'id'}
        for property_config_id, property_config in properties_config.items():
            result[f'p_{dtu(entity_type_config["id"])}_{property_config_id}'] = property_config['system_name']

        return result

    # TODO: delete cache on entity config update
    @cached(key_builder=key_builder)
    async def get_entity_type_id_by_name(self, project_name: str, entity_type_name: str) -> int:
        entity_types_config = await self.get_entity_types_config(project_name)

        try:
            return entity_types_config[entity_type_name]['id']
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found',
            )

    # TODO: delete cache on entity config update
    # TODO: separate query so the project_name is not required?
    @cached(key_builder=key_builder)
    async def get_entity_type_name_by_id(self, project_name: str, entity_type_id: id) -> int:
        entity_types_config = await self.get_entity_types_config(project_name)

        for entity_type_name in entity_types_config:
            if entity_types_config[entity_type_name]['id'] == entity_type_id:
                return entity_type_name

        # TODO log message
        raise HTTPException(
            status_code=404,
            detail=f'Entity type with id "{entity_type_id}" of project "{project_name}" not found',
        )

    # TODO: delete cache on relation config update
    @cached(key_builder=key_builder)
    async def get_relation_types_config(self, project_name: str) -> Dict:
        records = await self.fetch(
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
            }
        )

        result = {}
        for record in records:
            result[record['system_name']] = {
                'id': record['id'],
                'display_name': record['display_name'],
                'config': json_load(record['config']),
                'domain_names': record['domain_names'],
                'range_names': record['range_names'],
            }

        return result

    # TODO: delete cache on relation config update
    @cached(key_builder=key_builder)
    async def get_relation_type_property_mapping(self, project_name: str, relation_type_name: str) -> Dict:
        relation_types_config = await self.get_relation_types_config(project_name)

        try:
            relation_type_config = relation_types_config[relation_type_name]
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Relation type "{relation_type_name}" of project "{project_name}" not found',
            )

        properties_config = relation_type_config['config']['data']

        # leave the id property intact
        result = {'id': 'id'}
        for property_config_id, property_config in properties_config.items():
            result[f'p_{dtu(relation_type_config["id"])}_{property_config_id}'] = property_config['system_name']

        return result

    # TODO: delete cache on relation config update
    @cached(key_builder=key_builder)
    async def get_relation_type_id_by_name(self, project_name: str, relation_type_name: str) -> int:
        relation_types_config = await self.get_relation_types_config(project_name)

        try:
            return relation_types_config[relation_type_name]['id']
        except KeyError:
            # TODO log message
            raise HTTPException(
                status_code=404,
                detail=f'Relation type "{relation_type_name}" of project "{project_name}" not found',
            )
