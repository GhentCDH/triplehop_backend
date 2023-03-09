import json
import typing

import aiocache
import asyncpg
import fastapi
import starlette

from app.cache.core import no_arg_key_builder, skip_first_arg_key_builder
from app.db.config import ConfigRepository
from app.db.core import get_repository_from_request
from app.models.auth import UserWithPermissions
from app.utils import dtu


class ConfigManager:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions = None,
    ):
        self._user = user
        self._config_repo = get_repository_from_request(request, ConfigRepository)

    # TODO: delete cache on project config update
    @aiocache.cached(key_builder=no_arg_key_builder)
    async def get_projects_config(self) -> typing.Dict:
        records = await self._config_repo.get_projects_config()

        result = {}
        for record in records:
            result[record["system_name"]] = {
                "id": record["id"],
                "system_name": record["system_name"],
                "display_name": record["display_name"],
            }

        return result

    # TODO: delete cache on project config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_project_id_by_name(self, project_name: str) -> int:
        project_config = await self.get_projects_config()

        try:
            return project_config[project_name]["id"]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

    # TODO: delete cache on project config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_project_config(self, project_name: str) -> int:
        project_config = await self.get_projects_config()

        try:
            return project_config[project_name]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Project "{project_name}" not found',
            )

    # TODO: delete cache on entity config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_entity_types_config(
        self,
        project_name: str,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        records = await self._config_repo.get_entity_types_config(
            project_name, connection=connection
        )

        result = {}
        for record in records:
            config = json.loads(record["config"])
            # Add title to display on edit pages when creating relations
            # For now, [id] display.title is being used
            # If required, a more specific configuration option can be added later on
            if "es_data" not in config:
                config["es_data"] = {}
            if not "fields" in config["es_data"]:
                config["es_data"]["fields"] = []
            config["es_data"]["fields"].append(
                {
                    "system_name": "edit_relation_title",
                    "base": "",
                    "parts": {
                        "entity_type_name": record["system_name"],
                        "id": "$id",
                        "selector_value": " $||$ ".join(
                            [
                                f"[$id] {title_part}"
                                for title_part in config["display"]["title"].split(
                                    " $||$ "
                                )
                            ]
                        ),
                    },
                    "type": "nested",
                    "display_not_available": True,
                }
            )
            result[record["system_name"]] = {
                "id": record["id"],
                "display_name": record["display_name"],
                "config": config,
            }

        return result

    # TODO: delete cache on entity config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_entity_type_property_mapping(
        self, project_name: str, entity_type_name: str
    ) -> typing.Dict:
        entity_types_config = await self.get_entity_types_config(project_name)

        if entity_type_name == "__all__":
            result = {}
            for etn in entity_types_config:
                result.update(
                    await self.get_entity_type_property_mapping(project_name, etn)
                )
            return result

        try:
            entity_type_config = entity_types_config[entity_type_name]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found',
            )

        if (
            "data" in entity_type_config["config"]
            and "fields" in entity_type_config["config"]["data"]
        ):
            properties_config = entity_type_config["config"]["data"]["fields"]
        else:
            properties_config = {}

        # leave the id property intact
        result = {"id": "id"}
        for property_config_id, property_config in properties_config.items():
            result[f"p_{dtu(property_config_id)}"] = property_config["system_name"]

        return result

    async def get_entity_type_i_property_mapping(
        self, project_name: str, entity_type_name: str
    ) -> typing.Dict:
        return {
            v: k
            for k, v in (
                await self.get_entity_type_property_mapping(
                    project_name, entity_type_name
                )
            ).items()
        }

    # TODO: delete cache on entity config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_entity_type_id_by_name(
        self,
        project_name: str,
        entity_type_name: str,
        connection: asyncpg.Connection = None,
    ) -> str:
        entity_types_config = await self.get_entity_types_config(
            project_name, connection=connection
        )

        try:
            return entity_types_config[entity_type_name]["id"]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Entity type "{entity_type_name}" of project "{project_name}" not found',
            )

    # TODO: delete cache on entity config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_current_entity_type_revision_id_by_name(
        self, project_name: str, entity_type_name: str
    ) -> str:
        entity_type_id = await self.get_entity_type_id_by_name(
            project_name, entity_type_name
        )

        return await self._config_repo.get_current_entity_type_revision_id(
            entity_type_id
        )

    # TODO: delete cache on entity config update
    # TODO: separate query so the project_name is not required?
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_entity_type_name_by_id(
        self, project_name: str, entity_type_id: str
    ) -> str:
        entity_types_config = await self.get_entity_types_config(project_name)

        for entity_type_name in entity_types_config:
            if entity_types_config[entity_type_name]["id"] == entity_type_id:
                return entity_type_name

        # TODO log message
        raise fastapi.exceptions.HTTPException(
            status_code=404,
            detail=f'Entity type with id "{entity_type_id}" of project "{project_name}" not found',
        )

    # TODO: delete cache on relation config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_relation_types_config(
        self,
        project_name: str,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        records = await self._config_repo.get_relation_types_config(
            project_name, connection=connection
        )

        result = {}
        for record in records:
            result[record["system_name"]] = {
                "id": record["id"],
                "display_name": record["display_name"],
                "config": json.loads(record["config"]),
                "domain_names": list(set(record["domain_names"])),
                "range_names": list(set(record["range_names"])),
            }

        return result

    # TODO: delete cache on relation config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_relation_type_property_mapping(
        self, project_name: str, relation_type_name: str
    ) -> typing.Dict:
        # Special case: '_source_'
        if relation_type_name == "_source_":
            return {
                "id": "id",
                "properties": "properties",
                "source_props": "source_props",
            }

        relation_types_config = await self.get_relation_types_config(project_name)

        if relation_type_name == "__all__":
            result = {
                # Used to indicate a source is relevant an entire relation
                "p___rel__": "__rel__",
            }
            for rtn in relation_types_config:
                result.update(
                    await self.get_relation_type_property_mapping(project_name, rtn)
                )
            return result

        try:
            relation_type_config = relation_types_config[relation_type_name]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Relation type "{relation_type_name}" of project "{project_name}" not found',
            )

        if (
            "data" in relation_type_config["config"]
            and "fields" in relation_type_config["config"]["data"]
        ):
            properties_config = relation_type_config["config"]["data"]["fields"]
        else:
            properties_config = {}

        # leave the id property intact
        result = {"id": "id"}
        for property_config_id, property_config in properties_config.items():
            result[f"p_{dtu(property_config_id)}"] = property_config["system_name"]

        return result

    async def get_relation_type_i_property_mapping(
        self, project_name: str, relation_type_name: str
    ) -> typing.Dict:
        return {
            v: k
            for k, v in (
                await self.get_relation_type_property_mapping(
                    project_name, relation_type_name
                )
            ).items()
        }

    # TODO: delete cache on relation config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_relation_type_id_by_name(
        self,
        project_name: str,
        relation_type_name: str,
        connection: asyncpg.Connection = None,
    ) -> int:
        # Special case '_source__'
        if relation_type_name == "_source_":
            return "_source_"

        relation_types_config = await self.get_relation_types_config(
            project_name, connection=connection
        )

        try:
            return relation_types_config[relation_type_name]["id"]
        except KeyError:
            # TODO log message
            raise fastapi.exceptions.HTTPException(
                status_code=404,
                detail=f'Relation type "{relation_type_name}" of project "{project_name}" not found',
            )

    # TODO: delete cache on relation config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_relation_type_name_by_id(
        self,
        project_name: str,
        relation_type_id: str,
        connection: asyncpg.Connection = None,
    ) -> int:
        # Special case '_source__'
        if relation_type_id == "_source_":
            return "_source_"

        relation_types_config = await self.get_relation_types_config(
            project_name, connection=connection
        )

        for relation_type_name in relation_types_config:
            if relation_types_config[relation_type_name]["id"] == relation_type_id:
                return relation_type_name

        # TODO log message
        raise fastapi.exceptions.HTTPException(
            status_code=404,
            detail=f'Relation type with id "{relation_type_id}" of project "{project_name}" not found',
        )

    # TODO: delete cache on relation config update
    @aiocache.cached(key_builder=skip_first_arg_key_builder)
    async def get_current_relation_type_revision_id_by_name(
        self, project_name: str, relation_type_name: str
    ) -> str:
        relation_type_id = await self.get_relation_type_id_by_name(
            project_name, relation_type_name
        )

        return await self._config_repo.get_current_relation_type_revision_id(
            relation_type_id
        )
