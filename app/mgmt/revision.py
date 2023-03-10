import json
import typing

import asyncpg
import starlette

from app.db.core import get_repository_from_request
from app.db.revision import RevisionRepository
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions


class RevisionManager:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions = None,
    ):
        self._project_name = request.path_params["project_name"]
        self._config_manager = ConfigManager(request, user)
        self._revision_repo = get_repository_from_request(request, RevisionRepository)
        self._user = user
        self._entity_types_config = None
        self._relation_types_config = None
        self._project_id = None

    async def _get_project_id(self):
        if self._project_id is None:
            self._project_id = await self._config_manager.get_project_id_by_name(
                self._project_name
            )
        return self._project_id

    """
    Create a new revision
    data = {
        'entities': {
            entity_type_name: {
                entity_id: [
                    old_value,
                    new_value,
                ]
            }
        },
        'relations': {
            relation_type_name: {
                relation_id: [
                    start_entity_id,
                    end_entity_id,
                    old_value,
                    new_value,
                ]
            }
        }
    }
    """

    async def post_revision(
        self,
        data: typing.Dict,
        connection: asyncpg.connection.Connection,
    ):
        project_id = await self._get_project_id()
        raw_entities = []
        raw_relations = []

        if "entities" in data:
            for entity_type_name in data["entities"]:
                entity_type_revision_id = await self._config_manager.get_current_entity_type_revision_id_by_name(
                    self._project_name,
                    entity_type_name,
                )
                entity_type_id = await self._config_manager.get_entity_type_id_by_name(
                    self._project_name,
                    entity_type_name,
                )
                for entity_id, [old_value, new_value] in data["entities"][
                    entity_type_name
                ].items():
                    raw_entities.append(
                        [
                            entity_type_revision_id,
                            entity_type_id,
                            entity_id,
                            old_value,
                            new_value,
                        ]
                    )

        if "relations" in data:
            for relation_type_name in data["relations"]:
                relation_type_revision_id = await self._config_manager.get_current_relation_type_revision_id_by_name(
                    self._project_name,
                    relation_type_name,
                )
                relation_type_id = (
                    await self._config_manager.get_relation_type_id_by_name(
                        self._project_name,
                        relation_type_name,
                    )
                )

                # TODO generate separate revision table for relation sources
                # TODO now
                if relation_type_name == "_source_":
                    pass
                else:
                    for relation_id, [
                        old_relation_props,
                        new_relation_props,
                        start_entity_type_name,
                        start_entity_id,
                        end_entity_type_name,
                        end_entity_id,
                    ] in data["relations"][relation_type_name].items():
                        raw_relations.append(
                            [
                                relation_type_revision_id,
                                relation_type_id,
                                relation_id,
                                await self._config_manager.get_current_entity_type_revision_id_by_name(
                                    self._project_name,
                                    start_entity_type_name,
                                ),
                                await self._config_manager.get_entity_type_id_by_name(
                                    self._project_name,
                                    start_entity_type_name,
                                ),
                                start_entity_id,
                                await self._config_manager.get_current_entity_type_revision_id_by_name(
                                    self._project_name,
                                    end_entity_type_name,
                                ),
                                await self._config_manager.get_entity_type_id_by_name(
                                    self._project_name,
                                    end_entity_type_name,
                                ),
                                end_entity_id,
                                old_relation_props,
                                new_relation_props,
                            ]
                        )

        # Connection is required
        # There should already be a transaction on the connection, a nested one is created here
        async with connection.transaction():
            revision_id = await self._revision_repo.get_new_revision_count(
                project_id,
                connection,
            )
            if raw_entities:
                await self._revision_repo.post_entities_revision(
                    project_id,
                    [
                        {
                            "revision_id": revision_id,
                            "user_id": str(self._user.id),
                            "entity_type_revision_id": raw_entity[0],
                            "entity_type_id": raw_entity[1],
                            "entity_id": raw_entity[2],
                            "old_value": json.dumps(raw_entity[3]),
                            "new_value": json.dumps(raw_entity[4]),
                        }
                        for raw_entity in raw_entities
                    ],
                    connection,
                )
            if raw_relations:
                await self._revision_repo.post_relations_revision(
                    project_id,
                    [
                        {
                            "revision_id": revision_id,
                            "user_id": str(self._user.id),
                            "relation_type_revision_id": raw_relation[0],
                            "relation_type_id": raw_relation[1],
                            "relation_id": raw_relation[2],
                            "start_entity_type_revision_id": raw_relation[3],
                            "start_entity_type_id": raw_relation[4],
                            "start_entity_id": raw_relation[5],
                            "end_entity_type_revision_id": raw_relation[6],
                            "end_entity_type_id": raw_relation[7],
                            "end_entity_id": raw_relation[8],
                            "old_value": json.dumps(raw_relation[9]),
                            "new_value": json.dumps(raw_relation[10]),
                        }
                        for raw_relation in raw_relations
                    ],
                    connection,
                )
