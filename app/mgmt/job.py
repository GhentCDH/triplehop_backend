import uuid

import starlette

import app.mgmt.data
from app.db.core import get_repository_from_request
from app.db.job import JobRepository
from app.es.base import BaseElasticsearch
from app.es.core import get_es_from_request
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.models.job import JobToDisplay
from app.utils import BATCH_SIZE


class JobManager:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions,
    ):
        self._request = request
        self._project_name = request.path_params["project_name"]
        self._config_manager = ConfigManager(request, user)
        self._job_repo = get_repository_from_request(request, JobRepository)
        self._es = get_es_from_request(request, BaseElasticsearch)
        self._user = user

    async def get_by_project(self, id: uuid.UUID, project_name: str) -> JobToDisplay:
        record = await self._job_repo.get_by_project(str(id), project_name)
        if record:
            return JobToDisplay(**record)
        return None

    async def create(
        self, type: str, project_name: str = None, entity_type_name: str = None
    ) -> uuid.UUID:
        project_id = None
        entity_type_id = None
        if project_name is not None:
            project_id = await self._config_manager.get_project_id_by_name(project_name)
        if entity_type_name is not None:
            entity_type_id = await self._config_manager.get_entity_type_id_by_name(
                project_name, entity_type_name
            )

        return await self._job_repo.create(
            type, self._user.id, project_id, entity_type_id
        )

    # TODO: set edit prohibited mode, allowing end user warnings when indexing
    async def es_index(
        self, job_id: uuid.UUID, project_name: str, entity_type_name: str
    ):
        data_manager = app.mgmt.data.DataManager(self._request, self._user)
        entity_ids = await data_manager.get_entity_ids_by_type_name(entity_type_name)

        await self._job_repo.start(job_id, len(entity_ids))

        try:
            entity_types_config = await self._config_manager.get_entity_types_config(
                project_name
            )

            entity_type_config = entity_types_config[entity_type_name]
            es_data_config = entity_type_config["config"]["es_data"]["fields"]
            triplehop_query = BaseElasticsearch.extract_query_from_es_data_config(
                es_data_config
            )
            new_index_name = await self._es.create_new_index(es_data_config)

            batch_counter = 0
            while True:
                batch_ids = entity_ids[
                    batch_counter * BATCH_SIZE : (batch_counter + 1) * BATCH_SIZE
                ]
                batch_entities = await data_manager.get_entity_data(
                    batch_ids,
                    triplehop_query,
                    entity_type_name=entity_type_name,
                )

                batch_docs = BaseElasticsearch.convert_entities_to_docs(
                    entity_types_config, es_data_config, batch_entities
                )

                await self._es.add_bulk(new_index_name, batch_docs)

                if (batch_counter + 1) * BATCH_SIZE + 1 > len(entity_ids):
                    break

                await self._job_repo.update_counter(
                    job_id, (batch_counter + 1) * BATCH_SIZE
                )
                batch_counter += 1

            await self._es.switch_to_new_index(new_index_name, entity_type_config["id"])
            await self._job_repo.end_with_success(job_id)
        except Exception as e:
            await self._job_repo.end_with_error(job_id)
            # TODO: log error
            raise e
