import starlette
import uuid

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.db.job import JobRepository
from app.es.base import BaseElasticsearch
from app.es.core import get_es_from_request

BATCH_SIZE = 500


async def reindex(job_id: uuid.UUID, project_name: str, entity_type_name: str, request: starlette.requests.Request):
    data_repo = get_repository_from_request(request, DataRepository, project_name)
    entity_ids = await data_repo.get_entity_ids_by_type_name(entity_type_name)

    job_repo = get_repository_from_request(request, JobRepository)
    await job_repo.start(job_id, len(entity_ids))

    try:
        config_repo = get_repository_from_request(request, ConfigRepository)
        entity_types_config = await config_repo.get_entity_types_config(project_name)

        entity_type_config = entity_types_config[entity_type_name]
        es_data_config = entity_type_config['config']['es_data']
        crdb_query = BaseElasticsearch.extract_query_from_es_data_config(es_data_config)
        es = get_es_from_request(request, BaseElasticsearch)
        new_index_name = await es.create_new_index(entity_type_name, es_data_config)

        batch_counter = 0
        while True:
            batch_ids = entity_ids[batch_counter * BATCH_SIZE:(batch_counter + 1) * BATCH_SIZE]
            batch_entities = await data_repo.get_entity_data(
                entity_type_name,
                batch_ids,
                crdb_query,
            )

            batch_docs = BaseElasticsearch.convert_entities_to_docs(
                entity_types_config,
                es_data_config,
                batch_entities
            )

            await es.add_bulk(new_index_name, entity_type_name, batch_docs)

            if (batch_counter + 1) * BATCH_SIZE + 1 > len(entity_ids):
                break

            await job_repo.update_counter(job_id, (batch_counter + 1) * BATCH_SIZE)
            batch_counter += 1

        await es.switch_to_new_index(new_index_name, entity_type_config['id'])
        await job_repo.end_with_success(job_id)
    except Exception as e:
        await job_repo.end_with_error(job_id)
        # TODO: log error
        raise e
