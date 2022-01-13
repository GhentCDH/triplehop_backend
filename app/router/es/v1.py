from fastapi import APIRouter, BackgroundTasks, Depends
from starlette.requests import Request

from app.auth.permission import require_entity_type_permission
from app.db.config import ConfigRepository
from app.db.core import get_repository_from_request
from app.es.base import BaseElasticsearch
from app.es.core import get_es_from_request
from app.mgmt.auth import get_current_active_user_with_permissions
from app.mgmt.job import JobManager
from app.models.auth import UserWithPermissions
from app.models.es import ElasticSearchBody
from app.models.job import JobId

router = APIRouter()


@router.post('/{project_name}/{entity_type_name}/search')
async def search(
    project_name: str,
    entity_type_name: str,
    es_body: ElasticSearchBody,
    request: Request,
):
    config_repo = get_repository_from_request(request, ConfigRepository)
    entity_type_id = await config_repo.get_entity_type_id_by_name(project_name, entity_type_name)
    es = get_es_from_request(request, BaseElasticsearch)
    return await es.search(entity_type_id, es_body.dict())


@router.get('/{project_name}/{entity_type_name}/reindex', response_model=JobId)
async def reindex(
    project_name: str,
    entity_type_name: str,
    background_tasks: BackgroundTasks,
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions),
):
    require_entity_type_permission(
        user,
        project_name,
        entity_type_name,
        'es_data',
        'index',
    )
    job_manager = JobManager(request, user)
    job_id = await job_manager.create('es_index', project_name, entity_type_name)
    background_tasks.add_task(job_manager.es_index, job_id, project_name, entity_type_name)
    return JobId(id=job_id)
