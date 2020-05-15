from fastapi import APIRouter, BackgroundTasks, Depends
from starlette.requests import Request

from app.auth.core import get_current_active_user_with_permissions as get_user
from app.auth.permission import require_entity_permission
from app.db.core import get_repository_from_request
from app.db.job import JobRepository
from app.es.job import reindex as reindex_job
from app.models.auth import UserWithPermissions
from app.models.es import ElasticSearchRequest
from app.models.job import JobId

router = APIRouter()


@router.post('/reindex', response_model=JobId)
async def reindex(
    es_request: ElasticSearchRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    user: UserWithPermissions = Depends(get_user),
):
    project_name = es_request.project_name
    entity_type_name = es_request.entity_type_name
    require_entity_permission(
        user,
        project_name,
        entity_type_name,
        'es_index',
    )
    job_repository = await get_repository_from_request(request, JobRepository)
    job_id = await job_repository.create(user, 'es_index', project_name, entity_type_name)
    background_tasks.add_task(reindex_job, job_id, project_name, entity_type_name, request)
    return JobId(id=job_id)
