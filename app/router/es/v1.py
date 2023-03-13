from fastapi import APIRouter, BackgroundTasks, Depends
from starlette.requests import Request

from app.auth.permission import require_entity_type_permission
from app.mgmt.auth import get_current_active_user_with_permissions
from app.mgmt.es import ElasticsearchManager
from app.mgmt.job import JobManager
from app.models.auth import UserWithPermissions
from app.models.es import (
    ElasticAggregationSuggestBody,
    ElasticSearchBody,
    ElasticSuggestBody,
)
from app.models.job import JobId

router = APIRouter()


@router.post("/{project_name}/{entity_type_name}/search")
async def search(
    project_name: str,
    entity_type_name: str,
    body: ElasticSearchBody,
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions),
):
    elasticsearch_manager = ElasticsearchManager(
        project_name, entity_type_name, request, user
    )
    return await elasticsearch_manager.search(body.dict())


@router.post("/{project_name}/{entity_type_name}/suggest")
async def suggest(
    project_name: str,
    entity_type_name: str,
    body: ElasticSuggestBody,
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions),
):
    elasticsearch_manager = ElasticsearchManager(
        project_name, entity_type_name, request, user
    )
    return await elasticsearch_manager.suggest(body.dict())


@router.post("/{project_name}/{entity_type_name}/aggregation_suggest")
async def aggregation_suggest(
    project_name: str,
    entity_type_name: str,
    body: ElasticAggregationSuggestBody,
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions),
):
    elasticsearch_manager = ElasticsearchManager(
        project_name, entity_type_name, request, user
    )
    return await elasticsearch_manager.aggregation_suggest(body.dict())


@router.get("/{project_name}/{entity_type_name}/reindex", response_model=JobId)
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
        "es_data",
        "index",
    )
    job_manager = JobManager(request, user)
    job_id = await job_manager.create("es_index", project_name, entity_type_name)
    background_tasks.add_task(
        job_manager.es_index, job_id, project_name, entity_type_name
    )
    return JobId(id=job_id)
