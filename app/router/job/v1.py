from app.auth.permission import (
    require_entity_type_permission,
    require_project_permission,
)
from app.mgmt.auth import get_current_active_user_with_permissions
from app.mgmt.job import JobManager
from app.models.auth import UserWithPermissions
from app.models.job import JobToDisplay
from fastapi import APIRouter, Depends
from pydantic import UUID4
from starlette.requests import Request

router = APIRouter()

JOB_TYPE_PERMISSIONS = {
    "es_index": ["es_data", "index"],
}


@router.get("/{project_name}/{id}", response_model=JobToDisplay)
async def get_by_project(
    id: UUID4,
    project_name: str,
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions),
):
    job_manager = JobManager(request, user)
    job_to_display = await job_manager.get_by_project(id, project_name)

    if job_to_display is None:
        return None

    if job_to_display.entity_type_system_name is None:
        require_project_permission(
            user,
            job_to_display.project_system_name,
            *JOB_TYPE_PERMISSIONS[job_to_display.type],
        )

    if job_to_display.entity_type_system_name is not None:
        require_entity_type_permission(
            user,
            job_to_display.project_system_name,
            job_to_display.entity_type_system_name,
            *JOB_TYPE_PERMISSIONS[job_to_display.type],
        )

    if job_to_display.relation_type_system_name is not None:
        # TODO check permission
        return None

    return job_to_display
