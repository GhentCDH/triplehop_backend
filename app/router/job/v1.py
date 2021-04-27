from fastapi import APIRouter, Depends
from pydantic import UUID4
from starlette.requests import Request

from app.auth.core import get_current_active_user_with_permissions as get_user
from app.auth.permission import require_entity_permission, require_project_permission
from app.db.core import get_repository_from_request
from app.db.job import JobRepository
from app.models.auth import UserWithPermissions
from app.models.job import JobToDisplay

router = APIRouter()


@router.get('/{project_name}/{id}', response_model=JobToDisplay)
async def get_by_project(
    id: UUID4,
    project_name: str,
    request: Request,
    user: UserWithPermissions = Depends(get_user),
):
    job_repository = get_repository_from_request(request, JobRepository)
    job_to_display = await job_repository.get_by_project(id, project_name)

    if job_to_display is None:
        return None

    if job_to_display.entity_type_system_name is None:
        require_project_permission(
            user,
            job_to_display.project_system_name,
            job_to_display.type,
        )

    if job_to_display.entity_type_system_name is not None:
        require_entity_permission(
            user,
            job_to_display.project_system_name,
            job_to_display.entity_type_system_name,
            job_to_display.type,
        )

    if job_to_display.relation_type_system_name is not None:
        # TODO check permission
        return None

    return job_to_display
