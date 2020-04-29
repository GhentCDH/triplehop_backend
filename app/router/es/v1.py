from fastapi import APIRouter, BackgroundTasks, Depends
from starlette.requests import Request

from app.auth.core import get_current_active_user_with_permissions as get_user
from app.auth.models import UserWithPermissions
from app.models.es import ElasticSearchRequest
from app.models.job import Job

router = APIRouter()


def reindex_task(project_name: str, entity_name: str):
    # TODO: create job, start job
    print('reindex')


@router.post('/reindex')
async def reindex(
    request: ElasticSearchRequest,
    background_tasks: BackgroundTasks,
    user: UserWithPermissions = Depends(get_user),
):
    # TODO: auth
    # TODO: create task
    # TODO: start background task: background_tasks.add_task(reindex_task, )
    # TODO: return response
