from ariadne.asgi import GraphQL
from ariadne.constants import PLAYGROUND_HTML
from fastapi import APIRouter, Depends
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse


from app.auth.core import get_current_active_user_with_permissions
from app.graphql.data.v1 import create_schema

from app.models.auth import UserWithPermissions

router = APIRouter()


# see ariadne.asgi
@router.get('/{project_name}')
async def get_graphql_playground() -> HTMLResponse:
    return HTMLResponse(PLAYGROUND_HTML)


# see ariadne.asgi
@router.post('/{project_name}')
async def handle_graphql_request(
    request: Request,
    user: UserWithPermissions = Depends(get_current_active_user_with_permissions)
) -> JSONResponse:
    context = {
        'user': user,
    }
    graphql = GraphQL(await create_schema(request, user), context_value=context)
    return await graphql.graphql_http_server(request)
