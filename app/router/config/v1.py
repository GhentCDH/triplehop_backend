from ariadne.asgi import GraphQL
from ariadne.constants import PLAYGROUND_HTML
from fastapi import APIRouter, Depends
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse


from app.mgmt.auth import get_current_active_user_with_permissions
from app.graphql.config.v1 import GraphQLConfigBuilder

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
    graphql_builder = GraphQLConfigBuilder(request, user)
    graphql = GraphQL(await graphql_builder.create_schema())
    return await graphql.graphql_http_server(request)