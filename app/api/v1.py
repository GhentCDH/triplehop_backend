from ariadne.asgi import GraphQL
from fastapi import APIRouter
from starlette.requests import Request

from app.graphql.v1 import create_schema as create_schema_v1

router = APIRouter()


async def v1(request: Request):
    return GraphQL(await create_schema_v1(request))

router.add_route(
    path='/{project_name}',
    endpoint=v1,
    methods=['GET', 'POST'],
)
