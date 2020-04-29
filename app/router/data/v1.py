from ariadne.asgi import GraphQL
from fastapi import APIRouter
from starlette.requests import Request

from app.graphql.data.v1 import create_schema

router = APIRouter()


async def endpoint(request: Request):
    return GraphQL(await create_schema(request))

router.add_route(
    path='/{project_name}',
    endpoint=endpoint,
    methods=['GET', 'POST'],
)
