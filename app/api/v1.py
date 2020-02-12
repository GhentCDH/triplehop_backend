from ariadne.asgi import GraphQL
from fastapi import APIRouter
from starlette.requests import Request

from app.graphql.entity import create_schema

router = APIRouter()


async def v1(request: Request):
    return GraphQL(await create_schema(request), debug=True)

router.add_route(
    path='/{project_name}',
    endpoint=v1,
    methods=['GET', 'POST'],
)
