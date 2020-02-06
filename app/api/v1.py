from fastapi import APIRouter
from graphene import Schema
from graphql.execution.executors.asyncio import AsyncioExecutor
from starlette.graphql import GraphQLApp
from starlette.requests import Request

from app.graphql.entity import create_query

router = APIRouter()


async def v1(request: Request):
    return GraphQLApp(schema=Schema(query=await create_query(request)), executor_class=AsyncioExecutor)

router.add_route(
    path='/{project_name}',
    endpoint=v1,
    methods=['GET', 'POST'],
)
