from fastapi import APIRouter, Depends
from graphene import Field, Int, ObjectType, Schema, String
from graphql.execution.executors.asyncio import AsyncioExecutor
from starlette.graphql import GraphQLApp
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.entity import EntityRepository

router = APIRouter()


class Project(ObjectType):
    id = Int()
    system_name = String(required=True)
    display_name = String()
    # entity_types = List(EntityType)

    def resolve_id(parent, info):
        return parent.id

    def resolve_system_name(parent, info):
        return parent.system_name

    def resolve_display_name(parent, info):
        return parent.display_name


class Query(ObjectType):
    project = Field(Project, system_name=String(required=True))

    async def resolve_project(self, info, system_name):
        config_repo = await get_repository_from_request(info.context["request"], ConfigRepository)
        project = await config_repo.get_project_by_system_name(system_name)
        print(project)
        return Project(1913, system_name, 'Test')


router.add_route(
    path='/',
    endpoint=GraphQLApp(schema=Schema(query=Query), executor_class=AsyncioExecutor),
)
