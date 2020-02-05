from fastapi import APIRouter
from graphene import Field, Int, ObjectType, Schema, String
from graphql.execution.executors.asyncio import AsyncioExecutor
from starlette.graphql import GraphQLApp
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.entity import EntityRepository

TYPES = {
    'string': String,
    'int': Int,
}

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


# class Query(ObjectType):
#     project = Field(Project, system_name=String(required=True))

async def resolve_project(self, info, system_name):
    config_repo = await get_repository_from_request(info.context["request"], ConfigRepository)
    project = await config_repo.get_project_by_system_name(system_name)
    return Project(**project)


async def resolver(self, info, id):
    print(id)
    entity_repo = await get_repository_from_request(info.context["request"], EntityRepository)
    return await entity_repo.get_entity('cinecos', 'film', id)


def make_entity_resolver(project_name, entity_type_name):
    async def resolver(self, info, id):
        print(entity_id)
        entity_repo = await get_repository_from_request(info.context["request"], EntityRepository)
        return await entity_repo.get_entity(project_name, entity_type_name, id)

    resolver.__name__ = f'resolve_{entity_type_name}'
    return resolver


# TODO: fix graphql.error.located_error.GraphQLLocatedError: unhandled standard data type 'graphid' (OID 7002)

async def v1(request: Request):
    config_repo = await get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_type_config(request.path_params['project_name'])

    fields = {}
    for entity_type_name in entity_types_config:
        # entity_type_config = entity_types_config[entity_type_name]['config']
        # TODO: add types to database
        # fields = {
        #     'id': Int()
        # }
        # for field_id in entity_type_config:
        #     fields[entity_type_config[field_id]['system_name']] = TYPES[entity_type_config[field_id]['type']]()
        entity_fields = {
            'id': Int(required=True),
            'title': String(),
            'year': Int(),
        }

        # TODO: name and description?
        fields[entity_type_name] = Field(
            type(
                entity_type_name,
                (ObjectType,),
                fields,
            ),
            id=Int(required=True),
        )
        fields[f'resolve_{entity_type_name}'] = make_entity_resolver(
            request.path_params['project_name'],
            entity_type_name,
        )

    # TODO: make generic
    # Query = type(
    #     'Query',
    #     (ObjectType,),
    #     fields,
    # )

    Query = type(
        'Query',
        (ObjectType,),
        {
            'film': Field(
                type(
                    'film',
                    (ObjectType,),
                    {
                        'id': Int(required=True),
                        'title': String(),
                        'year': Int(),
                    },
                ),
                id=Int(required=True),
            ),
            'resolve_film': resolver,
        },
    )

    # TODO: add types to Schema constructor?
    return GraphQLApp(schema=Schema(query=Query), executor_class=AsyncioExecutor)


router.add_route(
    path='/{project_name}',
    endpoint=v1,
    methods=['GET', 'POST'],
)
