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


class Film(ObjectType):
    id = Int(required=True)
    title = String()
    year = Int()

    # def resolve_id(parent, info):
    #     return parent.id
    #
    # def resolve_title(parent, info):
    #     return parent.title
    #
    # def resolve_year(parent, info):
    #     return parent.year


# class Query(ObjectType):
#     project = Field(Project, system_name=String(required=True))

# async def resolve_project(self, info, system_name):
#     config_repo = await get_repository_from_request(info.context["request"], ConfigRepository)
#     project = await config_repo.get_project_config(system_name)
#     return Project(**project)


# async def resolver(self, info, id):
#     print(id)
#     entity_repo = await get_repository_from_request(info.context["request"], EntityRepository)
#     result = await entity_repo.get_entity('cinecos', 'film', id)
#     return result


def make_entity_resolver(entity_class, project_name, entity_type_name):
    async def resolver(self, info, id):
        entity_repo = await get_repository_from_request(info.context["request"], EntityRepository)
        result = await entity_repo.get_entity('cinecos', 'film', id)
        # return Film(**result)
        return entity_class(**result)

    resolver.__name__ = f'resolve_{entity_type_name}'
    return resolver


async def v1(request: Request):
    config_repo = await get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_types_config(request.path_params['project_name'])

    fields = {}
    for entity_type_name in entity_types_config:
        entity_type_config = entity_types_config[entity_type_name]['config']
        entity_fields = {
            'id': Int(required=True)
        }
        for field_id in entity_type_config:
            entity_fields[entity_type_config[field_id]['system_name']] = TYPES[entity_type_config[field_id]['type']]()

        entity_class = type(
            entity_type_name,
            (ObjectType,),
            entity_fields,
        )
        fields[entity_type_name] = Field(
            entity_class,
            id=Int(required=True),
        )
        fields[f'resolve_{entity_type_name}'] = make_entity_resolver(
            entity_class,
            request.path_params['project_name'],
            entity_type_name,
        )

    Query = type(
        'Query',
        (ObjectType,),
        fields,
    )

    return GraphQLApp(schema=Schema(query=Query), executor_class=AsyncioExecutor)


router.add_route(
    path='/{project_name}',
    endpoint=v1,
    methods=['GET', 'POST'],
)
