from graphene import Field, Int, ObjectType
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.entity import EntityRepository
from app.graphql.base import TYPES


def resolver_wrapper(entity_class, project_name, entity_type_name):
    async def resolver(self, info, id):
        entity_repo = await get_repository_from_request(info.context["request"], EntityRepository)
        result = await entity_repo.get_entity('cinecos', 'film', id)
        # return Film(**result)
        return entity_class(**result)

    resolver.__name__ = f'resolve_{entity_type_name}'
    return resolver


async def create_query(request: Request):
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
        fields[f'resolve_{entity_type_name}'] = resolver_wrapper(
            entity_class,
            request.path_params['project_name'],
            entity_type_name,
        )

    return type(
        'Query',
        (ObjectType,),
        fields,
    )
