from typing import Dict

from ariadne import gql, QueryType, make_executable_schema
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.entity import EntityRepository
from app.graphql.base import construct_type_def


def entity_resolver_wrapper(request: Request, project_name: str, entity_type_name: str):
    async def resolver(*_, id):
        entity_repo = await get_repository_from_request(request, EntityRepository)
        return await entity_repo.get_entity(project_name, entity_type_name, id)

    resolver.__name__ = f'resolve_{entity_type_name}'
    return resolver


async def create_type_defs(entity_types_config: Dict):
    type_defs_dict = {}
    for entity_type_name in entity_types_config:
        props = [['id', 'Int']]
        for prop in entity_types_config[entity_type_name]['config'].values():
            props.append([prop["system_name"], prop["type"]])
        type_defs_dict[entity_type_name] = props

    type_defs_dict['query'] = [[f'{etn}(id: Int!)', etn.capitalize()] for etn in type_defs_dict.keys()]

    type_defs_array = [construct_type_def(type.capitalize(), props) for type, props in type_defs_dict.items()]

    return gql('\n\n'.join(type_defs_array))


async def create_query(
    entity_repo: EntityRepository,
    project_name: str,
    entity_types_config: Dict,
):
    object_types = [QueryType()]

    for entity_type_name in entity_types_config:
        object_types[0].set_field(
            entity_type_name,
            entity_resolver_wrapper(entity_repo, project_name, entity_type_name),
        )

    return object_types


async def create_schema(request: Request):
    config_repo = await get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_types_config(request.path_params['project_name'])

    return make_executable_schema(
        await create_type_defs(entity_types_config),
        await create_query(
            request,
            request.path_params['project_name'],
            entity_types_config,
        )
    )
