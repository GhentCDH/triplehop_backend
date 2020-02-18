from typing import Dict

from ariadne import gql, make_executable_schema, ObjectType, QueryType
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_type_def


def entity_resolver_wrapper(request: Request, project_name: str, entity_type_name: str):
    async def resolver(*_, id):
        data_repo = await get_repository_from_request(request, DataRepository)
        result = await data_repo.get_entity(project_name, entity_type_name, id)
        # TODO: find a way to close connections automatically
        await data_repo.close()

        return result

    return resolver


def relation_resolver_wrapper(
    request: Request,
    project_name: str,
    relation_type_name: str,
    inverse: bool = False,
):
    async def resolver(parent, info):
        id = parent['id']
        entity_type_name = info.parent_type.name.lower()

        data_repo = await get_repository_from_request(request, DataRepository)
        db_results = await data_repo.get_relations_with_entity(
            project_name,
            entity_type_name,
            id,
            relation_type_name,
            inverse,
        )
        # TODO: find a way to close connections automatically
        await data_repo.close()

        results = []
        for db_result in db_results:
            result = db_result['relation']
            result['entity'] = db_result['entity']
            result['entity']['__typename'] = db_result['entity_type_name'].capitalize()

            results.append(result)

        return results

    return resolver


async def create_type_defs(entity_types_config: Dict, relation_types_config: Dict):
    # Main query
    type_defs_dict = {
        'query': [[f'{etn.capitalize()}(id: Int!)', etn.capitalize()] for etn in entity_types_config.keys()],
        'geometry': [
            ['type', 'String'],
            ['coordinates', '[Float!]!'],
        ]
    }

    # TODO: add plurals
    # Entities
    for etn in entity_types_config:
        props = [['id', 'Int']]
        for prop in entity_types_config[etn]['config'].values():
            props.append([prop["system_name"], prop["type"]])
        type_defs_dict[etn] = props

    # Relations
    # TODO: cardinality
    # TODO: bidirectional relations
    unions_array = []
    for rtn in relation_types_config:
        domain_names = relation_types_config[rtn]['domain_names']
        range_names = relation_types_config[rtn]['range_names']
        unions_array.append(f'union R_{rtn}_domain = {" | ".join([dn.capitalize() for dn in domain_names])}')
        unions_array.append(f'union R_{rtn}_range = {" | ".join([rn.capitalize() for rn in range_names])}')

        props = [['id', 'Int']]
        for prop in relation_types_config[rtn]['config'].values():
            props.append([prop["system_name"], prop["type"]])

        type_defs_dict[f'r_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
        type_defs_dict[f'ri_{rtn}'] = props + [['entity', f'R_{rtn}_domain']]

        for domain_name in domain_names:
            type_defs_dict[domain_name].append([f'r_{rtn}_s', f'[R_{rtn}!]!'])
        for range_name in range_names:
            type_defs_dict[range_name].append([f'ri_{rtn}_s', f'[Ri_{rtn}!]!'])

    type_defs_array = [construct_type_def(type.capitalize(), props) for type, props in type_defs_dict.items()]

    return gql('\n'.join(unions_array) + '\n\n' + '\n\n'.join(type_defs_array))


async def create_object_types(
    request: Request,
    project_name: str,
    entity_types_config: Dict,
    relation_types_config: Dict
):
    object_types = {'Query': QueryType()}

    for entity_type_name in entity_types_config:
        object_types['Query'].set_field(
            entity_type_name.capitalize(),
            entity_resolver_wrapper(request, project_name, entity_type_name),
        )

    for relation_type_name in relation_types_config:
        for domain_name in [dn.capitalize() for dn in relation_types_config[relation_type_name]['domain_names']]:
            if domain_name not in object_types:
                object_types[domain_name] = ObjectType(domain_name)

            object_types[domain_name].set_field(
                f'r_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name)
            )
        for range_name in [dn.capitalize() for dn in relation_types_config[relation_type_name]['range_names']]:
            if range_name not in object_types:
                object_types[range_name] = ObjectType(range_name)

            object_types[range_name].set_field(
                f'ri_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name, True)
            )

    return object_types.values()


# TODO: cache per project_name (app always hangs after 6 requests when using cache)
async def create_schema(request: Request):
    config_repo = await get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_types_config(request.path_params['project_name'])
    relation_types_config = await config_repo.get_relation_types_config(request.path_params['project_name'])
    await config_repo.close()

    type_defs = await create_type_defs(entity_types_config, relation_types_config)
    object_types = await create_object_types(
        request,
        request.path_params['project_name'],
        entity_types_config,
        relation_types_config,
    )

    return make_executable_schema(type_defs, *object_types)
