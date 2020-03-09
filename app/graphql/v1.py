from typing import Dict

from ariadne import gql, make_executable_schema, ObjectType, QueryType
from copy import deepcopy
from re import compile as re_compile
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_type_def

TITLE_CONVERSION_REGEX = re_compile(r'(?<![$])[$][0-9]+')


# TODO: cache
def entity_configs_resolver_wrapper(request: Request, project_name: str):
    async def resolver(*_):
        config_repo = await get_repository_from_request(request, ConfigRepository)
        # TODO: find a way to avoid unnecessary connection openings
        db_result = await config_repo.get_entity_types_config(project_name)
        # TODO: find a way to close connections automatically
        await config_repo.close()

        results = []
        for entity_system_name, entity_config in db_result.items():
            data = entity_config['config']['data']
            config_item = {
                'system_name': entity_system_name,
                'display_name': entity_config['display_name'],
                'data': list(data.values()),
            }
            config_item['display'] = {
                'title': TITLE_CONVERSION_REGEX.sub(
                    lambda m: '$' + data[m.group()[1:]]['system_name'] if m.group()[1:] in data else m[0],
                    entity_config['config']['display']['title']
                )
            }

            if 'layout' in entity_config['config']['display']:
                config_item['display']['layout'] = deepcopy(entity_config['config']['display']['layout'])
                for p in config_item['display']['layout']:
                    for f in p['fields']:
                        f['field'] = data[f['field']]['system_name']
                        if 'base_layer' in f:
                            f['base_layer'] = data[f['base_layer']]['system_name']

            results.append(config_item)

        return results

    return resolver


# TODO: cache
def relation_configs_resolver_wrapper(request: Request, project_name: str):
    async def resolver(*_):
        config_repo = await get_repository_from_request(request, ConfigRepository)
        # TODO: find a way to avoid unnecessary connection openings
        db_result = await config_repo.get_relation_types_config(project_name)
        # TODO: find a way to close connections automatically
        await config_repo.close()

        results = []
        for relation_system_name, relation_config in db_result.items():
            data = relation_config['config']['data']
            config_item = {
                'system_name': relation_system_name,
                'display_name': relation_config['display_name'],
                'data': list(data.values()),
                'display': {},
                'domain_names': relation_config['domain_names'],
                'range_names': relation_config['range_names'],
            }

            config_item['display']['layout'] = deepcopy(relation_config['config']['display']['layout'])
            for p in config_item['display']['layout']:
                for f in p['fields']:
                    f['field'] = data[f['field']]['system_name']
                    if 'base_layer' in f:
                        f['base_layer'] = data[f['base_layer']]['system_name']

            results.append(config_item)

        return results

    return resolver


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
    # TODO provide possibility to hide some fields from config, based on permissions
    type_defs_dict = {
        'query': [[f'{etn.capitalize()}(id: Int!)', etn.capitalize()] for etn in entity_types_config.keys()],
        'geometry': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
        'entity_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['data', '[Data_config!]'],
            ['display', 'Entity_display_config!'],
        ],
        'data_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['type', 'String!'],
        ],
        'entity_display_config': [
            ['title', 'String!'],
            ['layout', '[Display_panel_config!]'],
        ],
        'display_panel_config': [
            ['label', 'String'],
            ['fields', '[Display_panel_field_config!]!'],
        ],
        'display_panel_field_config': [
            ['label', 'String'],
            ['field', 'String!'],
            ['type', 'String'],
            # TODO: allow multiple base layers
            # TODO: add top layers
            ['base_layer', 'String'],
        ],
        'relation_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['data', '[Data_config!]'],
            ['display', 'Relation_display_config!'],
            ['domain_names', '[String!]!'],
            ['range_names', '[String!]!'],
        ],
        # Don't allow title override (multiple ranges / domains possible => impossible to define)
        'relation_display_config': [
            ['layout', '[Display_panel_config!]'],
        ],
    }
    type_defs_dict['query'].append(['Entity_config_s', '[Entity_config]'])
    type_defs_dict['query'].append(['Relation_config_s', '[Relation_config]'])

    # TODO: add plurals
    # Entities
    for etn in entity_types_config:
        props = [['id', 'Int']]
        for prop in entity_types_config[etn]['config']['data'].values():
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
        for prop in relation_types_config[rtn]['config']['data'].values():
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

    object_types['Query'].set_field(
        'Entity_config_s',
        entity_configs_resolver_wrapper(request, project_name),
    )

    object_types['Query'].set_field(
        'Relation_config_s',
        relation_configs_resolver_wrapper(request, project_name),
    )

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
