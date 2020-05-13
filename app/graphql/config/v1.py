from typing import Dict, List

from ariadne import gql, make_executable_schema, QueryType
from copy import deepcopy
from re import compile as re_compile
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.graphql.base import construct_type_def

TITLE_CONVERSION_REGEX = re_compile(r'(?<![$])[$][0-9]+')


def _layout_field_converter(layout: List, data_conf: Dict):
    result = deepcopy(layout)
    for panel in result:
        for field in panel['fields']:
            field['field'] = data_conf[field['field']]['system_name']
            if 'base_layer' in field:
                field['base_layer'] = data_conf[field['base_layer']]['system_name']
    return result


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
            data_conf = entity_config['config']['data']
            config_item = {
                'system_name': entity_system_name,
                'display_name': entity_config['display_name'],
                'data': list(data_conf.values()),
                # TODO: add display_names from data to display, so data doesn't need to be exported
                'display': {
                    'title': TITLE_CONVERSION_REGEX.sub(
                        lambda m: '$' + data_conf[m.group()[1:]]['system_name'] if m.group()[1:] in data_conf else m[0],
                        entity_config['config']['display']['title']
                    ),
                    'layout': _layout_field_converter(entity_config['config']['display']['layout'], data_conf),
                },
                # TODO: search_filters, search_columns (search_data doesn't need to be exported)
            }
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
            data_conf = relation_config['config']['data']
            config_item = {
                'system_name': relation_system_name,
                'display_name': relation_config['display_name'],
                'data': list(data_conf.values()),
                'display': {
                    'domain_title': relation_config['config']['display']['domain_title'],
                    'range_title': relation_config['config']['display']['range_title'],
                    'layout': _layout_field_converter(relation_config['config']['display']['layout'], data_conf),
                },
                'domain_names': relation_config['domain_names'],
                'range_names': relation_config['range_names'],
            }
            results.append(config_item)

        return results

    return resolver


async def create_type_defs():
    # Main query
    # TODO provide possibility to hide some fields from config, based on permissions
    type_defs_dict = {
        'query': [
            ['Entity_config_s', '[Entity_config]'],
            ['Relation_config_s', '[Relation_config]'],
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
            # TODO: add overlays
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
            ['domain_title', 'String!'],
            ['range_title', 'String!'],
            ['layout', '[Display_panel_config!]'],
        ],
    }

    type_defs_array = [construct_type_def(type.capitalize(), props) for type, props in type_defs_dict.items()]

    return gql('\n\n'.join(type_defs_array))


async def create_object_types(
    request: Request,
    project_name: str,
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

    return object_types.values()


# TODO: cache per project_name (app always hangs after 6 requests when using cache)
async def create_schema(request: Request):
    config_repo = await get_repository_from_request(request, ConfigRepository)
    await config_repo.close()

    type_defs = await create_type_defs()
    object_types = await create_object_types(
        request,
        request.path_params['project_name'],
    )

    return make_executable_schema(type_defs, *object_types)