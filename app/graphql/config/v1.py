from typing import Dict, List

from ariadne import gql, make_executable_schema, QueryType
from copy import deepcopy
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.graphql.base import construct_type_def
from app.utils import RE_FIELD_CONVERSION


def _layout_field_converter(layout: List, data_conf: Dict) -> List:
    result = deepcopy(layout)
    for panel in result:
        for field in panel['fields']:
            field['field'] = data_conf[field['field']]['system_name']
            if 'base_layer' in field:
                field['base_layer'] = data_conf[field['base_layer']]['system_name']
    return result


def _es_columns_converter(columns: List, es_data_conf: Dict) -> List:
    result = []
    for column in columns:
        result.append({
            'system_name': es_data_conf[column['column']]['system_name'],
            'display_name': es_data_conf[column['column']]['display_name'],
            'type': es_data_conf[column['column']]['type'],
            'sortable': column['sortable'],
        })
    return result


def _es_filters_converter(filters: List, es_data_conf: Dict) -> List:
    result = []
    for section in filters:
        res_section = {
            'filters': [],
        }
        for filter in section['filters']:
            filter_conf = {
                'system_name': es_data_conf[filter['filter']]['system_name'],
                'display_name': es_data_conf[filter['filter']]['display_name'],
                'type': filter['type'] if 'type' in filter else es_data_conf[filter['filter']]['type']
            }
            if 'interval' in filter:
                filter_conf['interval'] = filter['interval']
            res_section['filters'].append(filter_conf)
        result.append(res_section)
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
                    # TODO: check if this conversion shouldn't happen in db code
                    'title': RE_FIELD_CONVERSION.sub(
                        lambda m: '$' + data_conf[m.group()[1:]]['system_name'] if m.group()[1:] in data_conf else m[0],
                        entity_config['config']['display']['title']
                    ),
                    'layout': _layout_field_converter(entity_config['config']['display']['layout'], data_conf),
                },
            }
            if 'es_data' in entity_config['config']:
                es_data_conf = entity_config['config']['es_data']
                config_item['es_columns'] = _es_columns_converter(entity_config['config']['es_columns'], es_data_conf)
                config_item['es_filters'] = _es_filters_converter(entity_config['config']['es_filters'], es_data_conf)
            # TODO: es_filters, es_columns (es_data doesn't need to be exported)
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
            ['es_columns', '[Es_column_config!]'],
            ['es_filters', '[Es_filter_group_config!]'],
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
        'es_column_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['type', 'String!'],
            ['sortable', 'Boolean!'],
        ],
        'es_filter_group_config': [
            ['filters', '[Es_filter_config!]'],
        ],
        'es_filter_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['type', 'String!'],
            ['interval', 'Int']
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
