import ariadne
import copy
import typing
import starlette

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.graphql.base import construct_def
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.utils import RE_FIELD_CONVERSION


class GraphQLConfigBuilder:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions
    ) -> None:
        self._project_name = request.path_params['project_name']
        self._user = user
        self._config_manager = ConfigManager(
            get_repository_from_request(request, ConfigRepository),
            self._user,
        )




def _replace_field_ids_by_system_names(
    input: str,
    entity_field_lookup: typing.Dict,
    relation_lookup: typing.Dict,
    relation_field_lookup: typing.Dict,
) -> str:
    result = input
    for match in RE_FIELD_CONVERSION.finditer(input):
        if not match:
            continue

        path = [p.replace('$', '') for p in match.group(0).split('->')]
        for i, p in enumerate(path):
            # last element => p = relation.r_prop or e_prop
            if i == len(path) - 1:
                # relation property
                if '.' in p:
                    (rel_type_id, r_prop_id) = p.split('.')
                    result = result.replace(
                        rel_type_id.split('_')[1],
                        relation_lookup[rel_type_id.split('_')[1]]
                    )
                    result = result.replace(
                        r_prop_id,
                        relation_field_lookup[r_prop_id]
                    )
                    break
                # entity property
                result = result.replace(
                    p,
                    entity_field_lookup[p]
                )
                break
            # not last element => p = relation
            result = result.replace(
                p.split('_')[1],
                relation_lookup[p.split('_')[1]]
            )

    return result


# Relation layouts can only have data from their own data fields, this is passed as entity_field_lookup.
def _layout_field_converter(
    layout: typing.List,
    entity_field_lookup: typing.Dict,
    relation_lookup: typing.Dict = None,
    relation_field_lookup: typing.Dict = None,
) -> typing.List:
    result = copy.deepcopy(layout)
    for panel in result:
        for field in panel['fields']:
            field['field'] = _replace_field_ids_by_system_names(
                field['field'],
                entity_field_lookup,
                relation_lookup,
                relation_field_lookup,
            )
            if 'base_layer' in field:
                field['base_layer'] = _replace_field_ids_by_system_names(
                    field['base_layer'],
                    entity_field_lookup,
                    relation_lookup,
                    relation_field_lookup,
                )
    return result


def _es_columns_converter(columns: typing.List, es_data_conf: typing.Dict) -> typing.List:
    results = []
    for column in columns:
        result = {
            'system_name': es_data_conf[column['column'][1:]]['system_name'],
            'display_name': column.get('display_name', es_data_conf[column['column'][1:]]['display_name']),
            'type': es_data_conf[column['column'][1:]]['type'],
            'sortable': column['sortable'],
        }
        for key in ['main_link', 'link', 'sub_field', 'sub_field_type']:
            if key in column:
                result[key] = column[key]
        results.append(result)
    return results


def _es_filters_converter(filters: typing.List, es_data_conf: typing.Dict) -> typing.List:
    result = []
    for section in filters:
        res_section = {
            'filters': [],
        }
        for filter in section['filters']:
            filter_conf = {
                'system_name': es_data_conf[filter['filter'][1:]]['system_name'],
                'display_name': es_data_conf[filter['filter'][1:]]['display_name'],
                'type': filter['type'] if 'type' in filter else es_data_conf[filter['filter'][1:]]['type']
            }
            if 'interval' in filter:
                filter_conf['interval'] = filter['interval']
            res_section['filters'].append(filter_conf)
        result.append(res_section)
    return result


# TODO: cache
def project_config_resolver_wrapper(request: starlette.requests.Request, project_name: str):
    async def resolver(*_):
        config_repo = get_repository_from_request(request, ConfigRepository)
        # TODO: find a way to avoid unnecessary connection openings
        db_result = await config_repo.get_project_config(project_name)

        return db_result

    return resolver


# TODO: cache
def entity_configs_resolver_wrapper(request: starlette.requests.Request, project_name: str):
    async def resolver(*_):
        config_repo = get_repository_from_request(request, ConfigRepository)
        entity_types_config = await config_repo.get_entity_types_config(project_name)
        relation_types_config = await config_repo.get_relation_types_config(project_name)

        entity_field_lookup = {}
        for entity_config in entity_types_config.values():
            if 'data' in entity_config['config'] and 'fields' in entity_config['config']['data']:
                for id_, config in entity_config['config']['data']['fields'].items():
                    entity_field_lookup[id_] = config['system_name']

        relation_lookup = {}
        relation_field_lookup = {}
        for relation_system_name, relation_config in relation_types_config.items():
            relation_lookup[relation_config['id']] = relation_system_name
            if 'data' in relation_config['config'] and 'fields' in relation_config['config']['data']:
                for id_, config in relation_config['config']['data']['fields'].items():
                    relation_field_lookup[id_] = config['system_name']

        results = []
        for entity_system_name, entity_config in entity_types_config.items():
            data_conf = {}
            config_item = {
                'system_name': entity_system_name,
                'display_name': entity_config['display_name'],
            }
            # TODO: remove configs that cannot be used by users based on permissions
            if 'source' in entity_config['config']:
                config_item['source'] = entity_config['config']['source']
            if 'data' in entity_config['config'] and 'fields' in entity_config['config']['data']:
                data_conf = entity_config['config']['data']['fields']
                config_item['data'] = list(data_conf.values())
            # TODO: add display_names from data to display, so data doesn't need to be exported
            # TODO: figure out a way to add permissions for displaying layouts and fields
            if 'display' in entity_config['config']:
                config_item['display'] = {
                    'title': _replace_field_ids_by_system_names(
                        entity_config['config']['display']['title'],
                        entity_field_lookup,
                        relation_lookup,
                        relation_field_lookup,
                    ),
                    'layout': _layout_field_converter(
                        entity_config['config']['display']['layout'],
                        entity_field_lookup,
                        relation_lookup,
                        relation_field_lookup,
                    ),
                }
            if 'edit' in entity_config['config']:
                config_item['edit'] = {
                    'layout': _layout_field_converter(
                        entity_config['config']['edit']['layout'],
                        entity_field_lookup,
                        relation_lookup,
                        relation_field_lookup,
                    ),
                }
            if 'es_data' in entity_config['config']:
                es_data_conf = {esd['system_name']: esd for esd in entity_config['config']['es_data']}
                config_item['elasticsearch'] = {
                    'title': entity_config['config']['es_display']['title'],
                    'columns': _es_columns_converter(
                        entity_config['config']['es_display']['columns'],
                        es_data_conf,
                    ),
                    'filters': _es_filters_converter(
                        entity_config['config']['es_display']['filters'],
                        es_data_conf,
                    ),
                }
            results.append(config_item)

        return results

    return resolver


# TODO: cache
def relation_configs_resolver_wrapper(request: starlette.requests.Request, project_name: str):
    async def resolver(*_):
        config_repo = get_repository_from_request(request, ConfigRepository)
        relation_types_config = await config_repo.get_relation_types_config(project_name)

        relation_lookup = {}
        relation_field_lookup = {}
        for relation_system_name, relation_config in relation_types_config.items():
            relation_lookup[relation_config['id']] = relation_system_name
            if 'data' in relation_config['config'] and 'fields' in relation_config['config']['data']:
                for id_, config in relation_config['config']['data']['fields'].items():
                    relation_field_lookup[id_] = config['system_name']

        results = []
        for relation_system_name, relation_config in relation_types_config.items():
            config_item = {
                'system_name': relation_system_name,
                'display_name': relation_config['display_name'],
                'domain_names': relation_config['domain_names'],
                'range_names': relation_config['range_names'],
            }
            if 'data' in relation_config['config'] and 'fields' in relation_config['config']['data']:
                data_conf = relation_config['config']['data']['fields']
                config_item['data'] = list(data_conf.values())
            if 'display' in relation_config['config']:
                config_item['display'] = {}
                if 'domain_title' in relation_config['config']['display']:
                    config_item['display']['domain_title'] = relation_config['config']['display']['domain_title']
                if 'range_title' in relation_config['config']['display']:
                    config_item['display']['range_title'] = relation_config['config']['display']['range_title']
                if 'layout' in relation_config['config']['display']:
                    config_item['display']['layout'] = _layout_field_converter(
                        relation_config['config']['display']['layout'],
                        relation_field_lookup,
                    )
            results.append(config_item)

        return results

    return resolver


async def create_type_defs():
    # Main query
    # TODO provide possibility to hide some fields from config, based on permissions
    type_defs_dict = {
        'query': [
            ['Project_config', 'Project_config'],
            ['Entity_config_s', '[Entity_config]'],
            ['Relation_config_s', '[Relation_config]'],
        ],
        'project_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
        ],
        'entity_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['source', 'Boolean'],
            ['data', '[Data_config!]'],
            ['display', 'Entity_display_config'],
            ['edit', 'Entity_edit_config'],
            ['elasticsearch', 'Es_config'],
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
            ['fields', '[Display_panel_field_config!]'],
        ],
        'display_panel_field_config': [
            ['label', 'String'],
            ['field', 'String!'],
            ['type', 'String'],
            # TODO: allow multiple base layers
            # TODO: add overlays
            ['base_layer', 'String'],
            ['base_url', 'String'],
        ],
        'entity_edit_config': [
            ['layout', '[Edit_panel_config!]'],
        ],
        'edit_panel_config': [
            ['label', 'String'],
            ['fields', '[Edit_panel_field_config!]'],
        ],
        'edit_panel_field_config': [
            ['label', 'String'],
            ['field', 'String!'],
            ['type', 'String'],
        ],
        'es_config': [
            ['title', 'String!'],
            ['columns', '[Es_column_config!]'],
            ['filters', '[Es_filter_group_config!]'],
        ],
        'es_column_config': [
            ['system_name', 'String!'],
            ['display_name', 'String!'],
            ['type', 'String!'],
            ['sortable', 'Boolean!'],
            ['main_link', 'Boolean'],
            ['link', 'Boolean'],
            ['sub_field', 'String'],
            ['sub_field_type', 'String'],
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
            ['display', 'Relation_display_config'],
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

    type_defs_array = [construct_def('type', type.capitalize(), props) for type, props in type_defs_dict.items()]

    return ariadne.gql('\n\n'.join(type_defs_array))


async def create_object_types(
    request: starlette.requests.Request,
    project_name: str,
):
    object_types = {'Query': ariadne.QueryType()}

    object_types['Query'].set_field(
        'Project_config',
        project_config_resolver_wrapper(request, project_name),
    )

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
async def create_schema(request: starlette.requests.Request):
    type_defs = await create_type_defs()
    object_types = await create_object_types(
        request,
        request.path_params['project_name'],
    )

    return ariadne.make_executable_schema(type_defs, *object_types)
