
import aiodataloader
import ariadne
import typing

from starlette.requests import Request
from app.auth.permission import get_permission_entities_and_properties, get_permission_relations_and_properties

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_def, first_cap
from app.models.auth import UserWithPermissions


# TODO: only get the requested properties
# TODO: get all required information (entity -> relation -> entity -> ...) in a single request
# dataloader to prevent N+1: https://github.com/mirumee/ariadne/discussions/508)#discussioncomment-525811
def entity_resolver_wrapper(
    request: Request,
    project_name: str,
    entity_type_name: str,
):
    async def get_entities(entity_ids: typing.List[int]):
        data_repo = get_repository_from_request(request, DataRepository, project_name)
        data = await data_repo.get_entities_graphql(entity_type_name, entity_ids)
        # dataloader expects sequence of objects or None following order of ids in ids
        return [data.get(id) for id in entity_ids]

    async def load_entity(info, id: int) -> typing.Optional[typing.Dict]:
        if '__entity_loader' not in info.context:
            info.context['__entity_loader'] = aiodataloader.DataLoader(get_entities)
        return await info.context['__entity_loader'].load(id)

    async def resolver(parent, info, **_):
        return await load_entity(info, _['id'])

    return resolver


def update_entity_resolver_wrapper(
    request: Request,
    project_name: str,
    entity_type_name: str,
):
    async def update_entity(entity_id: int, input: typing.Dict):
        print('update_entity')
        print(request)
        data_repo = get_repository_from_request(request, DataRepository, project_name)
        return await data_repo.update_entity_graphql(entity_type_name, entity_id, input)

    async def resolver(_, info, id, input):
        return await update_entity(id, input)

    return resolver


def relation_resolver_wrapper(
    request: Request,
    project_name: str,
    relation_type_name: str,
    inverse: bool = False,
):
    async def get_relations(keys: typing.List[str]):
        data_repo = get_repository_from_request(request, DataRepository, project_name)
        grouped_ids = {}
        for key in keys:
            (entity_type_name, entity_id__str) = key.split('|')
            if entity_type_name not in grouped_ids:
                grouped_ids[entity_type_name] = []
            grouped_ids[entity_type_name].append(int(entity_id__str))
        grouped_data = {}
        for entity_type_name, entity_ids in grouped_ids.items():
            grouped_data[entity_type_name] = await data_repo.get_relations_graphql(
                entity_type_name,
                entity_ids,
                relation_type_name,
                inverse,
            )
        # dataloader expects sequence of objects or None following order of ids in ids
        results = []
        for key in keys:
            (entity_type_name, entity_id__str) = key.split('|')
            results.append(grouped_data.get(entity_type_name).get(int(entity_id__str)))

        return results

    async def load_relation(info, entity_type_name: str, id: int) -> typing.Optional[typing.Dict]:
        loader_key = f'__relation_loader_{project_name}_{relation_type_name}_{inverse}'
        if loader_key not in info.context:
            info.context[loader_key] = aiodataloader.DataLoader(get_relations)
        return await info.context[loader_key].load(f'{entity_type_name}|{id}')

    async def resolver(parent, info, **_):
        entity_id = parent['id']
        entity_type_name = info.parent_type.name.lower()

        db_results = await load_relation(info, entity_type_name, entity_id)

        if not db_results:
            return []

        results = []
        for db_result in db_results:
            result = db_result['relation']
            result['entity'] = db_result['entity']
            result['entity']['__typename'] = first_cap(db_result['entity_type_name'])

            result['_source_'] = []
            for source in db_result['sources']:
                source_result = source['relation']
                source_result['entity'] = source['entity']
                source_result['entity']['__typename'] = first_cap(source['entity_type_name'])
                result['_source_'].append(source_result)

            results.append(result)

        return results

    return resolver


def calc_props(
    config: typing.Dict,
    allowed_props: typing.List[str],
    type_defs_dict: typing.Dict,
    additional_type_defs_dict: typing.Dict,
):
    # TODO add properties which can contain multiple, values (sorted or unsorted)
    props = [['id', 'Int']]
    if 'data' in config:
        for prop in config['data'].values():
            if prop['system_name'] in allowed_props:
                prop_type = prop['type']
                props.append([prop['system_name'], prop_type])
                if prop_type in additional_type_defs_dict and prop_type not in type_defs_dict:
                    type_defs_dict[prop_type] = additional_type_defs_dict[prop_type]
    return props


def add_get_source_schema_parts(
    type_defs_dict: typing.Dict,
    unions: set,
    scalars: set,
    user: UserWithPermissions,
    entity_types_config: typing.Dict,
):
    source_entity_names = [
        etn
        for etn in entity_types_config
        if (
            'source' in entity_types_config[etn]['config']
            and entity_types_config[etn]['config']['source']
        )
    ]
    if not source_entity_names:
        return

    scalars.add('scalar JSON')
    unions.add(
        f'union Source_entity_types = {" | ".join([first_cap(sen) for sen in source_entity_names])}'
    )
    type_defs_dict['Source_'] = [
        ['id', 'Int!'],
        ['properties', '[String!]!'],
        ['source_props', 'JSON'],
        ['entity', 'Source_entity_types'],
    ]


def add_get_entity_schema_parts(
    type_defs_dict: typing.Dict,
    additional_type_defs_dict: typing.Dict,
    query_dict: typing.Dict,
    request: Request,
    project_name: str,
    user: UserWithPermissions,
    entity_types_config: typing.Dict,
):
    perms = get_permission_entities_and_properties(user, project_name, entity_types_config, 'get')
    for etn, allowed_props in perms.items():
        type_defs_dict['Query'].append([f'get{first_cap(etn)}(id: Int!)', first_cap(etn)])
        query_dict['Query'].set_field(
            f'get{first_cap(etn)}',
            entity_resolver_wrapper(request, project_name, etn),
        )
        # Needed for relation and source resolvers
        query_dict[first_cap(etn)] = ariadne.ObjectType(first_cap(etn))

        type_defs_dict[first_cap(etn)] = calc_props(
            entity_types_config[etn]['config'],
            allowed_props,
            type_defs_dict,
            additional_type_defs_dict,
        )

        if 'Source_' in type_defs_dict:
            type_defs_dict[first_cap(etn)].append(['_source_', '[Source_!]!'])
            query_dict[first_cap(etn)].set_field(
                '_source_',
                relation_resolver_wrapper(request, project_name, '_source_')
            )


def add_get_relation_schema_parts(
    type_defs_dict: typing.Dict,
    additional_type_defs_dict: typing.Dict,
    query_dict: typing.Dict,
    unions: set,
    request: Request,
    project_name: str,
    user: UserWithPermissions,
    relation_types_config: typing.Dict,
):
    # TODO: cardinality
    # TODO: bidirectional relations
    perms = get_permission_relations_and_properties(user, project_name, relation_types_config, 'get')
    for rtn, allowed_props in perms.items():
        # TODO provide possibility to hide relations from config, based on user permissions
        domain_names = relation_types_config[rtn]['domain_names']
        range_names = relation_types_config[rtn]['range_names']

        for domain_name in domain_names:
            type_defs_dict[first_cap(domain_name)].append([f'r_{rtn}_s', f'[R_{rtn}!]!'])
            query_dict[first_cap(domain_name)].set_field(
                f'r_{rtn}_s',
                relation_resolver_wrapper(request, project_name, rtn)
            )
        for range_name in range_names:
            type_defs_dict[first_cap(range_name)].append([f'ri_{rtn}_s', f'[Ri_{rtn}!]!'])
            query_dict[first_cap(range_name)].set_field(
                f'ri_{rtn}_s',
                relation_resolver_wrapper(request, project_name, rtn, True)
            )

        props = calc_props(
            relation_types_config[rtn]['config'],
            allowed_props,
            type_defs_dict,
            additional_type_defs_dict,
        )
        if 'Source_' in type_defs_dict:
            props.append(['_source_', '[Source_!]!'])

        unions.add(f'union Ri_{rtn}_domain = {" | ".join([first_cap(dn) for dn in domain_names])}')
        unions.add(f'union R_{rtn}_range = {" | ".join([first_cap(rn) for rn in range_names])}')

        type_defs_dict[f'R_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
        type_defs_dict[f'Ri_{rtn}'] = props + [['entity', f'Ri_{rtn}_domain']]

        # TODO: check if relation source can be queried


async def create_type_defs(
    project_name: str,
    entity_types_config: typing.Dict,
    relation_types_config: typing.Dict,
    user: UserWithPermissions,
):
    # Main query
    # TODO provide possibility to hide some fields from config, based on permissions
    additional_type_defs_dict = {
        'Geometry': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }
    additional_types = set()
    additional_input_type_defs_dict = {
        'GeometryInput': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }
    additional_input_types = set()

    type_defs_dict = {
        'Query': [[f'get{first_cap(etn)}(id: Int!)', first_cap(etn)] for etn in entity_types_config.keys()],
    }
    input_type_defs_dict = {}
    unions_array = []
    scalars_array = []

    # Sources
    source_entity_names = [
        etn
        for etn in entity_types_config
        if (
            'source' in entity_types_config[etn]['config']
            and entity_types_config[etn]['config']['source']
        )
    ]
    if source_entity_names:
        scalars_array.append('scalar JSON')
        type_defs_dict['Source_'] = [
            ['id', 'Int!'],
            ['properties', '[String!]!'],
            ['source_props', 'JSON']
        ]
        unions_array.append(
            f'union Source_entity_types = {" | ".join([first_cap(sen) for sen in source_entity_names])}'
        )
        type_defs_dict['Source_'].append(['entity', 'Source_entity_types'])

    # TODO: add props which can contain multiple, values (sorted or unsorted)
    # Entities
    for etn in entity_types_config:
        props = [['id', 'Int']]
        if 'data' in entity_types_config[etn]['config']:
            for prop in entity_types_config[etn]['config']['data'].values():
                prop_type = prop['type']
                props.append([prop['system_name'], prop_type])
                if prop_type in additional_type_defs_dict:
                    additional_types.add(prop_type)
        type_defs_dict[first_cap(etn)] = props

        # Entity sources
        if source_entity_names:
            type_defs_dict[first_cap(etn)].append(['_source_', '[Source_!]!'])

    for additional_type in additional_types:
        type_defs_dict[additional_type] = additional_type_defs_dict[additional_type]

    # Relations
    # TODO: cardinality
    # TODO: bidirectional relations
    for rtn in relation_types_config:
        domain_names = relation_types_config[rtn]['domain_names']
        range_names = relation_types_config[rtn]['range_names']
        unions_array.append(f'union Ri_{rtn}_domain = {" | ".join([first_cap(dn) for dn in domain_names])}')
        unions_array.append(f'union R_{rtn}_range = {" | ".join([first_cap(rn) for rn in range_names])}')

        props = [['id', 'Int']]
        if 'data' in relation_types_config[rtn]['config']:
            for prop in relation_types_config[rtn]['config']['data'].values():
                props.append([prop["system_name"], prop["type"]])

        type_defs_dict[f'R_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
        type_defs_dict[f'Ri_{rtn}'] = props + [['entity', f'Ri_{rtn}_domain']]

        # Relation sources
        type_defs_dict[f'R_{rtn}'].append(['_source_', '[Source_!]!'])
        type_defs_dict[f'Ri_{rtn}'].append(['_source_', '[Source_!]!'])

        for domain_name in domain_names:
            type_defs_dict[first_cap(domain_name)].append([f'r_{rtn}_s', f'[R_{rtn}!]!'])
        for range_name in range_names:
            type_defs_dict[first_cap(range_name)].append([f'ri_{rtn}_s', f'[Ri_{rtn}!]!'])

    entity_permissions = {
        perm: get_permission_entities_and_properties(user, project_name, entity_types_config, perm)
        for perm in ['update', 'create', 'delete']
    }
    if any(entity_permissions.values()):
        type_defs_dict['Mutation'] = []
        for perm in ['update', 'create']:
            if entity_permissions[perm]:
                for allowed_etn, allowed_props in entity_permissions[perm].items():
                    if 'data' in entity_types_config[etn]['config']:
                        type_defs_dict['Mutation'].append(
                            [
                                (
                                    f'{perm}{first_cap(allowed_etn)}('
                                    'id: Int!,'
                                    f'input: {first_cap(perm)}{first_cap(allowed_etn)}Input'
                                    ')'
                                ),
                                first_cap(etn),
                            ]
                        )
                        input_props = []
                        for prop in entity_types_config[allowed_etn]['config']['data'].values():
                            if prop['system_name'] in allowed_props:
                                prop_type = prop['type']
                                if prop_type in additional_type_defs_dict:
                                    prop_type += 'Input'
                                    additional_input_types.add(prop_type)
                                input_props.append([prop['system_name'], prop_type])
                        input_type_defs_dict[f'{first_cap(perm)}{first_cap(allowed_etn)}Input'] = input_props

        if entity_permissions['delete']:
            for allowed_etn in entity_permissions['delete']:
                type_defs_dict['Mutation'].append(
                    [
                        (
                            f'delete{first_cap(allowed_etn)}(id: Int!)'
                        ),
                        'Int'
                    ]
                )

        for additional_input_type in additional_input_types:
            type_defs_dict[additional_input_type] = additional_input_type_defs_dict[additional_input_type]

    type_defs_array = [construct_def('type', type, props) for type, props in type_defs_dict.items()]
    input_defs_array = [construct_def('input', input, props) for input, props in input_type_defs_dict.items()]

    return ariadne.gql(
        '\n'.join(scalars_array)
        + '\n\n'
        + '\n'.join(unions_array)
        + '\n\n'
        + '\n\n'.join(input_defs_array)
        + '\n\n'
        + '\n\n'.join(type_defs_array)
    )


async def create_object_types(
    request: Request,
    project_name: str,
    entity_types_config: typing.Dict,
    relation_types_config: typing.Dict,
    user: UserWithPermissions,
):
    object_types = {'Query': ariadne.QueryType()}

    # Entities
    for entity_type_name in entity_types_config:
        object_types['Query'].set_field(
            f'get{first_cap(entity_type_name)}',
            entity_resolver_wrapper(request, project_name, entity_type_name),
        )

        # Entity sources
        object_types[first_cap(entity_type_name)] = ariadne.ObjectType(first_cap(entity_type_name))
        object_types[first_cap(entity_type_name)].set_field(
            '_source_',
            relation_resolver_wrapper(request, project_name, '_source_')
        )

        #

    # Relations
    for relation_type_name in relation_types_config:
        for domain_name in [first_cap(dn) for dn in relation_types_config[relation_type_name]['domain_names']]:
            object_types[domain_name].set_field(
                f'r_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name)
            )
        for range_name in [first_cap(dn) for dn in relation_types_config[relation_type_name]['range_names']]:
            object_types[range_name].set_field(
                f'ri_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name, True)
            )

    return object_types.values()


# TODO: cache per project_name (app always hangs after 6 requests when using cache)
async def create_schema(
    request: Request,
    user: UserWithPermissions,
):
    project_name = request.path_params['project_name']
    config_repo = get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_types_config(project_name)
    relation_types_config = await config_repo.get_relation_types_config(project_name)

    type_defs_dict = {'Query': []}
    input_type_defs_dict = {}
    unions = set()
    scalars = set()

    query_dict = {'Query': ariadne.QueryType()}

    additional_type_defs_dict = {
        'Geometry': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }
    additional_input_type_defs_dict = {
        'GeometryInput': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }

    # First add source parts: type_defs_dict['_Source'] is checked in other schema_pars adders
    add_get_source_schema_parts(
        type_defs_dict,
        unions,
        scalars,
        user,
        entity_types_config,
    )

    # Then add entity parts: relations are later added to these
    add_get_entity_schema_parts(
        type_defs_dict,
        additional_type_defs_dict,
        query_dict,
        request,
        project_name,
        user,
        entity_types_config,
    )

    add_get_relation_schema_parts(
        type_defs_dict,
        additional_type_defs_dict,
        query_dict,
        unions,
        request,
        project_name,
        user,
        relation_types_config,
    )

    # type_defs = await create_type_defs(
    #     request.path_params['project_name'],
    #     entity_types_config,
    #     relation_types_config,
    #     user,
    # )
    # object_types = await create_object_types(
    #     request,
    #     request.path_params['project_name'],
    #     entity_types_config,
    #     relation_types_config,
    # )
    type_defs_array = [construct_def('type', type, props) for type, props in type_defs_dict.items()]
    input_type_defs_array = [construct_def('input', type, props) for type, props in input_type_defs_dict.items()]
    type_defs = ariadne.gql(
        '\n'.join(list(scalars))
        + '\n\n'
        + '\n'.join(list(unions))
        + '\n\n'
        + '\n\n'.join(input_type_defs_array)
        + '\n\n'
        + '\n\n'.join(type_defs_array)
    )

    return ariadne.make_executable_schema(type_defs, *query_dict.values())
