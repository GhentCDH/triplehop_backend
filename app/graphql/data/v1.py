
import aiocache
import aiodataloader
import ariadne
import typing

from starlette.requests import Request
from app.auth.permission import (
    get_permission_entities_and_properties,
    get_permission_relations_and_properties,
    has_global_permission,
)
from app.cache.core import request_user_key_builder

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_def, first_cap
from app.models.auth import UserWithPermissions


# TODO: only get the requested properties
# TODO: get all required information (entity -> relation -> entity -> ...) in a single request
# dataloader to prevent N+1: https://github.com/mirumee/ariadne/discussions/508)#discussioncomment-525811
def get_entity_resolver_wrapper(
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


def post_entity_resolver_wrapper(
    request: Request,
    project_name: str,
    entity_type_name: str,
):
    # TODO
    async def post_entity(entity_id: int, input: typing.Dict):
        print('post_entity')
        print(request)
        data_repo = get_repository_from_request(request, DataRepository, project_name)
        return await data_repo.put_entity_graphql(entity_type_name, entity_id, input)

    async def resolver(_, info, id, input):
        return await post_entity(id, input)

    return resolver


def put_entity_resolver_wrapper(
    request: Request,
    project_name: str,
    entity_type_name: str,
):
    async def put_entity(entity_id: int, input: typing.Dict):
        print('update_entity')
        print(request)
        data_repo = get_repository_from_request(request, DataRepository, project_name)
        return await data_repo.put_entity_graphql(entity_type_name, entity_id, input)

    async def resolver(_, info, id, input):
        return await put_entity(id, input)

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
    add_id: bool = True,
):
    # TODO add properties which can contain multiple, values (sorted or unsorted)
    if add_id:
        props = [['id', 'Int']]
    else:
        props = []
    if 'data' in config:
        for prop in config['data'].values():
            if prop['system_name'] in allowed_props:
                prop_type = prop['type']
                props.append([prop['system_name'], prop_type])
    return props


def add_additional_props(
    type_defs_dict: typing.Dict,
    additional_type_defs_dict: typing.Dict,
    props: typing.List[typing.List],
):
    for _, prop_type in props:
        if prop_type in additional_type_defs_dict and prop_type not in type_defs_dict:
            type_defs_dict[prop_type] = additional_type_defs_dict[prop_type]


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
) -> None:
    allowed = get_permission_entities_and_properties(user, project_name, entity_types_config, 'get')
    for etn, allowed_props in allowed.items():
        type_defs_dict['Query'].append([f'get{first_cap(etn)}(id: Int!)', first_cap(etn)])
        query_dict['Query'].set_field(
            f'get{first_cap(etn)}',
            get_entity_resolver_wrapper(request, project_name, etn),
        )
        # Needed for relation and source resolvers
        query_dict[first_cap(etn)] = ariadne.ObjectType(first_cap(etn))

        props = calc_props(
            entity_types_config[etn]['config'],
            allowed_props,
        )
        add_additional_props(
            type_defs_dict,
            additional_type_defs_dict,
            props,
        )
        type_defs_dict[first_cap(etn)] = props

        if 'Source_' in type_defs_dict:
            type_defs_dict[first_cap(etn)].append(['_source_', '[Source_!]!'])
            query_dict[first_cap(etn)].set_field(
                '_source_',
                relation_resolver_wrapper(request, project_name, '_source_')
            )


def add_post_put_entity_schema_parts(
    type_defs_dict: typing.Dict,
    input_type_defs_dict: typing.Dict,
    additional_input_type_defs_dict: typing.Dict,
    query_dict: typing.Dict,
    request: Request,
    project_name: str,
    user: UserWithPermissions,
    entity_types_config: typing.Dict,
) -> None:

    alloweds = {
        perm: get_permission_entities_and_properties(user, project_name, entity_types_config, perm)
        for perm in ['post', 'put']
    }

    for perm, allowed in alloweds.items():
        if len(allowed.keys()) != 0:
            if 'Mutation' not in type_defs_dict:
                type_defs_dict['Mutation'] = []
            if 'Mutation' not in query_dict:
                query_dict['Mutation'] = ariadne.MutationType()

            for etn, allowed_props in allowed.items():
                type_defs_dict['Mutation'].append(
                    [
                        f'{perm}{first_cap(etn)}(id: Int!, input: {first_cap(perm)}{first_cap(etn)}Input)',
                        first_cap(etn),
                    ]
                )
                if perm == 'post':
                    query_dict['Mutation'].set_field(
                        f'post{first_cap(etn)}',
                        post_entity_resolver_wrapper(request, project_name, etn),
                    )
                else:
                    query_dict['Mutation'].set_field(
                        f'put{first_cap(etn)}',
                        put_entity_resolver_wrapper(request, project_name, etn),
                    )

                props = []
                for prop_name, prop_type in calc_props(
                    entity_types_config[etn]['config'],
                    allowed_props,
                    # only global admins can update ids
                    has_global_permission(user, perm),
                ):
                    # Input types might differ from query types
                    if f'{prop_type}Input' in additional_input_type_defs_dict.keys():
                        props.append([prop_name, f'{prop_type}Input'])
                    else:
                        props.append([prop_name, prop_type])
                add_additional_props(
                    input_type_defs_dict,
                    additional_input_type_defs_dict,
                    props,
                )
                input_type_defs_dict[f'{first_cap(perm)}{first_cap(etn)}Input'] = props


def add_get_relation_schema_parts(
    type_defs_dict: typing.Dict,
    additional_type_defs_dict: typing.Dict,
    query_dict: typing.Dict,
    unions: set,
    request: Request,
    project_name: str,
    user: UserWithPermissions,
    relation_types_config: typing.Dict,
) -> None:
    # TODO: cardinality
    # TODO: bidirectional relations
    allowed = get_permission_relations_and_properties(user, project_name, relation_types_config, 'get')
    for rtn, allowed_props in allowed.items():
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
        )
        add_additional_props(
            type_defs_dict,
            additional_type_defs_dict,
            props,
        )
        if 'Source_' in type_defs_dict:
            props.append(['_source_', '[Source_!]!'])

        unions.add(f'union Ri_{rtn}_domain = {" | ".join([first_cap(dn) for dn in domain_names])}')
        unions.add(f'union R_{rtn}_range = {" | ".join([first_cap(rn) for rn in range_names])}')

        type_defs_dict[f'R_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
        type_defs_dict[f'Ri_{rtn}'] = props + [['entity', f'Ri_{rtn}_domain']]


# TODO: reset cache when project is updated or user permissions have been updated
@aiocache.cached(key_builder=request_user_key_builder)
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
    add_post_put_entity_schema_parts(
        type_defs_dict,
        input_type_defs_dict,
        additional_input_type_defs_dict,
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

    schema = ariadne.make_executable_schema(type_defs, *query_dict.values())

    return schema
