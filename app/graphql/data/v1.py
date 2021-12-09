import aiodataloader
import ariadne
import typing

from starlette.requests import Request

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


async def create_type_defs(
    entity_types_config: typing.Dict,
    relation_types_config: typing.Dict,
):
    # Main query
    # TODO provide possibility to hide some fields from config, based on permissions
    type_defs_dict = {
        'Query': [[f'get{first_cap(etn)}(id: Int!)', first_cap(etn)] for etn in entity_types_config.keys()],
        'Mutation': [
            [f'update{first_cap(etn)}(id: Int!, input: {first_cap(etn)}Input)', first_cap(etn)]
            for etn in entity_types_config.keys()
        ],
        'Geometry': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }
    input_defs_dict = {
        'GeometryInput': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }
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
    scalars_array.append('scalar JSON')
    type_defs_dict['Source_'] = [
        ['id', 'Int!'],
        ['properties', '[String!]!'],
        ['source_props', 'JSON']
    ]
    if source_entity_names:
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
                props.append([prop["system_name"], prop["type"]])
        type_defs_dict[first_cap(etn)] = props

        input_props = []
        if 'data' in entity_types_config[etn]['config']:
            for prop in entity_types_config[etn]['config']['data'].values():
                prop_type = prop["type"]
                if prop_type in ['Geometry']:
                    prop_type += 'Input'
                input_props.append([prop["system_name"], prop_type])
        input_defs_dict[f'{first_cap(etn)}Input'] = input_props

        # Entity sources
        type_defs_dict[first_cap(etn)].append(['_source_', '[Source_!]!'])

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

    type_defs_array = [construct_def('type', type, props) for type, props in type_defs_dict.items()]
    input_defs_array = [construct_def('input', input, props) for input, props in input_defs_dict.items()]

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
    print(user.permissions)
    config_repo = get_repository_from_request(request, ConfigRepository)
    entity_types_config = await config_repo.get_entity_types_config(request.path_params['project_name'])
    relation_types_config = await config_repo.get_relation_types_config(request.path_params['project_name'])

    type_defs = await create_type_defs(entity_types_config, relation_types_config)
    object_types = await create_object_types(
        request,
        request.path_params['project_name'],
        entity_types_config,
        relation_types_config,
    )

    return ariadne.make_executable_schema(type_defs, *object_types)
