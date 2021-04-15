import aiodataloader
import ariadne
import typing

from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_type_def


# TODO: only get the requested properties
# TODO: get all required information (entity -> relation -> entity -> ...) in a single request
# dataloader to prevent N+1: https://github.com/mirumee/ariadne/discussions/508)#discussioncomment-525811
def entity_resolver_wrapper(
    request: Request,
    project_name: str,
    entity_type_name: str,
):
    async def get_entities(entity_ids: typing.List[int]):
        data_repo = await get_repository_from_request(request, DataRepository, project_name)
        data = await data_repo.get_entities(entity_type_name, entity_ids)
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
        print('get_relations')
        print(relation_type_name)
        data_repo = await get_repository_from_request(request, DataRepository, project_name)
        grouped_ids = {}
        for key in keys:
            (entity_type_name, entity_id__str) = key.split('|')
            if entity_type_name not in grouped_ids:
                grouped_ids[entity_type_name] = []
            grouped_ids[entity_type_name].append(int(entity_id__str))
        grouped_data = {}
        for entity_type_name, entity_ids in grouped_ids.items():
            grouped_data[entity_type_name] = await data_repo.get_relations(
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
            result['entity']['__typename'] = db_result['entity_type_name'].capitalize()

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
        'query': [[f'{etn.capitalize()}(id: Int!)', etn.capitalize()] for etn in entity_types_config.keys()],
        'geometry': [
            ['type', 'String!'],
            ['coordinates', '[Float!]!'],
        ],
    }

    # TODO: add props which can contain multiple, values (sorted or unsorted)
    # Entities
    for etn in entity_types_config:
        props = [['id', 'Int']]
        if 'data' in entity_types_config[etn]['config']:
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
        unions_array.append(f'union Ri_{rtn}_domain = {" | ".join([dn.capitalize() for dn in domain_names])}')
        unions_array.append(f'union R_{rtn}_range = {" | ".join([rn.capitalize() for rn in range_names])}')

        props = [['id', 'Int']]
        if 'data' in relation_types_config[rtn]['config']:
            for prop in relation_types_config[rtn]['config']['data'].values():
                props.append([prop["system_name"], prop["type"]])

        type_defs_dict[f'r_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
        type_defs_dict[f'ri_{rtn}'] = props + [['entity', f'Ri_{rtn}_domain']]

        for domain_name in domain_names:
            type_defs_dict[domain_name].append([f'r_{rtn}_s', f'[R_{rtn}!]!'])
        for range_name in range_names:
            type_defs_dict[range_name].append([f'ri_{rtn}_s', f'[Ri_{rtn}!]!'])

    type_defs_array = [construct_type_def(type.capitalize(), props) for type, props in type_defs_dict.items()]

    return ariadne.gql('\n'.join(unions_array) + '\n\n' + '\n\n'.join(type_defs_array))


async def create_object_types(
    request: Request,
    project_name: str,
    entity_types_config: typing.Dict,
    relation_types_config: typing.Dict,
):
    object_types = {'Query': ariadne.QueryType()}

    for entity_type_name in entity_types_config:
        object_types['Query'].set_field(
            entity_type_name.capitalize(),
            entity_resolver_wrapper(request, project_name, entity_type_name),
        )

    for relation_type_name in relation_types_config:
        for domain_name in [dn.capitalize() for dn in relation_types_config[relation_type_name]['domain_names']]:
            if domain_name not in object_types:
                object_types[domain_name] = ariadne.ObjectType(domain_name)

            object_types[domain_name].set_field(
                f'r_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name)
            )
        for range_name in [dn.capitalize() for dn in relation_types_config[relation_type_name]['range_names']]:
            if range_name not in object_types:
                object_types[range_name] = ariadne.ObjectType(range_name)

            object_types[range_name].set_field(
                f'ri_{relation_type_name}_s',
                relation_resolver_wrapper(request, project_name, relation_type_name, True)
            )

    return object_types.values()


# TODO: cache per project_name (app always hangs after 6 requests when using cache)
async def create_schema(
    request: Request,
):
    config_repo = await get_repository_from_request(request, ConfigRepository)
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
