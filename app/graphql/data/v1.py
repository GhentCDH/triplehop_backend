
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
from app.cache.core import create_schema_key_builder

from app.db.core import get_repository_from_request
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.graphql.base import construct_def, first_cap
from app.mgmt.data import DataManager
from app.models.auth import UserWithPermissions


class GraphQLDataBuilder:
    def __init__(
        self,
        request: Request,
        user: UserWithPermissions
    ) -> None:
        self._project_name = request.path_params['project_name']
        self._config_repo = get_repository_from_request(request, ConfigRepository)
        self._data_repo = get_repository_from_request(request, DataRepository, self._project_name)
        self._user = user
        self._data_manager = DataManager(self._project_name, self._config_repo, self._data_repo, self._user)

    # TODO: only get the requested properties
    # TODO: get all required information (entity -> relation -> entity -> ...) in a single request
    # dataloader to prevent N+1: https://github.com/mirumee/ariadne/discussions/508)#discussioncomment-525811
    def _get_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        async def get_entities(entity_ids: typing.List[int]):
            data = await self._data_repo.get_entities_graphql(entity_type_name, entity_ids)
            # dataloader expects sequence of objects or None following order of ids in ids
            return [data.get(id) for id in entity_ids]

        async def load_entity(info, id: int) -> typing.Optional[typing.Dict]:
            if '__entity_loader' not in info.context:
                info.context['__entity_loader'] = aiodataloader.DataLoader(get_entities)
            return await info.context['__entity_loader'].load(id)

        async def resolver(parent, info, **_):
            return await load_entity(info, _['id'])

        return resolver

    def _post_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        # TODO
        async def post_entity(entity_id: int, input: typing.Dict):
            print('post_entity')
            print(self._request)
            return await self._data_repo.put_entity_graphql(entity_type_name, entity_id, input)

        async def resolver(_, info, id, input):
            return await post_entity(id, input)

        return resolver

    def _put_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        async def put_entity(entity_id: int, input: typing.Dict):
            return await self._data_manager.put_entity(entity_type_name, entity_id, input)

        async def resolver(_, info, id, input):
            return await put_entity(id, input)

        return resolver

    def _relation_resolver_wrapper(
        self,
        relation_type_name: str,
        inverse: bool = False,
    ):
        async def get_relations(keys: typing.List[str]):
            grouped_ids = {}
            for key in keys:
                (entity_type_name, entity_id__str) = key.split('|')
                if entity_type_name not in grouped_ids:
                    grouped_ids[entity_type_name] = []
                grouped_ids[entity_type_name].append(int(entity_id__str))
            grouped_data = {}
            for entity_type_name, entity_ids in grouped_ids.items():
                grouped_data[entity_type_name] = await self._data_repo.get_relations_graphql(
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
            loader_key = f'__relation_loader_{self._project_name}_{relation_type_name}_{inverse}'
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

    def _calc_props(
        self,
        entity_or_relation: str,
        type_name: str,
        allowed_props: typing.List[str],
        add_id: bool = True,
        input: bool = False,
    ):
        if entity_or_relation == 'entity':
            config = self._entity_types_config[type_name]['config']
        else:
            config = self._relation_types_config[type_name]['config']

        if add_id:
            props = [['id', 'Int']]
        else:
            props = []

        # TODO add properties which can contain multiple, values (sorted or unsorted)
        if 'data' in config:
            for prop in config['data'].values():
                if prop['system_name'] in allowed_props:
                    prop_type = prop['type']
                    # Input types might differ from query types
                    if input and f'{prop_type}Input' in self._additional_input_type_defs_dict.keys():
                        props.append([prop['system_name'], f'{prop_type}Input'])
                    else:
                        props.append([prop['system_name'], prop_type])
        return props

    def _add_additional_props(
        self,
        props: typing.List[typing.List],
        input: bool = False,
    ):
        if input:
            additonal_type_defs_dict = self._additional_input_type_defs_dict
            type_defs_dict = self._input_type_defs_dict
        else:
            additonal_type_defs_dict = self._additional_type_defs_dict
            type_defs_dict = self._type_defs_dict
        for _, prop_type in props:
            if prop_type in additonal_type_defs_dict and prop_type not in type_defs_dict:
                type_defs_dict[prop_type] = additonal_type_defs_dict[prop_type]

    def _get_permission_entities_and_properties(self, permission):
        return get_permission_entities_and_properties(
            self._user,
            self._project_name,
            self._entity_types_config,
            permission,
        )

    def _get_permission_relations_and_properties(self, permission):
        return get_permission_relations_and_properties(
            self._user,
            self._project_name,
            self._relation_types_config,
            permission,
        )

    def _add_get_source_schema_parts(self):
        source_entity_names = [
            etn
            for etn in self._entity_types_config
            if (
                'source' in self._entity_types_config[etn]['config']
                and self._entity_types_config[etn]['config']['source']
            )
        ]
        if not source_entity_names:
            return

        self._scalars.add('scalar JSON')
        self._unions.add(
            f'union Source_entity_types = {" | ".join([first_cap(sen) for sen in source_entity_names])}'
        )
        self._type_defs_dict['Source_'] = [
            ['id', 'Int!'],
            ['properties', '[String!]!'],
            ['source_props', 'JSON'],
            ['entity', 'Source_entity_types'],
        ]

    def _add_get_entity_schema_parts(self) -> None:
        allowed = self._get_permission_entities_and_properties('get')
        for etn, allowed_props in allowed.items():
            self._type_defs_dict['Query'].append([f'get{first_cap(etn)}(id: Int!)', first_cap(etn)])
            self._query_dict['Query'].set_field(
                f'get{first_cap(etn)}',
                self._get_entity_resolver_wrapper(etn),
            )
            # Needed for relation and source resolvers
            self._query_dict[first_cap(etn)] = ariadne.ObjectType(first_cap(etn))

            props = self._calc_props(
                'entity',
                etn,
                allowed_props,
            )
            self._add_additional_props(
                props,
            )
            self._type_defs_dict[first_cap(etn)] = props

            if 'Source_' in self._type_defs_dict:
                self._type_defs_dict[first_cap(etn)].append(['_source_', '[Source_!]!'])
                self._query_dict[first_cap(etn)].set_field(
                    '_source_',
                    self._relation_resolver_wrapper('_source_')
                )

    def _add_post_put_entity_schema_parts(self) -> None:

        alloweds = {
            perm: self._get_permission_entities_and_properties(perm)
            for perm in ['post', 'put']
        }

        for perm, allowed in alloweds.items():
            if len(allowed.keys()) != 0:
                if 'Mutation' not in self._type_defs_dict:
                    self._type_defs_dict['Mutation'] = []
                if 'Mutation' not in self._query_dict:
                    self._query_dict['Mutation'] = ariadne.MutationType()

                for etn, allowed_props in allowed.items():
                    self._type_defs_dict['Mutation'].append(
                        [
                            f'{perm}{first_cap(etn)}(id: Int!, input: {first_cap(perm)}{first_cap(etn)}Input)',
                            first_cap(etn),
                        ]
                    )
                    if perm == 'post':
                        self._query_dict['Mutation'].set_field(
                            f'post{first_cap(etn)}',
                            self._post_entity_resolver_wrapper(etn),
                        )
                    else:
                        self._query_dict['Mutation'].set_field(
                            f'put{first_cap(etn)}',
                            self._put_entity_resolver_wrapper(etn),
                        )

                    props = self._calc_props(
                        'entity',
                        etn,
                        allowed_props,
                        # only global admins can update ids
                        has_global_permission(self._user, perm),
                        True
                    )
                    self._add_additional_props(props, True)
                    self._input_type_defs_dict[f'{first_cap(perm)}{first_cap(etn)}Input'] = props

    def _add_get_relation_schema_parts(self) -> None:
        # TODO: cardinality
        # TODO: bidirectional relations
        allowed = self._get_permission_relations_and_properties('get')
        for rtn, allowed_props in allowed.items():
            domain_names = self._relation_types_config[rtn]['domain_names']
            range_names = self._relation_types_config[rtn]['range_names']

            for domain_name in domain_names:
                self._type_defs_dict[first_cap(domain_name)].append([f'r_{rtn}_s', f'[R_{rtn}!]!'])
                self._query_dict[first_cap(domain_name)].set_field(
                    f'r_{rtn}_s',
                    self._relation_resolver_wrapper(rtn)
                )
            for range_name in range_names:
                self._type_defs_dict[first_cap(range_name)].append([f'ri_{rtn}_s', f'[Ri_{rtn}!]!'])
                self._query_dict[first_cap(range_name)].set_field(
                    f'ri_{rtn}_s',
                    self._relation_resolver_wrapper(rtn, True)
                )

            props = self._calc_props(
                'relation',
                rtn,
                allowed_props,
            )
            self._add_additional_props(props)
            if 'Source_' in self._type_defs_dict:
                props.append(['_source_', '[Source_!]!'])

            self._unions.add(f'union Ri_{rtn}_domain = {" | ".join([first_cap(dn) for dn in domain_names])}')
            self._unions.add(f'union R_{rtn}_range = {" | ".join([first_cap(rn) for rn in range_names])}')

            self._type_defs_dict[f'R_{rtn}'] = props + [['entity', f'R_{rtn}_range']]
            self._type_defs_dict[f'Ri_{rtn}'] = props + [['entity', f'Ri_{rtn}_domain']]

    # TODO: reset cache when project is updated or user permissions have been updated
    @aiocache.cached(key_builder=create_schema_key_builder)
    async def create_schema(self):
        self._entity_types_config = await self._config_repo.get_entity_types_config(self._project_name)
        self._relation_types_config = await self._config_repo.get_relation_types_config(self._project_name)

        self._type_defs_dict = {'Query': []}
        self._input_type_defs_dict = {}
        self._unions = set()
        self._scalars = set()

        self._query_dict = {'Query': ariadne.QueryType()}

        self._additional_type_defs_dict = {
            'Geometry': [
                ['type', 'String!'],
                ['coordinates', '[Float!]!'],
            ],
        }
        self._additional_input_type_defs_dict = {
            'GeometryInput': [
                ['type', 'String!'],
                ['coordinates', '[Float!]!'],
            ],
        }

        # First add source parts: type_defs_dict['_Source'] is checked in other schema_pars adders
        self._add_get_source_schema_parts()

        # Then add entity parts: relations are later added to these
        self._add_get_entity_schema_parts()
        self._add_post_put_entity_schema_parts()

        self._add_get_relation_schema_parts()

        type_defs_array = [
            construct_def('type', type, props)
            for type, props in self._type_defs_dict.items()
        ]
        input_type_defs_array = [
            construct_def('input', type, props)
            for type, props in self._input_type_defs_dict.items()
        ]
        type_defs = ariadne.gql(
            '\n'.join(list(self._scalars))
            + '\n\n'
            + '\n'.join(list(self._unions))
            + '\n\n'
            + '\n\n'.join(input_type_defs_array)
            + '\n\n'
            + '\n\n'.join(type_defs_array)
        )

        schema = ariadne.make_executable_schema(type_defs, *self._query_dict.values())

        return schema
