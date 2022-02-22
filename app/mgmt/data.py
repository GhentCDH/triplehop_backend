import fastapi
import json
import typing
import starlette

from app.db.core import get_repository_from_request
from app.db.data import DataRepository
from app.mgmt.auth import allowed_entities_or_relations_and_properties
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.utils import RE_SOURCE_PROP_INDEX, dtu, first_cap, utd


class DataManager:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions = None,
    ):
        self._project_name = request.path_params['project_name']
        self._config_manager = ConfigManager(request, user)
        self._data_repo = get_repository_from_request(request, DataRepository)
        self._user = user
        self._entity_types_config = None
        self._relation_types_config = None
        self._project_id = None

    async def _get_project_id(self):
        if self._project_id is None:
            self._project_id = await self._config_manager.get_project_id_by_name(self._project_name)
        return self._project_id

    @staticmethod
    def valid_prop_value(prop_type: str, prop_value: typing.Any) -> bool:
        """Check if a property value is of the correct type."""
        if prop_type == 'String':
            return isinstance(prop_value, str)

    @staticmethod
    def _require_entity_type_name_or_entity_type_id(entity_type_name, entity_type_id) -> None:
        if entity_type_name is not None and entity_type_id is not None:
            raise Exception('Only one keyword argument is required: entity_type_name or entity_type_id')
        if entity_type_name is None and entity_type_id is None:
            raise Exception('One keyword argument is required: entity_type_name or entity_type_id')

    @staticmethod
    def _require_relation_type_name_or_entity_type_id(relation_type_name, relation_type_id) -> None:
        if relation_type_name is not None and relation_type_id is not None:
            raise Exception('Only one keyword argument is required: relation_type_name or relation_type_id')
        if relation_type_name is None and relation_type_id is None:
            raise Exception('One keyword argument is required: relation_type_name or relation_type_id')

    async def _check_permission(
        self,
        permission: str,
        entities_or_relations: str,
        type_name: str,
        props: typing.List,
    ) -> None:
        # TODO: check source permissions using config
        if (
            entities_or_relations == 'relations'
            and type_name == '_source_'
            and permission == 'get'
        ):
            for prop in props:
                if prop not in ['id', 'properties', 'source_props']:
                    raise fastapi.exceptions.HTTPException(status_code=403, detail="Forbidden")
            return

        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            entities_or_relations,
            'data',
            permission,
        )
        if type_name not in allowed:
            raise fastapi.exceptions.HTTPException(status_code=403, detail="Forbidden")

        allowed_props = allowed[type_name]
        for prop in props:
            if prop not in allowed_props:
                raise fastapi.exceptions.HTTPException(status_code=403, detail="Forbidden")

    async def _validate_input(
        self,
        entity_type_name: str,
        input: typing.Dict,
    ) -> None:
        # TODO: set _entity_types_config on init
        if self._entity_types_config is None:
            self._entity_types_config = await self._config_manager.get_entity_types_config(self._project_name)

        data_config = self._entity_types_config[entity_type_name]['config']['data']['fields']
        for prop_name, prop_value in input.items():
            etipm = await self._config_manager.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
            # Strip p_ from prop id
            prop_type = data_config[utd(etipm[prop_name][2:])]['type']
            if not self.__class__.valid_prop_value(prop_type, prop_value):
                raise fastapi.exceptions.HTTPException(status_code=422, detail="Invalid value")

    async def _get_entities_crdb(
        self,
        entity_ids: typing.List[int],
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
    ) -> typing.Dict:
        self.__class__._require_entity_type_name_or_entity_type_id(entity_type_name, entity_type_id)

        if entity_type_id is None:
            entity_type_id = await self._config_manager.get_entity_type_id_by_name(self._project_name, entity_type_name)

        records = await self._data_repo.get_entities(await self._get_project_id(), entity_type_id, entity_ids)

        results = {
            record['id']: {
                'e_props': json.loads(record['properties'])
            }
            for record in records
        }
        return results

    async def get_entities(
        self,
        entity_type_name: str,
        props: typing.List[str],
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        await self._check_permission('get', 'entities', entity_type_name, props)

        crdb_results = await self._get_entities_crdb(entity_ids, entity_type_name=entity_type_name)
        if len(crdb_results) == 0:
            return {}

        etpm = await self._config_manager.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {
            entity_id: {
                etpm[k]: v
                for k, v in crdb_result['e_props'].items()
                if k in etpm
            }
            for entity_id, crdb_result in crdb_results.items()
        }

    async def put_entity(
        self,
        entity_type_name: str,
        entity_id: int,
        input: typing.Dict,
    ):
        await self._check_permission('put', 'entities', entity_type_name, input.keys())

        await self._validate_input(entity_type_name, input)

        # Insert in database
        entity_type_id = await self._config_manager.get_entity_type_id_by_name(self._project_name, entity_type_name)
        etipm = await self._config_manager.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
        db_input = {
            etipm[k]: v
            for k, v in input.items()
        }

        async with self._data_repo.connection() as connection:
            async with connection.transaction():
                old_raw_entities = await self._data_repo.get_entities(
                    await self._get_project_id(),
                    entity_type_id,
                    [entity_id],
                    connection
                )
                if len(old_raw_entities) != 1 or old_raw_entities[0]['id'] != entity_id:
                    raise fastapi.exceptions.HTTPException(status_code=404, detail="Entity not found")
                old_entity = json.loads(old_raw_entities[0]['properties'])
                new_raw_entity = await self._data_repo.put_entity(
                    await self._get_project_id(),
                    entity_type_id,
                    entity_id,
                    db_input,
                    connection
                )
                if new_raw_entity is None:
                    raise fastapi.exceptions.HTTPException(status_code=404, detail="Entity not found")
                # strip off ::vertex
                new_entity = json.loads(new_raw_entity['n'][:-8])['properties']

                print(old_entity)
                print(new_entity)
                # TODO: log change

        etpm = await self._config_manager.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {etpm[k]: v for k, v in new_entity.items() if k in etpm}

        # # Update elasticsearch (directly and inderectly)

    async def _get_relations_crdb(
        self,
        entity_ids: typing.List[int],
        inverse: bool = False,
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
        relation_type_name: typing.Optional[str] = None,
        relation_type_id: typing.Optional[str] = None,
    ) -> typing.Dict:
        '''
        Get relations and linked entity information starting from an entity type, entity ids and a relation type.

        Return: Dict = {
            entity_id: {
                relation_id: {
                    r_props: Dict, # relation properties
                    e_props: Dict, # linked entity properties
                    entity_type_id: str, # linked entity type
                }
            }
        }
        '''
        self.__class__._require_entity_type_name_or_entity_type_id(entity_type_name, entity_type_id)
        self.__class__._require_relation_type_name_or_entity_type_id(relation_type_name, relation_type_id)

        if entity_type_id is None:
            entity_type_id = await self._config_manager.get_entity_type_id_by_name(
                self._project_name,
                entity_type_name,
            )

        if relation_type_id is None:
            relation_type_id = await self._config_manager.get_relation_type_id_by_name(
                self._project_name,
                relation_type_name,
            )

        records = await self._data_repo.get_relations(
            await self._get_project_id(),
            entity_type_id,
            entity_ids,
            relation_type_id,
            inverse
        )

        # build temporary dict so json only needs to be loaded once
        results = {}
        for record in records:
            entity_id = record['id']
            relation_properties = json.loads(record['e_properties'])
            entity_properties = json.loads(record['n_properties'])
            etid = await self._data_repo.get_entity_type_id_from_vertex_graph_id(
                await self._get_project_id(),
                record['n_id'],
            )

            if entity_id not in results:
                results[entity_id] = {}

            results[entity_id][relation_properties['id']] = {
                'r_props': relation_properties,
                'e_props': entity_properties,
                'entity_type_id': etid,
                'sources': [],
            }
        return results

    async def get_relations(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        relation_type_name: str,
        inverse: bool = False,
    ) -> typing.Dict:
        # TODO: check permission for requested properties
        await self._check_permission('get', 'relations', relation_type_name, {})

        crdb_results = await self._get_relations_crdb(
            entity_ids,
            inverse,
            entity_type_name=entity_type_name,
            relation_type_name=relation_type_name
        )

        if len(crdb_results) == 0:
            return {}

        relation_ids = [rid for eid in crdb_results for rid in crdb_results[eid]]
        relation_type_id = await self._config_manager.get_relation_type_id_by_name(
            self._project_name,
            relation_type_name,
        )
        source_records = await self._data_repo.get_relation_sources(
            await self._get_project_id(),
            relation_type_id,
            relation_ids,
        )

        # build temporary dict so sources can easily be retrieved
        source_results = {}
        for source_record in source_records:
            rel_id = source_record['id']
            if rel_id not in source_results:
                source_results[rel_id] = []

            source_results[rel_id].append({
                'r_props': json.loads(source_record['e_properties']),
                'e_props': json.loads(source_record['n_properties']),
                'entity_type_id': await self._data_repo.get_entity_type_id_from_vertex_graph_id(
                    await self._get_project_id(),
                    source_record['n_id'],
                ),
            })

        rtpm = await self._config_manager.get_relation_type_property_mapping(self._project_name, relation_type_name)
        etpma = await self._config_manager.get_entity_type_property_mapping(self._project_name, '__all__')
        rtpma = await self._config_manager.get_relation_type_property_mapping(self._project_name, '__all__')
        srtpm = await self._config_manager.get_relation_type_property_mapping(self._project_name, '_source_')
        etd = {}

        results = {}
        for entity_id, crdb_result in crdb_results.items():
            results[entity_id] = []
            for rel_id, rel_result in crdb_result.items():
                etid = rel_result['entity_type_id']
                # keep a dict of entity type definitions
                if etid not in etd:
                    etn = await self._config_manager.get_entity_type_name_by_id(self._project_name, etid)
                    etd[etid] = {
                        'etn': etn,
                        'etpm': await self._config_manager.get_entity_type_property_mapping(self._project_name, etn)
                    }
                etpm = etd[etid]['etpm']

                result = {
                    rtpm[k]: v
                    for k, v in rel_result['r_props'].items()
                    if k in rtpm
                }
                result['entity'] = {
                    etpm[k]: v
                    for k, v in rel_result['e_props'].items()
                    if k in etpm
                }
                result['entity']['__typename'] = first_cap(etd[etid]['etn'])

                # Add properties for source relations
                if relation_type_id == '_source_':
                    if 'properties' in result:
                        props = []
                        for p in result['properties']:
                            m = RE_SOURCE_PROP_INDEX.match(p)
                            if m:
                                p = f'p_{dtu(m.group("property"))}'
                                if p in etpma:
                                    props.append(f'{etpma[p]}[{m.group("index")}]')
                            else:
                                p = f'p_{dtu(p)}'
                                if p in etpma:
                                    props.append(etpma[p])
                        result['properties'] = props

                # Source information on relations
                result['_source_'] = []
                if rel_id in source_results:
                    for source in source_results[rel_id]:
                        setid = source['entity_type_id']
                        if setid not in etd:
                            etn = await self._config_manager.get_entity_type_name_by_id(self._project_name, setid)
                            etd[setid] = {
                                'etn': etn,
                                'etpm': await self._config_manager.get_entity_type_property_mapping(
                                    self._project_name,
                                    etn,
                                )
                            }
                        setpm = etd[setid]['etpm']

                        source_result = {
                            srtpm[k]: v
                            for k, v in source['r_props'].items()
                            if k in srtpm
                        }
                        source_result['entity'] = {
                            setpm[k]: v
                            for k, v in source['e_props'].items()
                            if k in setpm
                        }
                        source_result['entity']['__typename'] = first_cap(etd[setid]['etn'])
                        if 'properties' in source_result:
                            props = []
                            for p in source_result['properties']:
                                m = RE_SOURCE_PROP_INDEX.match(p)
                                if m:
                                    p = f'p_{dtu(m.group("property"))}'
                                    if p in rtpma:
                                        props.append(f'{rtpma[p]}[{m.group("index")}]')
                                else:
                                    p = f'p_{dtu(p)}'
                                    if p in rtpm:
                                        props.append(rtpma[p])
                            source_result['properties'] = props
                        result['_source_'].append(source_result)

            results[entity_id].append(result)
        return results

    async def get_entity_ids_by_type_name(
        self,
        entity_type_name: str,
    ):
        return await self._data_repo.get_entity_ids(
            await self._get_project_id(),
            await self._config_manager.get_entity_type_id_by_name(self._project_name, entity_type_name)
        )

    async def get_entity_data(
        self,
        entity_ids: typing.List[int],
        crdb_query: typing.Dict,
        first_iteration: bool = True,
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
    ) -> typing.Dict:
        if not entity_ids:
            return {}

        if not crdb_query:
            raise Exception('Empty query')

        self.__class__._require_entity_type_name_or_entity_type_id(entity_type_name, entity_type_id)

        entity_type_name_or_id = {}
        if entity_type_name is not None:
            entity_type_name_or_id['entity_type_name'] = entity_type_name
        elif entity_type_id is not None:
            entity_type_name_or_id['entity_type_id'] = entity_type_id

        results = {}
        # start entity
        if first_iteration:
            # check if entity props are requested
            if crdb_query['e_props']:
                results = await self._get_entities_crdb(entity_ids, **entity_type_name_or_id)

        for relation_type_id in crdb_query['relations']:
            # get relation data
            raw_results = await self._get_relations_crdb(
                entity_ids,
                relation_type_id.split('_')[0] == 'ri',
                **entity_type_name_or_id,
                relation_type_id=relation_type_id.split('_')[1],
            )
            for entity_id, raw_result in raw_results.items():
                if entity_id not in results:
                    results[entity_id] = {}
                if 'relations' not in results[entity_id]:
                    results[entity_id]['relations'] = {}
                results[entity_id]['relations'][relation_type_id] = raw_result

            # gather what further information is required
            rel_entities = {}
            raw_rel_results_per_entity_type_id = {}
            # mapping so results (identified by entity_type_name, entity_id)
            # can be added in the right place (identified by relation_type_id, relation_id)
            mapping = {}
            if crdb_query['relations'][relation_type_id]['relations']:
                for entity_id, raw_relation_results in raw_results.items():
                    for relation_id, raw_result in raw_relation_results.items():
                        rel_entity_type_id = raw_result['entity_type_id']
                        rel_entity_id = raw_result['e_props']['id']

                        if rel_entity_type_id not in rel_entities:
                            rel_entities[rel_entity_type_id] = set()
                        rel_entities[rel_entity_type_id].add(rel_entity_id)

                        if relation_type_id not in mapping:
                            mapping[relation_type_id] = {}
                        mapping[relation_type_id][relation_id] = [
                            rel_entity_type_id,
                            rel_entity_id,
                        ]

                # recursively obtain further relation data
                for rel_entity_type_id, rel_entity_ids in rel_entities.items():
                    raw_rel_results_per_entity_type_id[rel_entity_type_id] = await self.get_entity_data(
                        list(rel_entity_ids),
                        crdb_query['relations'][relation_type_id],
                        False,
                        **entity_type_name_or_id,
                    )

                # add the additional relation data to the result
                for entity_id in results:
                    if 'relations' in results[entity_id] and relation_type_id in results[entity_id]['relations']:
                        for relation_id in results[entity_id]['relations'][relation_type_id]:
                            (rel_entity_type_id, rel_entity_id) = mapping[relation_type_id][relation_id]
                            if rel_entity_id in raw_rel_results_per_entity_type_id[rel_entity_type_id]:
                                results[entity_id]['relations'][relation_type_id][relation_id]['relations'] = \
                                    raw_rel_results_per_entity_type_id[rel_entity_type_id][rel_entity_id]['relations']

        return results
