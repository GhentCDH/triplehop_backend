import typing
from fastapi.exceptions import HTTPException

from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.mgmt.auth import allowed_entities_or_relations_and_properties
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.utils import RE_SOURCE_PROP_INDEX, dtu, first_cap, utd


class DataManager:
    def __init__(
        self,
        project_name: str,
        config_repo: ConfigRepository,
        data_repo: DataRepository,
        user: UserWithPermissions,
    ):
        self._project_name = project_name
        self._config_manager = ConfigManager(config_repo)
        self._data_repo = data_repo
        self._user = user
        self._entity_types_config = None
        self._relation_types_config = None
        self._project_id = None

    @staticmethod
    def valid_prop_value(prop_type: str, prop_value: typing.Any) -> bool:
        """Check if a property value is of the correct type."""
        if prop_type == 'String':
            return isinstance(prop_value, str)

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
                    raise HTTPException(status_code=403, detail="Forbidden")
            return

        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            entities_or_relations,
            'data',
            permission,
        )
        if type_name not in allowed:
            raise HTTPException(status_code=403, detail="Forbidden")

        allowed_props = allowed[type_name]
        for prop in props:
            if prop not in allowed_props:
                raise HTTPException(status_code=403, detail="Forbidden")

    async def _validate_input(
        self,
        entity_type_name: str,
        input: typing.Dict,
    ) -> None:
        # TODO: set _entity_types_config on init
        if self._entity_types_config is None:
            self._entity_types_config = await self._config_manager.get_entity_types_config(self._project_name)

        data_config = self._entity_types_config[entity_type_name]['config']['data']
        for prop_name, prop_value in input.items():
            etipm = await self._config_manager.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
            # Strip p_ from prop id
            prop_type = data_config[utd(etipm[prop_name][2:])]['type']
            if not self.__class__.valid_prop_value(prop_type, prop_value):
                raise HTTPException(status_code=422, detail="Invalid value")

    async def get_entities(
        self,
        entity_type_name: str,
        props: typing.List[str],
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        await self._check_permission('get', 'entities', entity_type_name, props)

        entity_type_id = await self._config_manager.get_entity_type_id_by_name(self._project_name, entity_type_name)

        db_results = await self._data_repo.get_entities(entity_type_id, entity_ids)

        if len(db_results) == 0:
            return []

        etpm = await self._config_manager.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {
            entity_id: {etpm[k]: v for k, v in db_result['e_props'].items() if k in etpm}
            for entity_id, db_result in db_results.items()
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
                db_result = await self._data_repo.put_entity(entity_type_id, entity_id, db_input, connection)

        if db_result is None:
            return None

        etpm = await self._config_manager.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {etpm[k]: v for k, v in db_result.items() if k in etpm}

        # # Update elasticsearch

    async def get_relations(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
        relation_type_name: str,
        inverse: bool = False,
    ) -> typing.Dict:
        # TODO: check permission for requested properties
        await self._check_permission('get', 'relations', relation_type_name, {})

        entity_type_id = await self._config_manager.get_entity_type_id_by_name(self._project_name, entity_type_name)
        relation_type_id = await self._config_manager.get_relation_type_id_by_name(self._project_name, relation_type_name)

        db_results = await self._data_repo.get_relations(entity_type_id, entity_ids, relation_type_id, inverse)

        results = {}
        rtpm = await self._config_manager.get_relation_type_property_mapping(self._project_name, relation_type_name)
        etpma = await self._config_manager.get_entity_type_property_mapping(self._project_name, '__all__')
        rtpma = await self._config_manager.get_relation_type_property_mapping(self._project_name, '__all__')
        etd = {}
        for entity_id, db_result in db_results.items():
            results[entity_id] = []
            for db_relation_result in db_result.values():
                etid = db_relation_result['entity_type_id']
                if etid not in etd:
                    etn = await self._config_manager.get_entity_type_name_by_id(self._project_name, etid)
                    etd[etid] = {
                        'etn': etn,
                        'etpm': await self._config_manager.get_entity_type_property_mapping(self._project_name, etn)
                    }
                etpm = etd[etid]['etpm']

                result = {
                    rtpm[k]: v
                    for k, v in db_relation_result['r_props'].items()
                    if k in rtpm
                }
                result['entity'] = {
                    etpm[k]: v
                    for k, v in db_relation_result['e_props'].items()
                    if k in etpm
                }
                result['entity']['__typename'] = first_cap(etd[etid]['etn'])
                result['_source_'] = []
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

                # Source information
                srtpm = await self._config_manager.get_relation_type_property_mapping(self._project_name, '_source_')
                for source in db_relation_result['sources']:
                    setid = source['entity_type_id']
                    if setid not in etd:
                        etn = await self._config_manager.get_entity_type_name_by_id(self._project_name, setid)
                        etd[setid] = {
                            'etn': etn,
                            'etpm': await self._config_manager.get_entity_type_property_mapping(self._project_name, etn)
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
