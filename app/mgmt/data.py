import typing
from fastapi.exceptions import HTTPException

from app.auth.permission import get_permission_entities_and_properties
from app.db.config import ConfigRepository
from app.db.data import DataRepository
from app.models.auth import UserWithPermissions
from app.utils import utd


class DataManager:
    def __init__(
        self,
        project_name: str,
        config_repo: ConfigRepository,
        data_repo: DataRepository,
        user: UserWithPermissions,
    ):
        self._project_name = project_name
        self._config_repo = config_repo
        self._data_repo = data_repo
        self._user = user
        self._entity_types_config = None

    @staticmethod
    def valid_prop_value(prop_type: str, prop_value: typing.Any) -> bool:
        """Check if a property value is of the correct type."""
        if prop_type == 'String':
            return isinstance(prop_value, str)

    async def _check_permission(
        self,
        permission: str,
        entity_type_name: str,
        input: typing.Dict,
    ) -> None:
        # TODO: set _entity_types_config on init
        if self._entity_types_config is None:
            self._entity_types_config = await self._config_repo.get_entity_types_config(self._project_name)

        allowed = get_permission_entities_and_properties(
            self._user,
            self._project_name,
            self._entity_types_config,
            permission,
        )
        if entity_type_name not in allowed:
            raise HTTPException(status_code=403, detail="Forbidden")

        allowed_props = allowed[entity_type_name]
        for prop_name in input:
            if prop_name not in allowed_props:
                raise HTTPException(status_code=403, detail="Forbidden")

    async def _validate_input(
        self,
        entity_type_name: str,
        input: typing.Dict,
    ) -> None:
        # TODO: set _entity_types_config on init
        if self._entity_types_config is None:
            self._entity_types_config = await self._config_repo.get_entity_types_config(self._project_name)

        data_config = self._entity_types_config[entity_type_name]['config']['data']
        for prop_name, prop_value in input.items():
            etipm = await self._config_repo.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
            # Strip p_ from prop id
            prop_type = data_config[utd(etipm[prop_name][2:])]['type']
            if not self.__class__.valid_prop_value(prop_type, prop_value):
                raise HTTPException(status_code=422, detail="Invalid value")

    async def get_entities(
        self,
        entity_type_name: str,
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        # TODO: check permission for requested properties
        await self._check_permission('get', entity_type_name, {})

        entity_type_id = await self._config_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)

        db_results = await self._data_repo.get_entities(entity_type_id, entity_ids)

        if len(db_results) == 0:
            return []

        etpm = await self._config_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {
            entity_id: {etpm[k]: v for k, v in raw_result['e_props'].items() if k in etpm}
            for entity_id, raw_result in db_results.items()
        }

    async def put_entity(
        self,
        entity_type_name: str,
        entity_id: int,
        input: typing.Dict,
    ):
        await self._check_permission('put', entity_type_name, input)

        await self._validate_input(entity_type_name, input)

        # Insert in database
        entity_type_id = await self._config_repo.get_entity_type_id_by_name(self._project_name, entity_type_name)
        etipm = await self._config_repo.get_entity_type_i_property_mapping(self._project_name, entity_type_name)
        db_input = {
            etipm[k]: v
            for k, v in input.items()
        }

        async with self._data_repo.connection() as connection:
            async with connection.transaction():
                db_result = await self._data_repo.put_entity(entity_type_id, entity_id, db_input, connection)

        print(db_result)

        if db_result is None:
            return None

        etpm = await self._config_repo.get_entity_type_property_mapping(self._project_name, entity_type_name)

        return {etpm[k]: v for k, v in db_result.items() if k in etpm}

        # # Update elasticsearch
