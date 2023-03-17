import json
import re
import typing

import asyncpg
import dictdiffer
import edtf
import fastapi
import starlette

from app.db.core import get_repository_from_request
from app.db.data import DataRepository
from app.es.base import BaseElasticsearch
from app.es.core import get_es_from_request
from app.mgmt.auth import allowed_entities_or_relations_and_properties
from app.mgmt.config import ConfigManager
from app.mgmt.revision import RevisionManager
from app.models.auth import UserWithPermissions
from app.utils import (
    BATCH_SIZE,
    RE_FIELD_CONVERSION,
    RE_SOURCE_PROP_INDEX,
    dtu,
    first_cap,
    utd,
)


class DataManager:
    def __init__(
        self,
        request: starlette.requests.Request,
        user: UserWithPermissions = None,
    ):
        self._request = request
        self._project_name = request.path_params["project_name"]
        self._config_manager = ConfigManager(request, user)
        self._revision_manager = RevisionManager(request, user)
        self._data_repo = get_repository_from_request(request, DataRepository)
        self._es = get_es_from_request(request, BaseElasticsearch)
        self._user = user
        self._entity_types_config = None
        self._relation_types_config = None
        self._project_id = None

    async def _get_project_id(self):
        if self._project_id is None:
            self._project_id = await self._config_manager.get_project_id_by_name(
                self._project_name
            )
        return self._project_id

    async def _get_entity_types_config(self):
        if self._entity_types_config is None:
            self._entity_types_config = (
                await self._config_manager.get_entity_types_config(self._project_name)
            )
        return self._entity_types_config

    async def _get_relation_types_config(self):
        if self._relation_types_config is None:
            self._relation_types_config = (
                await self._config_manager.get_relation_types_config(self._project_name)
            )
        return self._relation_types_config

    @staticmethod
    def raise_validation_exception(
        validator: typing.Optional[typing.Dict[str, str]] = None,
        type_error_message: typing.Optional[str] = None,
    ):
        if validator is not None and "error_message" in validator:
            raise Exception(validator["error_message"])
        if type_error_message is not None:
            raise Exception(type_error_message)
        raise Exception("The value provided for this field is invalid.")

    @staticmethod
    def validate_prop_value_validators(
        prop_value: typing.Any, validators: typing.Optional[typing.List] = None
    ):
        if validators is None:
            return
        for validator in validators:
            if validator["type"] == "required":
                if prop_value is None or (
                    isinstance(prop_value, str) and prop_value == ""
                ):
                    DataManager.raise_validation_exception(
                        validator, "This field is required."
                    )
                continue

            if validator["type"] == "edtf":
                if prop_value == "":
                    continue
                try:
                    # edtf module needs to be updated to the newest revision
                    # https://github.com/ixc/python-edtf/issues/24
                    old_edtf_text = prop_value.replace("X", "u")
                    edtf_date = edtf.parse_edtf(old_edtf_text)
                except edtf.parser.edtf_exceptions.EDTFParseException:
                    DataManager.raise_validation_exception(validator)
                continue

            if validator["type"] == "edtf_year":
                if prop_value == "":
                    continue
                try:
                    # edtf module needs to be updated to the newest revision
                    # https://github.com/ixc/python-edtf/issues/24
                    old_edtf_text = prop_value.replace("X", "u")
                    edtf_date = edtf.parse_edtf(old_edtf_text)
                except edtf.parser.edtf_exceptions.EDTFParseException:
                    DataManager.raise_validation_exception(validator)
                # Check if it is a year or has more precision
                if len(str(edtf_date).split("-")) > 1:
                    DataManager.raise_validation_exception(validator)
                continue

            if validator["type"] == "regex":
                if not re.match(validator["regex"], prop_value):
                    DataManager.raise_validation_exception(validator)
                continue

            if validator["type"] == "geometry_point":
                if prop_value == "":
                    continue
                if (
                    "type" not in prop_value
                    # TODO: other geometries
                    or prop_value["type"] not in ["Point"]
                    or "coordinates" not in prop_value
                ):
                    DataManager.raise_validation_exception(validator)
                coordinates = prop_value["coordinates"]
                if (
                    not isinstance(coordinates, list)
                    # TODO: other geometries
                    or not len(coordinates) == 2
                    or not isinstance(coordinates[0], (int, float))
                    or not isinstance(coordinates[1], (int, float))
                    or not (-180 < coordinates[0] < 180)
                    or not (-90 < coordinates[1] < 90)
                ):
                    DataManager.raise_validation_exception(validator)
                continue

            if validator["type"] == "list":
                if prop_value == "":
                    continue
                if prop_value not in validator["allowed_values"]:
                    DataManager.raise_validation_exception(validator)
                continue

            if validator["type"] == "table":
                if prop_value == "":
                    continue
                try:
                    table = json.loads(prop_value)
                except json.decoder.JSONDecodeError:
                    DataManager.raise_validation_exception(validator)
                # Table must be a list; at least a header row must be present
                if not isinstance(table, list) or len(table) < 2:
                    DataManager.raise_validation_exception(validator)
                row_length = len(table[0])
                for row in table:
                    # All rows must be lists and must be of equal length
                    if not isinstance(row, list) or len(row) != row_length:
                        DataManager.raise_validation_exception(validator)
                    for cell in row:
                        # All cells in a row must be strings
                        if not isinstance(cell, str):
                            DataManager.raise_validation_exception(validator)
                continue

            raise Exception("Validator type not yet implemented")

    @staticmethod
    def validate_prop_value(
        prop_value: typing.Any,
        prop_type: str,
        validators: typing.Optional[typing.List] = None,
    ) -> None:
        """Check if a property value is of the correct type."""
        if prop_type == "String":
            if not isinstance(prop_value, str):
                DataManager.raise_validation_exception()
            DataManager.validate_prop_value_validators(prop_value, validators)
            return

        if prop_type == "[String]":
            if not isinstance(prop_value, list):
                DataManager.raise_validation_exception()
            for prop_val in prop_value:
                if not isinstance(prop_val, str):
                    DataManager.raise_validation_exception()
                DataManager.validate_prop_value_validators(prop_val, validators)
            return

        if prop_type == "Geometry":
            if not isinstance(prop_value, dict) and not (
                isinstance(prop_value, str) and prop_value == ""
            ):
                DataManager.raise_validation_exception()
            DataManager.validate_prop_value_validators(prop_value, validators)
            return

        if prop_type == "Int":
            if not isinstance(prop_value, int) and not prop_value is None:
                DataManager.raise_validation_exception()
            DataManager.validate_prop_value_validators(prop_value, validators)
            return

        raise Exception("Prop type not yet implemented")

    @staticmethod
    def _require_entity_type_name_or_entity_type_id(
        entity_type_name, entity_type_id
    ) -> None:
        if entity_type_name is not None and entity_type_id is not None:
            raise Exception(
                "Only one keyword argument is required: entity_type_name or entity_type_id"
            )
        if entity_type_name is None and entity_type_id is None:
            raise Exception(
                "One keyword argument is required: entity_type_name or entity_type_id"
            )

    @staticmethod
    def _require_relation_type_name_or_entity_type_id(
        relation_type_name, relation_type_id
    ) -> None:
        if relation_type_name is not None and relation_type_id is not None:
            raise Exception(
                "Only one keyword argument is required: relation_type_name or relation_type_id"
            )
        if relation_type_name is None and relation_type_id is None:
            raise Exception(
                "One keyword argument is required: relation_type_name or relation_type_id"
            )

    async def _check_permission(
        self,
        permission: str,
        entities_or_relations: str,
        type_name: str,
        props: typing.List = None,
    ) -> None:
        # Special case: source
        # TODO: check source permissions using config
        if (
            entities_or_relations == "relations"
            and type_name == "_source_"
            and permission == "get"
        ):
            for prop in props:
                if prop not in ["id", "properties", "source_props"]:
                    raise fastapi.exceptions.HTTPException(
                        status_code=403, detail="Forbidden"
                    )
            return

        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            entities_or_relations,
            "data",
            permission,
        )
        if type_name not in allowed:
            raise fastapi.exceptions.HTTPException(status_code=403, detail="Forbidden")

        if props is None:
            return

        allowed_props = allowed[type_name]
        for prop in props:
            if prop not in allowed_props:
                raise fastapi.exceptions.HTTPException(
                    status_code=403, detail="Forbidden"
                )

    async def _validate_input(
        self,
        entities_or_relations: str,
        type_name: str,
        input: typing.Dict,
    ) -> None:
        if not isinstance(input, dict):
            DataManager.raise_validation_exception()

        if entities_or_relations == "entities":
            data_config = (await self._get_entity_types_config())[type_name]["config"][
                "data"
            ]["fields"]
            ipm = await self._config_manager.get_entity_type_i_property_mapping(
                self._project_name, type_name
            )
        else:
            data_config = (await self._get_relation_types_config())[type_name][
                "config"
            ]["data"]["fields"]
            ipm = await self._config_manager.get_relation_type_i_property_mapping(
                self._project_name, type_name
            )

        for prop_name, prop_value in input.items():
            # Strip p_ from prop id
            prop_config = data_config[utd(ipm[prop_name][2:])]
            prop_validators = prop_config.get("validators")
            self.__class__.validate_prop_value(
                prop_value, prop_config["type"], prop_validators
            )

    async def _get_entities_triplehop(
        self,
        entity_ids: typing.List[int],
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        self.__class__._require_entity_type_name_or_entity_type_id(
            entity_type_name, entity_type_id
        )

        if entity_type_id is None:
            entity_type_id = await self._config_manager.get_entity_type_id_by_name(
                self._project_name,
                entity_type_name,
                connection=connection,
            )

        records = await self._data_repo.get_entities(
            await self._get_project_id(),
            entity_type_id,
            entity_ids,
            connection=connection,
        )

        results = {
            record["id"]: {"e_props": json.loads(record["properties"])}
            for record in records
        }
        return results

    async def get_entities(
        self,
        entity_type_name: str,
        props: typing.List[str],
        entity_ids: typing.List[int],
    ) -> typing.Dict:
        await self._check_permission("get", "entities", entity_type_name, props)

        # TODO: only return requested props
        # -> change dataloader in graphql/data
        triplehop_results = await self._get_entities_triplehop(
            entity_ids, entity_type_name=entity_type_name
        )
        if len(triplehop_results) == 0:
            return {}

        etpm = await self._config_manager.get_entity_type_property_mapping(
            self._project_name, entity_type_name
        )

        return {
            entity_id: {
                etpm[k]: v for k, v in triplehop_result["e_props"].items() if k in etpm
            }
            for entity_id, triplehop_result in triplehop_results.items()
        }

    async def _sanitize_entity_input(
        self,
        entity_type_name: str,
        input: typing.Dict,
    ) -> typing.Dict:
        entity_input = {}
        if "entity" in input:
            entity_input = json.loads(input.pop("entity"))
            await self._check_permission(
                "post", "entities", entity_type_name, entity_input.keys()
            )

            await self._validate_input("entities", entity_type_name, entity_input)
        return entity_input

    async def _sanitize_entity_relations_input(
        self,
        input: typing.Dict,
    ) -> typing.Dict:
        # TODO: check if a relation is not put and deleted in a single mutation
        relations_data = {}
        if input:
            for relation_type_name, relation_data_as_json in input.items():
                # strip r_ or ri_ and _s
                clean_relation_type_name = "_".join(relation_type_name.split("_")[1:-1])
                relation_data = json.loads(relation_data_as_json)
                if not isinstance(relation_data, dict):
                    DataManager.raise_validation_exception()

                relations_data[relation_type_name] = {}

                for operation, operation_data in relation_data.items():
                    await self._check_permission(
                        operation, "relations", clean_relation_type_name
                    )

                    if operation == "post":
                        if not isinstance(operation_data, list):
                            DataManager.raise_validation_exception()

                        relations_data[relation_type_name]["post"] = []
                        for relation_input_wrapper in operation_data:
                            if not isinstance(relation_input_wrapper, dict):
                                DataManager.raise_validation_exception()
                            if not "entity" in relation_input_wrapper:
                                DataManager.raise_validation_exception()
                            if not isinstance(relation_input_wrapper["entity"], dict):
                                DataManager.raise_validation_exception()
                            if not "entityTypeName" in relation_input_wrapper["entity"]:
                                DataManager.raise_validation_exception()
                            if (
                                relation_input_wrapper["entity"]["entityTypeName"]
                                not in await self._get_entity_types_config()
                            ):
                                DataManager.raise_validation_exception()
                            if not "id" in relation_input_wrapper["entity"]:
                                DataManager.raise_validation_exception()
                            if not relation_input_wrapper["entity"]["id"].isdigit():
                                DataManager.raise_validation_exception()
                            if not "relation" in relation_input_wrapper:
                                DataManager.raise_validation_exception()
                            await self._validate_input(
                                "relations",
                                clean_relation_type_name,
                                relation_input_wrapper["relation"],
                            )
                            relations_data[relation_type_name]["post"].append(
                                relation_input_wrapper
                            )
                        continue

                    if operation == "put":
                        relations_data[relation_type_name]["put"] = {}
                        if not isinstance(operation_data, dict):
                            DataManager.raise_validation_exception()
                        for (
                            relation_id,
                            relation_input_wrapper,
                        ) in operation_data.items():
                            if not isinstance(relation_id, str):
                                DataManager.raise_validation_exception()
                            if not relation_id.isdigit():
                                DataManager.raise_validation_exception()
                            if not isinstance(relation_input_wrapper, dict):
                                DataManager.raise_validation_exception()
                            if not "relation" in relation_input_wrapper:
                                DataManager.raise_validation_exception()
                            await self._validate_input(
                                "relations",
                                clean_relation_type_name,
                                relation_input_wrapper["relation"],
                            )
                            relations_data[relation_type_name]["put"][
                                int(relation_id)
                            ] = relation_input_wrapper["relation"]
                        continue

                    if operation == "delete":
                        if not isinstance(operation_data, list):
                            DataManager.raise_validation_exception()

                        relations_data[relation_type_name]["delete"] = []
                        for relation_id in operation_data:
                            if not isinstance(relation_id, str):
                                DataManager.raise_validation_exception()
                            if not relation_id.isdigit():
                                DataManager.raise_validation_exception()

                            # Checking if the relation exists happens before actually deleting the relation

                            relations_data[relation_type_name]["delete"].append(
                                int(relation_id)
                            )
        return relations_data

    async def _transform_to_db_inputs(
        self,
        entity_type_name: str,
        entity_input: typing.Dict,
        relations_data: typing.Dict,
    ) -> typing.Dict:
        db_inputs = {}
        if entity_input:
            etipm = await self._config_manager.get_entity_type_i_property_mapping(
                self._project_name, entity_type_name
            )
            db_inputs["entity"] = {etipm[k]: v for k, v in entity_input.items()}
        for relation_type_name, relation_type_data in relations_data.items():
            # strip r_ or ri_ and _s
            clean_relation_type_name = "_".join(relation_type_name.split("_")[1:-1])
            relation_type_id = await self._config_manager.get_relation_type_id_by_name(
                self._project_name,
                clean_relation_type_name,
            )
            db_inputs[relation_type_id] = {
                "name": relation_type_name,
            }
            if "post" in relation_type_data or "put" in relation_type_data:
                rtipm = await self._config_manager.get_relation_type_i_property_mapping(
                    self._project_name, clean_relation_type_name
                )
            if "post" in relation_type_data:
                db_inputs[relation_type_id]["post"] = []
                for relation_input in relation_type_data["post"]:
                    db_inputs[relation_type_id]["post"].append(
                        {
                            "entity": relation_input["entity"],
                            "relation": {
                                rtipm[k]: v
                                for k, v in relation_input["relation"].items()
                            },
                        }
                    )
            if "put" in relation_type_data:
                db_inputs[relation_type_id]["put"] = {}
                for relation_id, relation_input in relation_type_data["put"].items():
                    db_inputs[relation_type_id]["put"][relation_id] = {
                        rtipm[k]: v for k, v in relation_input.items()
                    }
            if "delete" in relation_type_data:
                db_inputs[relation_type_id]["delete"] = relation_type_data["delete"]
        return db_inputs

    async def _db_and_es_for_relations(
        self,
        entity_type_name: str,
        entity_id: int,
        db_inputs: typing.Dict,
        revisions: typing.Dict,  # will be updated
        es_query: typing.Dict,  # will be updated
        connection: asyncpg.Connection,
        new_id: int = None,
    ):
        for relation_type_id, relation_data in db_inputs.items():
            relation_type_name = relation_data["name"]
            # strip r_ or ri_ and _s
            clean_relation_type_name = "_".join(relation_type_name.split("_")[1:-1])
            if "post" in relation_data:
                for db_input in relation_data["post"]:
                    entity_type_id = (
                        await self._config_manager.get_entity_type_id_by_name(
                            self._project_name,
                            entity_type_name,
                        )
                    )
                    other_entity_type_id = (
                        await self._config_manager.get_entity_type_id_by_name(
                            self._project_name,
                            db_input["entity"]["entityTypeName"],
                        )
                    )
                    other_entity_id = int(db_input["entity"]["id"])
                    # swap start and end node if relation is reverse
                    if relation_type_name.split("_")[0] == "ri":
                        entity_type_id, other_entity_type_id = (
                            other_entity_type_id,
                            entity_type_id,
                        )
                        entity_id, other_entity_id = other_entity_id, entity_id
                    raw_data = await self._data_repo.post_relation(
                        await self._get_project_id(),
                        relation_type_id,
                        entity_type_id,
                        entity_id,
                        other_entity_type_id,
                        other_entity_id,
                        db_input["relation"],
                        connection,
                    )
                    if raw_data is None:
                        raise fastapi.exceptions.HTTPException(
                            status_code=404, detail="Relation not found"
                        )
                    # strip off ::edge
                    new_relation_props = json.loads(raw_data["e"][:-6])["properties"]
                    relation_id = new_relation_props["id"]
                    # strip of ::vertex
                    start_entity_data = json.loads(raw_data["d"][:-8])
                    start_entity_type_name = (
                        await self._config_manager.get_entity_type_name_by_id(
                            self._project_name,
                            utd(start_entity_data["label"][2:]),
                        )
                    )
                    start_entity_id = start_entity_data["properties"]["id"]
                    # strip of ::vertex
                    end_entity_data = json.loads(raw_data["r"][:-8])
                    end_entity_type_name = (
                        await self._config_manager.get_entity_type_name_by_id(
                            self._project_name,
                            utd(end_entity_data["label"][2:]),
                        )
                    )
                    end_entity_id = end_entity_data["properties"]["id"]

                    if "relations" not in revisions:
                        revisions["relations"] = {}
                    if clean_relation_type_name not in revisions["relations"]:
                        revisions["relations"][clean_relation_type_name] = {}
                    revisions["relations"][clean_relation_type_name][relation_id] = [
                        None,
                        new_relation_props,
                        start_entity_type_name,
                        start_entity_id,
                        end_entity_type_name,
                        end_entity_id,
                    ]

                    await self.update_es_query(
                        es_query,
                        "relations",
                        clean_relation_type_name,
                        relation_id,
                        dictdiffer.diff({}, new_relation_props),
                        connection,
                        new_id=new_id,
                    )

            if "put" in relation_data:
                for relation_id, db_input in relation_data["put"].items():
                    old_raw_relation = await self._data_repo.get_relation(
                        await self._get_project_id(),
                        relation_type_id,
                        relation_id,
                        connection,
                    )
                    if (
                        old_raw_relation is None
                        or old_raw_relation["id"] != relation_id
                    ):
                        raise fastapi.exceptions.HTTPException(
                            status_code=404, detail="Relation not found"
                        )
                    old_relation_props = old_raw_relation["properties"]

                    # check if there are any changes
                    changes = False
                    for k, v in db_input.items():
                        if k not in old_relation_props:
                            changes = True
                            break
                        if old_relation_props[k] != v:
                            changes = True
                            break
                    if changes:
                        # TODO: delete relation sources before removing properties linked to them
                        raw_data = await self._data_repo.put_relation(
                            await self._get_project_id(),
                            relation_type_id,
                            relation_id,
                            db_input,
                            connection,
                        )
                        if raw_data is None:
                            raise fastapi.exceptions.HTTPException(
                                status_code=404, detail="Relation not found"
                            )
                        # strip off ::edge
                        new_relation_props = json.loads(raw_data["e"][:-6])[
                            "properties"
                        ]
                        # strip of ::vertex
                        start_entity_data = json.loads(raw_data["d"][:-8])
                        start_entity_type_name = (
                            await self._config_manager.get_entity_type_name_by_id(
                                self._project_name,
                                utd(start_entity_data["label"][2:]),
                            )
                        )
                        start_entity_id = start_entity_data["properties"]["id"]
                        # strip of ::vertex
                        end_entity_data = json.loads(raw_data["r"][:-8])
                        end_entity_type_name = (
                            await self._config_manager.get_entity_type_name_by_id(
                                self._project_name,
                                utd(end_entity_data["label"][2:]),
                            )
                        )
                        end_entity_id = end_entity_data["properties"]["id"]

                        if "relations" not in revisions:
                            revisions["relations"] = {}
                        if clean_relation_type_name not in revisions["relations"]:
                            revisions["relations"][clean_relation_type_name] = {}
                        revisions["relations"][clean_relation_type_name][
                            relation_id
                        ] = [
                            old_relation_props,
                            new_relation_props,
                            start_entity_type_name,
                            start_entity_id,
                            end_entity_type_name,
                            end_entity_id,
                        ]

                        await self.update_es_query(
                            es_query,
                            "relations",
                            clean_relation_type_name,
                            relation_id,
                            dictdiffer.diff(old_relation_props, new_relation_props),
                            connection,
                            new_id=new_id,
                        )

            if "delete" in relation_data:
                for relation_id in relation_data["delete"]:
                    old_raw_relation = await self._data_repo.get_relation(
                        await self._get_project_id(),
                        relation_type_id,
                        relation_id,
                        connection,
                    )
                    if (
                        old_raw_relation is None
                        or old_raw_relation["id"] != relation_id
                    ):
                        raise fastapi.exceptions.HTTPException(
                            status_code=404, detail="Relation not found"
                        )
                    old_relation_props = old_raw_relation["properties"]

                    # Delete relation sources before deleting the relations themselves
                    old_raw_relation_sources = (
                        await self._data_repo.delete_relation_sources(
                            await self._get_project_id(),
                            relation_type_id,
                            relation_id,
                            connection,
                        )
                    )
                    for old_raw_relation_source in old_raw_relation_sources:
                        if "relations" not in revisions:
                            revisions["relations"] = {}
                        if "_source_" not in revisions["relations"]:
                            revisions["relations"]["_source_"] = {}
                        # strip off ::edge
                        source_relation_properties = json.loads(
                            old_raw_relation_source["e"][:-6]
                        )["properties"]
                        # strip off ::vertex
                        source = json.loads(old_raw_relation_source["s"][:-8])
                        revisions["relations"]["_source_"][
                            source_relation_properties["id"]
                        ] = [
                            source_relation_properties,
                            None,
                            clean_relation_type_name,
                            relation_id,
                            # strip n_
                            await self._config_manager.get_entity_type_name_by_id(
                                self._project_name,
                                utd(source["label"][2:]),
                            ),
                            source["properties"]["id"],
                        ]

                    # Generate Elasticsearch update query before deleting the relations
                    await self.update_es_query(
                        es_query,
                        "relations",
                        clean_relation_type_name,
                        relation_id,
                        dictdiffer.diff(old_relation_props, {}),
                        connection,
                        new_id=new_id,
                    )

                    raw_data = await self._data_repo.delete_relation(
                        await self._get_project_id(),
                        relation_type_id,
                        relation_id,
                        connection,
                    )
                    if raw_data is None:
                        raise fastapi.exceptions.HTTPException(
                            status_code=404, detail="Relation not found"
                        )
                    # strip of ::vertex
                    start_entity_data = json.loads(raw_data["d"][:-8])
                    start_entity_type_name = (
                        await self._config_manager.get_entity_type_name_by_id(
                            self._project_name,
                            utd(start_entity_data["label"][2:]),
                        )
                    )
                    start_entity_id = start_entity_data["properties"]["id"]
                    # strip of ::vertex
                    end_entity_data = json.loads(raw_data["r"][:-8])
                    end_entity_type_name = (
                        await self._config_manager.get_entity_type_name_by_id(
                            self._project_name,
                            utd(end_entity_data["label"][2:]),
                        )
                    )
                    end_entity_id = end_entity_data["properties"]["id"]

                    if "relations" not in revisions:
                        revisions["relations"] = {}
                    if clean_relation_type_name not in revisions["relations"]:
                        revisions["relations"][clean_relation_type_name] = {}
                    revisions["relations"][clean_relation_type_name][relation_id] = [
                        old_relation_props,
                        None,
                        start_entity_type_name,
                        start_entity_id,
                        end_entity_type_name,
                        end_entity_id,
                    ]

    async def post_entity(
        self,
        entity_type_name: str,
        input: typing.Dict,
        props: typing.List[str],
    ):
        # TODO: only return requested props
        entity_input = await self._sanitize_entity_input(entity_type_name, input)
        relations_data = await self._sanitize_entity_relations_input(input)
        db_inputs = await self._transform_to_db_inputs(
            entity_type_name,
            entity_input,
            relations_data,
        )
        entity_type_id = await self._config_manager.get_entity_type_id_by_name(
            self._project_name, entity_type_name
        )

        # Insert in database and update Elasticsearch
        es_query = {}
        revisions = {}
        async with self._data_repo.connection() as connection:
            async with connection.transaction():
                # Always create a new entity
                db_input = {}
                if "entity" in db_inputs:
                    db_input = db_inputs.pop("entity")

                new_raw_entity = await self._data_repo.post_entity(
                    await self._get_project_id(),
                    entity_type_id,
                    db_input,
                    connection,
                )
                if new_raw_entity is None:
                    raise fastapi.exceptions.HTTPException(
                        status_code=404, detail="Entity not found"
                    )
                # strip off ::vertex
                new_entity = json.loads(new_raw_entity[:-8])["properties"]
                entity_id = new_entity["id"]

                revisions["entities"] = {
                    entity_type_name: {
                        entity_id: [
                            None,
                            new_entity,
                        ]
                    }
                }

                await self.update_es_query(
                    es_query,
                    "entities",
                    entity_type_name,
                    entity_id,
                    dictdiffer.diff({}, new_entity),
                    connection,
                    new_id=entity_id,
                )

                await self._db_and_es_for_relations(
                    entity_type_name,
                    entity_id,
                    db_inputs,
                    revisions,
                    es_query,
                    connection,
                    entity_id,
                )

                await self._revision_manager.post_revision(
                    revisions,
                    connection,
                )

                await self.update_es(es_query, connection)

        return (
            await self.get_entities(
                entity_type_name,
                props,
                [entity_id],
            )
        )[entity_id]

    async def put_entity(
        self,
        entity_type_name: str,
        entity_id: int,
        input: typing.Dict,
        props: typing.List[str],
    ):
        # TODO: only return requested props
        # TODO: implement edit and read locks to prevent elasticsearch from using outdated information

        # Checking if the entity exists happens before actually updating the entity

        entity_input = await self._sanitize_entity_input(entity_type_name, input)
        relations_data = await self._sanitize_entity_relations_input(input)
        db_inputs = await self._transform_to_db_inputs(
            entity_type_name,
            entity_input,
            relations_data,
        )
        entity_type_id = await self._config_manager.get_entity_type_id_by_name(
            self._project_name, entity_type_name
        )

        # Insert in database and update Elasticsearch
        es_query = {}
        revisions = {}
        async with self._data_repo.connection() as connection:
            async with connection.transaction():
                if "entity" in db_inputs:
                    db_input = db_inputs.pop("entity")
                    old_raw_entities = await self._data_repo.get_entities(
                        await self._get_project_id(),
                        entity_type_id,
                        [entity_id],
                        connection,
                    )
                    if (
                        len(old_raw_entities) != 1
                        or old_raw_entities[0]["id"] != entity_id
                    ):
                        raise fastapi.exceptions.HTTPException(
                            status_code=404, detail="Entity not found"
                        )
                    old_entity = json.loads(old_raw_entities[0]["properties"])

                    # check if there are any changes
                    changes = False
                    for k, v in db_input.items():
                        if k not in old_entity:
                            changes = True
                            break
                        if old_entity[k] != v:
                            changes = True
                            break
                    if changes:
                        new_raw_entity = await self._data_repo.put_entity(
                            await self._get_project_id(),
                            entity_type_id,
                            entity_id,
                            db_input,
                            connection,
                        )
                        if new_raw_entity is None:
                            raise fastapi.exceptions.HTTPException(
                                status_code=404, detail="Entity not found"
                            )
                        # strip off ::vertex
                        new_entity = json.loads(new_raw_entity["n"][:-8])["properties"]

                        revisions["entities"] = {
                            entity_type_name: {
                                entity_id: [
                                    old_entity,
                                    new_entity,
                                ]
                            }
                        }

                        await self.update_es_query(
                            es_query,
                            "entities",
                            entity_type_name,
                            entity_id,
                            dictdiffer.diff(old_entity, new_entity),
                            connection,
                        )
                await self._db_and_es_for_relations(
                    entity_type_name,
                    entity_id,
                    db_inputs,
                    revisions,
                    es_query,
                    connection,
                )

                await self._revision_manager.post_revision(
                    revisions,
                    connection,
                )

                await self.update_es(es_query, connection)

        return (
            await self.get_entities(
                entity_type_name,
                props,
                [entity_id],
            )
        )[entity_id]

    async def delete_entity(
        self,
        entity_type_name: str,
        entity_id: int,
    ):
        # TODO: implement edit and read locks to prevent elasticsearch from using outdated information
        # TODO: delete entity and relations in one query
        #   when optional match + detach delete works
        #   and additional indices are no longer required
        #   relation sources still need to be deleted separately

        # Checking if the entity exists happens before actually deleting the entity

        entity_type_id = await self._config_manager.get_entity_type_id_by_name(
            self._project_name, entity_type_name
        )

        # Insert in database and update Elasticsearch
        es_query = {}
        revisions = {}
        async with self._data_repo.connection() as connection:
            async with connection.transaction():
                old_raw_entities = await self._data_repo.get_entities(
                    await self._get_project_id(),
                    entity_type_id,
                    [entity_id],
                    connection,
                )
                if len(old_raw_entities) != 1 or old_raw_entities[0]["id"] != entity_id:
                    raise fastapi.exceptions.HTTPException(
                        status_code=404, detail="Entity not found"
                    )
                old_entity = json.loads(old_raw_entities[0]["properties"])

                # Delete relations before entity is deleted
                old_raw_relations = await self._data_repo.get_all_entity_relations(
                    await self._get_project_id(), entity_type_id, entity_id, connection
                )
                grouped_relation_ids = {}
                relations = {}
                for old_raw_relation in old_raw_relations:
                    relation_graph_id = old_raw_relation["id"]
                    relation_type_id = await self._data_repo.get_type_id_from_graph_id(
                        await self._get_project_id(),
                        relation_graph_id,
                        connection,
                    )
                    if relation_type_id not in grouped_relation_ids:
                        grouped_relation_ids[relation_type_id] = {
                            "nids": [],
                            "ids": [],
                        }
                    grouped_relation_ids[relation_type_id]["nids"].append(
                        old_raw_relation["id"]
                    )
                    properties = json.loads(old_raw_relation["properties"])
                    grouped_relation_ids[relation_type_id]["ids"].append(
                        properties["id"]
                    )
                    relations[relation_graph_id] = {
                        "relation_type_id": relation_type_id,
                        "properties": properties,
                        "start_id": old_raw_relation["start_id"],
                        "start_properties": json.loads(
                            old_raw_relation["start_properties"]
                        ),
                        "end_id": old_raw_relation["end_id"],
                        "end_properties": json.loads(
                            old_raw_relation["end_properties"]
                        ),
                    }

                # Generate Elasticsearch update query before deleting the relations
                # Add to revisions as well
                for relation_type_id, ids in grouped_relation_ids.items():
                    relation_type_name = (
                        await self._config_manager.get_relation_type_name_by_id(
                            self._project_name,
                            relation_type_id,
                            connection,
                        )
                    )
                    for nid, id in zip(ids["nids"], ids["ids"]):
                        await self.update_es_query(
                            es_query,
                            "relations",
                            relation_type_name,
                            id,
                            dictdiffer.diff(relations[nid]["properties"], {}),
                            connection,
                            old_id=entity_id,
                        )

                    if not "relations" in revisions:
                        revisions["relations"] = {}

                    revisions["relations"][relation_type_name] = {}
                    for nid in ids["nids"]:
                        relation = relations[nid]
                        start_entity_type_id = (
                            await self._data_repo.get_type_id_from_graph_id(
                                await self._get_project_id(),
                                relation["start_id"],
                                connection,
                            )
                        )
                        end_entity_type_id = (
                            await self._data_repo.get_type_id_from_graph_id(
                                await self._get_project_id(),
                                relation["end_id"],
                                connection,
                            )
                        )
                        revisions["relations"][relation_type_name][
                            relation["properties"]["id"]
                        ] = [
                            relation["properties"],
                            None,
                            start_entity_type_id,
                            relation["start_properties"]["id"],
                            end_entity_type_id,
                            relation["end_properties"]["id"],
                        ]

                # Delete relation sources before relations are deleted
                # TODO: optimize
                for relation_type_id, ids in grouped_relation_ids.items():
                    for relation_id in ids:
                        old_raw_relation_sources = (
                            await self._data_repo.delete_relation_sources(
                                await self._get_project_id(),
                                relation_type_id,
                                relation_id,
                                connection,
                            )
                        )
                        for old_raw_relation_source in old_raw_relation_sources:
                            if "relations" not in revisions:
                                revisions["relations"] = {}
                            if "_source_" not in revisions["relations"]:
                                revisions["relations"]["_source_"] = {}
                            # strip off ::edge
                            source_relation_properties = json.loads(
                                old_raw_relation_source["e"][:-6]
                            )["properties"]
                            # strip off ::vertex
                            source = json.loads(old_raw_relation_source["s"][:-8])
                            revisions["relations"]["_source_"][
                                source_relation_properties["id"]
                            ] = [
                                source_relation_properties,
                                None,
                                relation_type_name,
                                relation_id,
                                # strip n_
                                await self._config_manager.get_entity_type_name_by_id(
                                    self._project_name,
                                    utd(source["label"][2:]),
                                ),
                                source["properties"]["id"],
                            ]

                for relation_type_id, ids in grouped_relation_ids.items():
                    await self._data_repo.delete_raw_relations(
                        await self._get_project_id(),
                        relation_type_id,
                        ids["nids"],
                        ids["ids"],
                        connection,
                    )

                # Generate Elasticsearch update query before deleting the entity
                await self.update_es_query(
                    es_query,
                    "entities",
                    entity_type_name,
                    entity_id,
                    dictdiffer.diff(old_entity, {}),
                    connection,
                    old_id=entity_id,
                )

                # Delete entity
                await self._data_repo.delete_entity(
                    await self._get_project_id(),
                    entity_type_id,
                    entity_id,
                    connection,
                )

                revisions["entities"] = {
                    entity_type_name: {
                        entity_id: [
                            old_entity,
                            None,
                        ]
                    }
                }

                await self._revision_manager.post_revision(
                    revisions,
                    connection,
                )

                await self.update_es(es_query, connection)

    async def _get_relations_triplehop(
        self,
        entity_ids: typing.List[int],
        inverse: bool = False,
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
        relation_type_name: typing.Optional[str] = None,
        relation_type_id: typing.Optional[str] = None,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        """
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
        """
        self.__class__._require_entity_type_name_or_entity_type_id(
            entity_type_name, entity_type_id
        )
        self.__class__._require_relation_type_name_or_entity_type_id(
            relation_type_name, relation_type_id
        )

        if entity_type_id is None:
            entity_type_id = await self._config_manager.get_entity_type_id_by_name(
                self._project_name,
                entity_type_name,
                connection=connection,
            )

        if relation_type_id is None:
            relation_type_id = await self._config_manager.get_relation_type_id_by_name(
                self._project_name,
                relation_type_name,
                connection=connection,
            )

        records = await self._data_repo.get_relations_from_start_entities(
            await self._get_project_id(),
            entity_type_id,
            entity_ids,
            relation_type_id,
            inverse,
            connection=connection,
        )

        # build temporary dict so json only needs to be loaded once
        results = {}
        for record in records:
            entity_id = record["id"]
            relation_properties = json.loads(record["e_properties"])
            entity_properties = json.loads(record["n_properties"])
            etid = await self._data_repo.get_type_id_from_graph_id(
                await self._get_project_id(),
                record["n_id"],
                connection=connection,
            )

            if entity_id not in results:
                results[entity_id] = {}

            results[entity_id][relation_properties["id"]] = {
                "r_props": relation_properties,
                "e_props": entity_properties,
                "entity_type_id": etid,
                "sources": [],
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
        await self._check_permission("get", "relations", relation_type_name, {})

        triplehop_results = await self._get_relations_triplehop(
            entity_ids,
            inverse,
            entity_type_name=entity_type_name,
            relation_type_name=relation_type_name,
        )

        if len(triplehop_results) == 0:
            return {}

        relation_ids = [
            rid for eid in triplehop_results for rid in triplehop_results[eid]
        ]
        relation_type_id = await self._config_manager.get_relation_type_id_by_name(
            self._project_name,
            relation_type_name,
        )

        source_records = []
        if relation_type_name != "_source_":
            source_records = await self._data_repo.get_relations_sources(
                await self._get_project_id(),
                relation_type_id,
                relation_ids,
            )

        # build temporary dict so sources can easily be retrieved
        source_results = {}
        for source_record in source_records:
            rel_id = source_record["id"]
            if rel_id not in source_results:
                source_results[rel_id] = []

            source_results[rel_id].append(
                {
                    "r_props": json.loads(source_record["e_properties"]),
                    "e_props": json.loads(source_record["n_properties"]),
                    "entity_type_id": await self._data_repo.get_type_id_from_graph_id(
                        await self._get_project_id(),
                        source_record["n_id"],
                    ),
                }
            )

        rtpm = await self._config_manager.get_relation_type_property_mapping(
            self._project_name, relation_type_name
        )
        etpma = await self._config_manager.get_entity_type_property_mapping(
            self._project_name, "__all__"
        )
        rtpma = await self._config_manager.get_relation_type_property_mapping(
            self._project_name, "__all__"
        )
        srtpm = await self._config_manager.get_relation_type_property_mapping(
            self._project_name, "_source_"
        )
        etd = {}

        results = {}
        for entity_id, triplehop_result in triplehop_results.items():
            results[entity_id] = []
            for rel_id, rel_result in triplehop_result.items():
                etid = rel_result["entity_type_id"]
                # keep a dict of entity type definitions
                if etid not in etd:
                    etn = await self._config_manager.get_entity_type_name_by_id(
                        self._project_name, etid
                    )
                    etd[etid] = {
                        "etn": etn,
                        "etpm": await self._config_manager.get_entity_type_property_mapping(
                            self._project_name, etn
                        ),
                    }
                etpm = etd[etid]["etpm"]

                result = {
                    rtpm[k]: v for k, v in rel_result["r_props"].items() if k in rtpm
                }
                result["entity"] = {
                    etpm[k]: v for k, v in rel_result["e_props"].items() if k in etpm
                }
                result["entity"]["__typename"] = first_cap(etd[etid]["etn"])

                # Add properties for source relations
                if relation_type_id == "_source_":
                    if "properties" in result:
                        props = []
                        for p in result["properties"]:
                            m = RE_SOURCE_PROP_INDEX.match(p)
                            if m:
                                p = f'p_{dtu(m.group("property"))}'
                                if p in etpma:
                                    props.append(f'{etpma[p]}[{m.group("index")}]')
                            else:
                                p = f"p_{dtu(p)}"
                                if p in etpma:
                                    props.append(etpma[p])
                        result["properties"] = props

                # Source information on relations
                result["_source_"] = []
                if rel_id in source_results:
                    for source in source_results[rel_id]:
                        setid = source["entity_type_id"]
                        if setid not in etd:
                            etn = await self._config_manager.get_entity_type_name_by_id(
                                self._project_name, setid
                            )
                            etd[setid] = {
                                "etn": etn,
                                "etpm": await self._config_manager.get_entity_type_property_mapping(
                                    self._project_name,
                                    etn,
                                ),
                            }
                        setpm = etd[setid]["etpm"]

                        source_result = {
                            srtpm[k]: v
                            for k, v in source["r_props"].items()
                            if k in srtpm
                        }
                        source_result["entity"] = {
                            setpm[k]: v
                            for k, v in source["e_props"].items()
                            if k in setpm
                        }
                        source_result["entity"]["__typename"] = first_cap(
                            etd[setid]["etn"]
                        )
                        if "properties" in source_result:
                            props = []
                            for p in source_result["properties"]:
                                m = RE_SOURCE_PROP_INDEX.match(p)
                                if m:
                                    p = f'p_{dtu(m.group("property"))}'
                                    if p in rtpma:
                                        props.append(f'{rtpma[p]}[{m.group("index")}]')
                                else:
                                    p = f"p_{dtu(p)}"
                                    if p in rtpm:
                                        props.append(rtpma[p])
                            source_result["properties"] = props
                        result["_source_"].append(source_result)

                results[entity_id].append(result)
        return results

    async def get_entity_ids_by_type_name(
        self,
        entity_type_name: str,
    ):
        return await self._data_repo.get_entity_ids(
            await self._get_project_id(),
            await self._config_manager.get_entity_type_id_by_name(
                self._project_name, entity_type_name
            ),
        )

    async def get_entity_data(
        self,
        entity_ids: typing.List[int],
        triplehop_query: typing.Dict,
        first_iteration: bool = True,
        entity_type_name: typing.Optional[str] = None,
        entity_type_id: typing.Optional[str] = None,
        connection: asyncpg.Connection = None,
    ) -> typing.Dict:
        if not entity_ids:
            return {}

        if not triplehop_query:
            raise Exception("Empty query")

        self.__class__._require_entity_type_name_or_entity_type_id(
            entity_type_name, entity_type_id
        )

        entity_type_name_or_id = {}
        if entity_type_name is not None:
            entity_type_name_or_id["entity_type_name"] = entity_type_name
        elif entity_type_id is not None:
            entity_type_name_or_id["entity_type_id"] = entity_type_id

        results = {entity_id: {} for entity_id in entity_ids}
        # start entity
        if first_iteration:
            # check if entity props are requested
            if triplehop_query["e_props"]:
                results = await self._get_entities_triplehop(
                    entity_ids, **entity_type_name_or_id, connection=connection
                )

        for relation_type_id in triplehop_query["relations"]:
            # get relation data
            raw_results = await self._get_relations_triplehop(
                entity_ids,
                relation_type_id.split("_")[0] == "ri",
                **entity_type_name_or_id,
                relation_type_id=relation_type_id.split("_")[1],
                connection=connection,
            )
            for entity_id, raw_result in raw_results.items():
                if "relations" not in results[entity_id]:
                    results[entity_id]["relations"] = {}
                results[entity_id]["relations"][relation_type_id] = raw_result

            # gather what further information is required
            rel_entities = {}
            raw_rel_results_per_entity_type_id = {}
            # mapping so results (identified by entity_type_name, entity_id)
            # can be added in the right place (identified by relation_type_id, relation_id)
            mapping = {}
            if triplehop_query["relations"][relation_type_id]["relations"]:
                for entity_id, raw_relation_results in raw_results.items():
                    for relation_id, raw_result in raw_relation_results.items():
                        rel_entity_type_id = raw_result["entity_type_id"]
                        rel_entity_id = raw_result["e_props"]["id"]

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
                    raw_rel_results_per_entity_type_id[
                        rel_entity_type_id
                    ] = await self.get_entity_data(
                        list(rel_entity_ids),
                        triplehop_query["relations"][relation_type_id],
                        False,
                        entity_type_id=rel_entity_type_id,
                        connection=connection,
                    )

                # add the additional relation data to the result
                for entity_id in results:
                    if (
                        "relations" in results[entity_id]
                        and relation_type_id in results[entity_id]["relations"]
                    ):
                        for relation_id in results[entity_id]["relations"][
                            relation_type_id
                        ]:
                            (rel_entity_type_id, rel_entity_id) = mapping[
                                relation_type_id
                            ][relation_id]
                            if (
                                rel_entity_id
                                in raw_rel_results_per_entity_type_id[
                                    rel_entity_type_id
                                ]
                                and "relations"
                                in raw_rel_results_per_entity_type_id[
                                    rel_entity_type_id
                                ][rel_entity_id]
                            ):
                                results[entity_id]["relations"][relation_type_id][
                                    relation_id
                                ]["relations"] = raw_rel_results_per_entity_type_id[
                                    rel_entity_type_id
                                ][
                                    rel_entity_id
                                ][
                                    "relations"
                                ]

        return results

    async def update_es_query(
        self,
        es_query: typing.Dict,
        entities_or_relations: str,
        type_name: str,
        id: int,
        diff_gen: typing.Generator,
        connection: asyncpg.Connection,
        new_id: int = None,
        old_id: int = None,
    ) -> None:
        if entities_or_relations == "entities":
            type_id = await self._config_manager.get_entity_type_id_by_name(
                self._project_name,
                type_name,
                connection=connection,
            )
        else:
            type_id = await self._config_manager.get_relation_type_id_by_name(
                self._project_name,
                type_name,
                connection=connection,
            )

        p_diff_field_ids = set()
        r_diff_ids = set()
        for diff in diff_gen:
            # add or remove on root
            if diff[1] == "":
                property_names = [alter[0] for alter in diff[2]]
                if "id" in property_names:
                    if entities_or_relations == "entities":
                        # post of a new entity
                        for property_name in property_names:
                            # don't add id to properties to index
                            if property_name == "id":
                                continue
                            p_diff_field_ids.add(property_name)
                    else:
                        # relation is being added or deleted
                        r_diff_ids.add(type_id)
                else:
                    for property_name in property_names:
                        p_diff_field_ids.add(property_name)
            # When processing list value changes, the dictdiffer key is a list
            elif isinstance(diff[1], list):
                p_diff_field_ids.add(diff[1][0])
            else:
                p_diff_field_ids.add(diff[1])

        # add $, strip p_, replace underscores with dashes
        diff_field_ids = [
            f"${utd(p_diff_field_id[2:])}" for p_diff_field_id in p_diff_field_ids
        ]
        for r_diff_id in r_diff_ids:
            diff_field_ids.append(f"$r_{r_diff_id}")
            diff_field_ids.append(f"$ri_{r_diff_id}")

        async def add_entities_and_field_to_update(
            es_entity_type_id: str,
            diff_field_id: str,
            es_field_system_name: str,
            selector_value: str = None,
        ) -> None:
            if diff_field_id[:2] != "$r":
                entity_ids = await self.find_entities_to_update(
                    es_entity_type_id,
                    entities_or_relations,
                    type_id,
                    id,
                    selector_value,
                    diff_field_id,
                    connection,
                )
            else:
                entity_ids = await self.find_entities_to_update_for_relation(
                    es_entity_type_id,
                    type_id,
                    id,
                    diff_field_id,
                    connection,
                )

            for e_id in entity_ids:
                action = "update"
                if e_id == new_id:
                    action = "index"
                if e_id == old_id:
                    action = "delete"
                if action not in es_query:
                    es_query[action] = {}
                if es_entity_type_id not in es_query[action]:
                    es_query[action][es_entity_type_id] = {}
                if e_id not in es_query[action][es_entity_type_id]:
                    es_query[action][es_entity_type_id][e_id] = set()
                if action != "delete":
                    es_query[action][es_entity_type_id][e_id].add(es_field_system_name)

        for es_etn, etd in (await self._get_entity_types_config()).items():
            if "config" in etd:
                es_entity_type_id = (
                    await self._config_manager.get_entity_type_id_by_name(
                        self._project_name,
                        es_etn,
                    )
                )

                if "es_data" in etd["config"]:
                    # TODO: merge with es.base.extract_query_from_es_data_config?
                    for es_field_def in etd["config"]["es_data"]["fields"]:
                        if es_field_def["type"] in ["nested", "nested_flatten"]:
                            # If added or removed relation in base:
                            # update complete nested field
                            # diff_field_id will start with $r, so a selector_value is not needed
                            for diff_field_id in diff_field_ids:
                                if diff_field_id in es_field_def["base"]:
                                    await add_entities_and_field_to_update(
                                        es_entity_type_id,
                                        diff_field_id,
                                        es_field_def["system_name"],
                                    )
                            for part in es_field_def["parts"].values():
                                for diff_field_id in diff_field_ids:
                                    if diff_field_id in part:
                                        if part[0] == ".":
                                            selector_value = (
                                                f"{es_field_def['base']}{part}"
                                            )
                                        else:
                                            selector_value = (
                                                f"{es_field_def['base']}->{part}"
                                            )
                                        await add_entities_and_field_to_update(
                                            es_entity_type_id,
                                            diff_field_id,
                                            es_field_def["system_name"],
                                            selector_value,
                                        )
                                        if "filter" in es_field_def:
                                            if es_field_def["filter"][0] == ".":
                                                selector_value = f"{es_field_def['base']}{es_field_def['filter']}"
                                            else:
                                                selector_value = f"{es_field_def['base']}->{es_field_def['filter']}"
                                            await add_entities_and_field_to_update(
                                                es_entity_type_id,
                                                diff_field_id,
                                                es_field_def["system_name"],
                                                selector_value,
                                            )
                        elif es_field_def["type"] == "edtf_interval":
                            for diff_field_id in diff_field_ids:
                                if diff_field_id in es_field_def["start"]:
                                    await add_entities_and_field_to_update(
                                        es_entity_type_id,
                                        diff_field_id,
                                        es_field_def["system_name"],
                                        es_field_def["start"],
                                    )
                                if diff_field_id in es_field_def["end"]:
                                    await add_entities_and_field_to_update(
                                        es_entity_type_id,
                                        diff_field_id,
                                        es_field_def["system_name"],
                                        es_field_def["end"],
                                    )
                        else:
                            for diff_field_id in diff_field_ids:
                                if diff_field_id in es_field_def["selector_value"]:
                                    await add_entities_and_field_to_update(
                                        es_entity_type_id,
                                        diff_field_id,
                                        es_field_def["system_name"],
                                        es_field_def["selector_value"],
                                    )

    async def update_es(
        self,
        es_query: typing.Dict,
        connection: asyncpg.Connection,
    ) -> None:
        entity_types_config = await self._config_manager.get_entity_types_config(
            self._project_name
        )

        for action in es_query:
            for es_entity_type_id in es_query[action]:
                entity_type_name = (
                    await self._config_manager.get_entity_type_name_by_id(
                        self._project_name, es_entity_type_id
                    )
                )
                entity_type_config = entity_types_config[entity_type_name]
                # Batch entities in lists with the same entity type and the same required fields
                while es_query[action][es_entity_type_id]:
                    batch_entity_ids = []
                    [e_id, es_field_system_names] = es_query[action][
                        es_entity_type_id
                    ].popitem()
                    batch_entity_ids = [
                        i
                        for i in es_query[action][es_entity_type_id]
                        if es_query[action][es_entity_type_id][i]
                        == es_field_system_names
                    ]
                    for other_e_id in batch_entity_ids:
                        del es_query[action][es_entity_type_id][other_e_id]

                    batch_entity_ids.append(e_id)

                    es_data_config = [
                        field_def
                        for field_def in entity_type_config["config"]["es_data"][
                            "fields"
                        ]
                        if field_def["system_name"] in es_field_system_names
                    ]
                    triplehop_query = (
                        BaseElasticsearch.extract_query_from_es_data_config(
                            es_data_config
                        )
                    )

                    batch_counter = 0
                    while True:
                        batch_ids = batch_entity_ids[
                            batch_counter
                            * BATCH_SIZE : (batch_counter + 1)
                            * BATCH_SIZE
                        ]
                        if action == "delete":
                            batch_entities = {batch_id: None for batch_id in batch_ids}

                        else:
                            batch_entities = await self.get_entity_data(
                                batch_ids,
                                triplehop_query,
                                entity_type_id=es_entity_type_id,
                                connection=connection,
                            )

                        batch_docs = BaseElasticsearch.convert_entities_to_docs(
                            entity_types_config, es_data_config, batch_entities
                        )

                        await self._es.op_bulk(es_entity_type_id, batch_docs, action)

                        if (batch_counter + 1) * BATCH_SIZE + 1 > len(batch_entity_ids):
                            break

                        batch_counter += 1

    # TODO: merge with es.base.extract_query_from_es_data_config?
    async def find_entities_to_update(
        self,
        es_entity_type_id: str,
        entities_or_relations: str,
        type_id: str,
        id: int,
        selector_value: str,
        diff_field_id: str,
        connection: asyncpg.Connection,
    ) -> typing.Set:
        result = set()
        for match in RE_FIELD_CONVERSION.finditer(selector_value):
            if diff_field_id not in match.group(0):
                continue

            if entities_or_relations == "entities":
                # Property of the entity type itself
                if diff_field_id == match.group(0):
                    result.add(id)
                    continue

                # Property of another entity type
                path = match.group(0).split("->")
                if path[-1] != diff_field_id:
                    raise Exception("Updated field is not last part of query path")

                entity_ids = await self._data_repo.find_entities_linked_to_entity(
                    await self._get_project_id(),
                    es_entity_type_id,
                    type_id,
                    id,
                    path[:-1],
                    connection,
                )
            else:
                # Property of an entity type
                path = match.group(0).split(".")
                if path[1] != diff_field_id:
                    raise Exception("Updated field is not last part of query path")

                path = path[0].split("->")

                entity_ids = await self._data_repo.find_entities_linked_to_relation(
                    await self._get_project_id(),
                    es_entity_type_id,
                    type_id,
                    id,
                    path,
                    connection,
                )

            if entity_ids:
                result.update(entity_ids)

        return result

    async def find_entities_to_update_for_relation(
        self,
        es_entity_type_id: str,
        type_id: str,
        id: int,
        diff_field_id: str,
        connection: asyncpg.Connection,
    ) -> typing.Set:
        result = set()

        entity_ids = await self._data_repo.find_entities_linked_to_relation(
            await self._get_project_id(),
            es_entity_type_id,
            type_id,
            id,
            [diff_field_id],
            connection,
        )

        if entity_ids:
            result.update(entity_ids)

        return result
