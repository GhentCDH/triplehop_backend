import copy
import typing

import aiocache
import ariadne
import starlette

from app.cache.core import create_schema_key_builder
from app.graphql.base import construct_def
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.utils import RE_FIELD_CONVERSION


class GraphQLConfigBuilder:
    def __init__(
        self, request: starlette.requests.Request, user: UserWithPermissions
    ) -> None:
        self._project_name = request.path_params["project_name"]
        self._user = user
        self._config_manager = ConfigManager(request, self._user)

    @staticmethod
    def _replace_field_ids_by_system_names(
        input: str,
        entity_field_lookup: typing.Dict,
        relation_field_lookup: typing.Dict,
        relation_lookup: typing.Dict,
    ) -> str:
        result = input
        for match in RE_FIELD_CONVERSION.finditer(input):
            if not match:
                continue

            path = [p.replace("$", "") for p in match.group(0).split("->")]
            for i, p in enumerate(path):
                # last element => p = relation.r_prop or e_prop
                if i == len(path) - 1:
                    # relation property
                    if "." in p:
                        (rel_type_id, r_prop_id) = p.split(".")
                        result = result.replace(
                            rel_type_id.split("_")[1],
                            relation_lookup[rel_type_id.split("_")[1]],
                        )
                        result = result.replace(
                            r_prop_id, relation_field_lookup[r_prop_id]
                        )
                        break
                    # entity property
                    if p != "id":
                        result = result.replace(p, entity_field_lookup[p])
                    break
                # not last element => p = relation
                result = result.replace(
                    p.split("_")[1], relation_lookup[p.split("_")[1]]
                )

        return result

    @staticmethod
    def _replace_show_condition(
        input: str,
        entity_lookup: typing.Dict,
    ) -> str:
        result = input
        for entity_type_id, entity_type_name in entity_lookup.items():
            result = result.replace(f"${entity_type_id}", f"${entity_type_name}")

        return result

    # Relation layouts can only have data from their own data fields, this is passed as entity_field_lookup.
    @staticmethod
    def _layout_field_converter(
        layout: typing.List,
        entity_field_lookup: typing.Dict = None,
        relation_field_lookup: typing.Dict = None,
        entity_lookup: typing.Dict = None,
        relation_lookup: typing.Dict = None,
    ) -> typing.List:
        result = copy.deepcopy(layout)
        for panel in result:
            for field in panel["fields"]:
                field[
                    "field"
                ] = GraphQLConfigBuilder._replace_field_ids_by_system_names(
                    input=field["field"],
                    entity_field_lookup=entity_field_lookup,
                    relation_field_lookup=relation_field_lookup,
                    relation_lookup=relation_lookup,
                )
                if "base_layer" in field:
                    field[
                        "base_layer"
                    ] = GraphQLConfigBuilder._replace_field_ids_by_system_names(
                        input=field["base_layer"],
                        entity_field_lookup=entity_field_lookup,
                        relation_field_lookup=relation_field_lookup,
                        relation_lookup=relation_lookup,
                    )
                if "show_condition" in field:
                    field[
                        "show_condition"
                    ] = GraphQLConfigBuilder._replace_show_condition(
                        input=field["show_condition"],
                        entity_lookup=entity_lookup,
                    )
        return result

    @staticmethod
    def _es_columns_converter(
        columns: typing.List, es_data_conf: typing.Dict
    ) -> typing.List:
        results = []
        for column in columns:
            result = {
                "system_name": es_data_conf[column["column"][1:]]["system_name"],
                "display_name": column.get(
                    "display_name", es_data_conf[column["column"][1:]]["display_name"]
                ),
                "type": es_data_conf[column["column"][1:]]["type"],
                "sortable": column["sortable"],
            }
            for key in [
                "searchable",
                "main_link",
                "link",
                "sub_field",
                "sub_field_type",
            ]:
                if key in column:
                    result[key] = column[key]
            results.append(result)
        return results

    @staticmethod
    def _es_filters_converter(
        filters: typing.List, es_data_conf: typing.Dict
    ) -> typing.List:
        result = []
        for section in filters:
            res_section = {
                "filters": [],
            }
            for filter in section["filters"]:
                filter_conf = {
                    "system_name": es_data_conf[filter["filter"][1:]]["system_name"],
                    "display_name": es_data_conf[filter["filter"][1:]]["display_name"],
                    "type": filter.get(
                        "type", es_data_conf[filter["filter"][1:]]["type"]
                    ),
                }
                if "interval" in filter:
                    filter_conf["interval"] = filter["interval"]
                res_section["filters"].append(filter_conf)
            result.append(res_section)
        return result

    def _get_project_config_resolver_wrapper(self):
        async def resolver(*_):
            return await self._config_manager.get_project_config(self._project_name)

        return resolver

    def _get_entity_configs_resolver_wrapper(self):
        async def resolver(*_):
            entity_types_config = await self._config_manager.get_entity_types_config(
                self._project_name
            )
            relation_types_config = (
                await self._config_manager.get_relation_types_config(self._project_name)
            )

            entity_field_lookup = {}
            for entity_config in entity_types_config.values():
                if (
                    "data" in entity_config["config"]
                    and "fields" in entity_config["config"]["data"]
                ):
                    for field_id, config in entity_config["config"]["data"][
                        "fields"
                    ].items():
                        entity_field_lookup[field_id] = config["system_name"]

            relation_lookup = {}
            relation_field_lookup = {}
            for relation_system_name, relation_config in relation_types_config.items():
                relation_lookup[relation_config["id"]] = relation_system_name
                if (
                    "data" in relation_config["config"]
                    and "fields" in relation_config["config"]["data"]
                ):
                    for field_id, config in relation_config["config"]["data"][
                        "fields"
                    ].items():
                        relation_field_lookup[field_id] = config["system_name"]

            results = []
            for entity_system_name, entity_config in entity_types_config.items():
                data_conf = {}
                config_item = {
                    "system_name": entity_system_name,
                    "display_name": entity_config["display_name"],
                }
                # TODO: remove configs that cannot be used by users based on permissions
                if "detail" in entity_config["config"]:
                    config_item["detail"] = entity_config["config"]["detail"]
                if "source" in entity_config["config"]:
                    config_item["source"] = entity_config["config"]["source"]
                if (
                    "data" in entity_config["config"]
                    and "fields" in entity_config["config"]["data"]
                ):
                    data_conf = entity_config["config"]["data"]["fields"]
                    config_item["data"] = list(data_conf.values())
                # TODO: add display_names from data to display, so data doesn't need to be exported
                # TODO: figure out a way to add permissions for displaying layouts and fields
                if "display" in entity_config["config"]:
                    config_item["display"] = {}
                    if "title" in entity_config["config"]["display"]:
                        config_item["display"][
                            "title"
                        ] = self.__class__._replace_field_ids_by_system_names(
                            input=entity_config["config"]["display"]["title"],
                            entity_field_lookup=entity_field_lookup,
                            relation_field_lookup=relation_field_lookup,
                            relation_lookup=relation_lookup,
                        )
                    if "layout" in entity_config["config"]["display"]:
                        config_item["display"][
                            "layout"
                        ] = self.__class__._layout_field_converter(
                            layout=entity_config["config"]["display"]["layout"],
                            entity_field_lookup=entity_field_lookup,
                            relation_field_lookup=relation_field_lookup,
                            relation_lookup=relation_lookup,
                        )
                if "edit" in entity_config["config"]:
                    config_item["edit"] = {
                        "layout": self.__class__._layout_field_converter(
                            layout=entity_config["config"]["edit"]["layout"],
                            entity_field_lookup=entity_field_lookup,
                            relation_field_lookup=relation_field_lookup,
                            relation_lookup=relation_lookup,
                        ),
                    }
                if (
                    "es_data" in entity_config["config"]
                    and "fields" in entity_config["config"]["es_data"]
                    and "es_display" in entity_config["config"]
                ):
                    es_data_conf = {
                        esd["system_name"]: esd
                        for esd in entity_config["config"]["es_data"]["fields"]
                    }
                    config_item["elasticsearch"] = {
                        "title": entity_config["config"]["es_display"]["title"],
                        "columns": self.__class__._es_columns_converter(
                            entity_config["config"]["es_display"]["columns"],
                            es_data_conf,
                        ),
                        "filters": self.__class__._es_filters_converter(
                            entity_config["config"]["es_display"]["filters"],
                            es_data_conf,
                        ),
                    }
                if "style" in entity_config["config"]:
                    config_item["style"] = entity_config["config"]["style"]
                results.append(config_item)

            return results

        return resolver

    def _get_relation_configs_resolver_wrapper(self):
        async def resolver(*_):
            entity_types_config = await self._config_manager.get_entity_types_config(
                self._project_name
            )
            relation_types_config = (
                await self._config_manager.get_relation_types_config(self._project_name)
            )

            entity_lookup = {}
            for entity_system_name, entity_config in entity_types_config.items():
                entity_lookup[entity_config["id"]] = entity_system_name

            relation_lookup = {}
            relation_field_lookup = {}
            for relation_system_name, relation_config in relation_types_config.items():
                relation_lookup[relation_config["id"]] = relation_system_name
                if (
                    "data" in relation_config["config"]
                    and "fields" in relation_config["config"]["data"]
                ):
                    for id_, config in relation_config["config"]["data"][
                        "fields"
                    ].items():
                        relation_field_lookup[id_] = config["system_name"]

            results = []
            for relation_system_name, relation_config in relation_types_config.items():
                config_item = {
                    "system_name": relation_system_name,
                    "display_name": relation_config["display_name"],
                    "domain_names": relation_config["domain_names"],
                    "range_names": relation_config["range_names"],
                }
                if (
                    "data" in relation_config["config"]
                    and "fields" in relation_config["config"]["data"]
                ):
                    data_conf = relation_config["config"]["data"]["fields"]
                    config_item["data"] = list(data_conf.values())
                if "display" in relation_config["config"]:
                    config_item["display"] = {}
                    if "domain_title" in relation_config["config"]["display"]:
                        config_item["display"]["domain_title"] = relation_config[
                            "config"
                        ]["display"]["domain_title"]
                    if "range_title" in relation_config["config"]["display"]:
                        config_item["display"]["range_title"] = relation_config[
                            "config"
                        ]["display"]["range_title"]
                    if "layout" in relation_config["config"]["display"]:
                        config_item["display"][
                            "layout"
                        ] = self.__class__._layout_field_converter(
                            layout=relation_config["config"]["display"]["layout"],
                            # use relation_field_lookup as entity_field_lookup
                            entity_field_lookup=relation_field_lookup,
                            entity_lookup=entity_lookup,
                        )
                if "edit" in relation_config["config"]:
                    config_item["edit"] = {}
                    if "domain_title" in relation_config["config"]["edit"]:
                        config_item["edit"]["domain_title"] = relation_config["config"][
                            "edit"
                        ]["domain_title"]
                    if "range_title" in relation_config["config"]["edit"]:
                        config_item["edit"]["range_title"] = relation_config["config"][
                            "edit"
                        ]["range_title"]
                    if "layout" in relation_config["config"]["edit"]:
                        config_item["edit"][
                            "layout"
                        ] = self.__class__._layout_field_converter(
                            layout=relation_config["config"]["edit"]["layout"],
                            # use relation_field_lookup as entity_field_lookup
                            entity_field_lookup=relation_field_lookup,
                        )
                results.append(config_item)

            return results

        return resolver

    def _add_get_project_config_schema_parts(self):
        self._type_defs_dict["Query"].append(["getProject_config", "Project_config"])
        self._type_defs_dict.update(
            {
                "Project_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                ],
            }
        )

        self._query_dict["Query"].set_field(
            "getProject_config",
            self._get_project_config_resolver_wrapper(),
        )

    def _add_common_type_defs(self):
        self._type_defs_dict.update(
            {
                "Data_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                    ["type", "String!"],
                    ["validators", "[Validator!]"],
                ],
                "Validator": [
                    ["type", "String!"],
                    ["regex", "String"],
                    ["error_message", "String"],
                ],
                "Display_panel_config": [
                    ["label", "String"],
                    ["fields", "[Display_panel_field_config!]"],
                ],
                "Display_panel_field_config": [
                    ["label", "String"],
                    ["field", "String!"],
                    ["type", "String"],
                    # TODO: allow multiple base layers
                    # TODO: add overlays
                    ["base_layer", "String"],
                    ["base_url", "String"],
                    ["show_condition", "String"],
                ],
                "Edit_panel_config": [
                    ["label", "String"],
                    ["fields", "[Edit_panel_field_config!]"],
                ],
                "Edit_panel_field_config": [
                    ["label", "String"],
                    ["field", "String!"],
                    ["type", "String"],
                    ["placeholder", "String"],
                    ["help_message", "String"],
                    ["multi", "Boolean"],
                    ["options", "[String!]"],
                ],
            }
        )

    def _add_get_entity_configs_schema_parts(self):
        self._type_defs_dict["Query"].append(["getEntity_config_s", "[Entity_config]"])
        self._type_defs_dict.update(
            {
                "Entity_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                    ["detail", "Boolean"],
                    ["source", "Boolean"],
                    ["data", "[Data_config!]"],
                    ["display", "Entity_display_config"],
                    ["edit", "Entity_edit_config"],
                    ["elasticsearch", "Es_config"],
                    ["style", "Style"],
                ],
                "Entity_display_config": [
                    ["title", "String!"],
                    ["layout", "[Display_panel_config!]"],
                ],
                "Entity_edit_config": [
                    ["layout", "[Edit_panel_config!]"],
                ],
                "Es_config": [
                    ["title", "String!"],
                    ["columns", "[Es_column_config!]"],
                    ["filters", "[Es_filter_group_config!]"],
                ],
                "Es_column_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                    ["type", "String!"],
                    ["sortable", "Boolean!"],
                    ["searchable", "Boolean"],
                    ["main_link", "Boolean"],
                    ["link", "Boolean"],
                    ["sub_field", "String"],
                    ["sub_field_type", "String"],
                ],
                "Es_filter_group_config": [
                    ["filters", "[Es_filter_config!]"],
                ],
                "Es_filter_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                    ["type", "String!"],
                    ["interval", "Int"],
                ],
                "Style": [
                    ["search", "[String]"],
                    ["detail", "[String]"],
                ],
            }
        )

        self._query_dict["Query"].set_field(
            "getEntity_config_s",
            self._get_entity_configs_resolver_wrapper(),
        )

    def _add_get_relation_configs_schema_parts(self):
        self._type_defs_dict["Query"].append(
            ["getRelation_config_s", "[Relation_config]"]
        )
        self._type_defs_dict.update(
            {
                "Relation_config": [
                    ["system_name", "String!"],
                    ["display_name", "String!"],
                    ["data", "[Data_config!]"],
                    ["display", "Relation_display_config"],
                    ["edit", "Relation_edit_config"],
                    ["domain_names", "[String!]!"],
                    ["range_names", "[String!]!"],
                ],
                # Don't allow title override (multiple ranges / domains possible => impossible to define)
                "Relation_display_config": [
                    ["domain_title", "String!"],
                    ["range_title", "String!"],
                    ["layout", "[Display_panel_config!]"],
                ],
                "Relation_edit_config": [
                    ["domain_title", "String!"],
                    ["range_title", "String!"],
                    ["layout", "[Edit_panel_config!]"],
                ],
            }
        )

        self._query_dict["Query"].set_field(
            "getRelation_config_s",
            self._get_relation_configs_resolver_wrapper(),
        )

    # TODO: reset cache when project is updated or user permissions have been updated
    @aiocache.cached(key_builder=create_schema_key_builder)
    async def create_schema(self):
        self._type_defs_dict = {"Query": []}
        self._query_dict = {"Query": ariadne.QueryType()}

        self._add_get_project_config_schema_parts()

        # Data config, Display panel and edit panel are used both in entity config and relation config
        self._add_common_type_defs()

        self._add_get_entity_configs_schema_parts()
        self._add_get_relation_configs_schema_parts()

        type_defs_array = [
            construct_def("type", type, props)
            for type, props in self._type_defs_dict.items()
        ]

        type_defs = ariadne.gql("\n\n".join(type_defs_array))

        return ariadne.make_executable_schema(type_defs, *self._query_dict.values())
