import typing

import aiocache
import aiodataloader
import ariadne
import graphql
import starlette

from app.cache.core import create_schema_key_builder
from app.graphql.base import construct_def
from app.mgmt.auth import allowed_entities_or_relations_and_properties
from app.mgmt.config import ConfigManager
from app.mgmt.data import DataManager
from app.models.auth import UserWithPermissions
from app.utils import first_cap


class GraphQLDataBuilder:
    def __init__(
        self, request: starlette.requests.Request, user: UserWithPermissions
    ) -> None:
        self._project_name = request.path_params["project_name"]
        self._user = user
        self._data_manager = DataManager(request, self._user)
        self._config_manager = ConfigManager(request, self._user)

    @staticmethod
    def _get_requested_entity_props(info: graphql.GraphQLResolveInfo):
        return sorted(
            list(
                set(
                    [
                        selection.name.value
                        for selection in info.field_nodes[0].selection_set.selections
                        if (
                            selection.name.value[0] != "_"
                            and selection.name.value[0:2] != "r_"
                            and selection.name.value[0:3] != "ri_"
                        )
                    ]
                )
            )
        )

    # TODO: only get the requested properties
    # TODO: get all required information (entity -> relation -> entity -> ...) in a single request
    # dataloader to prevent N+1: https://github.com/mirumee/ariadne/discussions/508)#discussioncomment-525811
    def _get_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        def get_entities_wrapper(props: typing.List[str]):
            async def get_entities(entity_ids: typing.List[int]):
                data = await self._data_manager.get_entities(
                    entity_type_name, props, entity_ids
                )
                # dataloader expects sequence of objects or None following order of ids in ids
                return [data.get(id) for id in entity_ids]

            return get_entities

        async def load_entity(
            info, id: int, props: typing.List[str]
        ) -> typing.Optional[typing.Dict]:
            loader_key = f'__entity_loader_{self._project_name}_{entity_type_name}_{"|".join(props)}'
            # TODO: if only the requested props are requested at the database:
            # use different loaders for different combinations of requested props
            if loader_key not in info.context:
                info.context[loader_key] = aiodataloader.DataLoader(
                    get_entities_wrapper(props)
                )
            return await info.context[loader_key].load(id)

        async def resolver(parent, info, **kwargs):
            entity_id = kwargs["id"]
            props = self.__class__._get_requested_entity_props(info)
            return await load_entity(info, entity_id, props)

        return resolver

    def _post_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        async def post_entity(input: typing.Dict, props: typing.List[str]):
            return await self._data_manager.post_entity(entity_type_name, input, props)

        async def resolver(_, info, input):
            props = self.__class__._get_requested_entity_props(info)
            return await post_entity(input, props)

        return resolver

    def _put_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        async def put_entity(
            entity_id: int,
            input: typing.Dict,
            props: typing.List[str],
        ):
            return await self._data_manager.put_entity(
                entity_type_name, entity_id, input, props
            )

        async def resolver(_, info, id, input):
            props = self.__class__._get_requested_entity_props(info)
            return await put_entity(id, input, props)

        return resolver

    def _delete_entity_resolver_wrapper(
        self,
        entity_type_name: str,
    ):
        async def delete_entity(entity_id: int):
            return await self._data_manager.delete_entity(entity_type_name, entity_id)

        async def resolver(_, __, id):
            return await delete_entity(id)

        return resolver

    def _get_relation_resolver_wrapper(
        self,
        relation_type_name: str,
        inverse: bool = False,
    ):
        async def get_relations(keys: typing.List[str]):
            grouped_ids = {}
            for key in keys:
                (entity_type_name, entity_id__str) = key.split("|")
                if entity_type_name not in grouped_ids:
                    grouped_ids[entity_type_name] = []
                grouped_ids[entity_type_name].append(int(entity_id__str))
            grouped_data = {}
            for entity_type_name, entity_ids in grouped_ids.items():
                grouped_data[entity_type_name] = await self._data_manager.get_relations(
                    entity_type_name,
                    entity_ids,
                    relation_type_name,
                    inverse,
                )
            # dataloader expects sequence of objects or None following order of ids in ids
            results = []
            for key in keys:
                (entity_type_name, entity_id__str) = key.split("|")
                results.append(
                    grouped_data.get(entity_type_name).get(int(entity_id__str), [])
                )

            return results

        async def load_relation(
            info, entity_type_name: str, id: int
        ) -> typing.Optional[typing.Dict]:
            loader_key = (
                f"__relation_loader_{self._project_name}_{relation_type_name}_{inverse}"
            )
            if loader_key not in info.context:
                info.context[loader_key] = aiodataloader.DataLoader(get_relations)
            return await info.context[loader_key].load(f"{entity_type_name}|{id}")

        async def resolver(parent, info, **_):
            entity_type_name = info.parent_type.name.lower()
            entity_id = parent["id"]

            return await load_relation(info, entity_type_name, entity_id)

        return resolver

    def _calc_props(
        self,
        entity_or_relation: str,
        type_name: str,
        allowed_props: typing.List[str],
        add_id: bool = False,
        input: bool = False,
    ):
        if entity_or_relation == "entity":
            config = self._entity_types_config[type_name]["config"]
        else:
            config = self._relation_types_config[type_name]["config"]

        if add_id:
            props = [["id", "Int"]]
        else:
            props = []

        # TODO add properties which can contain multiple, values (sorted or unsorted)
        if "data" not in config:
            return props
        if "fields" not in config["data"]:
            return props

        for prop in config["data"]["fields"].values():
            if prop["system_name"] in allowed_props:
                prop_type = prop["type"]
                # Input types might differ from query types
                if (
                    input
                    and f"{prop_type}Input"
                    in self._additional_input_type_defs_dict.keys()
                ):
                    props.append([prop["system_name"], f"{prop_type}Input"])
                else:
                    props.append([prop["system_name"], prop_type])
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
            if (
                prop_type in additonal_type_defs_dict
                and prop_type not in type_defs_dict
            ):
                type_defs_dict[prop_type] = additonal_type_defs_dict[prop_type]

    def _add_get_source_schema_parts(self):
        # TODO: check source permissions using config
        source_entity_names = [
            etn
            for etn in self._entity_types_config
            if (
                "source" in self._entity_types_config[etn]["config"]
                and self._entity_types_config[etn]["config"]["source"]
            )
        ]
        if not source_entity_names:
            return

        self._scalars.add("scalar JSON")
        self._unions.add(
            f'union Source_entity_types = {" | ".join([first_cap(sen) for sen in source_entity_names])}'
        )
        self._type_defs_dict["Source_"] = [
            ["id", "Int!"],
            ["properties", "[String!]!"],
            ["source_props", "JSON"],
            ["entity", "Source_entity_types"],
        ]

    def _add_get_entity_schema_parts(self) -> None:
        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "entities",
            "data",
            "get",
        )
        for etn, allowed_props in allowed.items():
            self._type_defs_dict["Query"].append(
                [f"get{first_cap(etn)}(id: Int!)", first_cap(etn)]
            )
            self._query_dict["Query"].set_field(
                f"get{first_cap(etn)}",
                self._get_entity_resolver_wrapper(etn),
            )
            # Needed for relation and source resolvers
            self._query_dict[first_cap(etn)] = ariadne.ObjectType(first_cap(etn))

            props = self._calc_props(
                "entity",
                etn,
                allowed_props,
                add_id=True,
            )
            self._add_additional_props(
                props,
            )
            self._type_defs_dict[first_cap(etn)] = props

            if "Source_" in self._type_defs_dict:
                self._type_defs_dict[first_cap(etn)].append(["_source_", "[Source_!]!"])
                self._query_dict[first_cap(etn)].set_field(
                    "_source_", self._get_relation_resolver_wrapper("_source_")
                )

    def _add_mutation_to_type_defs_and_query(self) -> None:
        if "Mutation" not in self._type_defs_dict:
            self._type_defs_dict["Mutation"] = []
        if "Mutation" not in self._query_dict:
            self._query_dict["Mutation"] = ariadne.MutationType()

    def _add_post_put_entity_schema_parts(self, perm: str) -> None:
        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "entities",
            "data",
            perm,
        )

        if not allowed:
            return

        self._add_mutation_to_type_defs_and_query()
        for etn in allowed:
            self._type_defs_dict["Mutation"].append(
                [
                    (
                        f"{perm}{first_cap(etn)}("
                        f'{"id: Int!, " if perm == "put" else ""}'
                        f"input: {first_cap(perm)}{first_cap(etn)}Input"
                        f")"
                    ),
                    first_cap(etn),
                ]
            )
            self._query_dict["Mutation"].set_field(
                f"{perm}{first_cap(etn)}",
                getattr(self, f"_{perm}_entity_resolver_wrapper")(etn),
            )

            # TODO: don't accept stringified JSON, use detailed GraphQL schema
            self._input_type_defs_dict[f"{first_cap(perm)}{first_cap(etn)}Input"] = [
                ["entity", "String"]
            ]

    def _add_delete_entity_schema_parts(self) -> None:
        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "entities",
            "data",
            "delete",
        )
        for etn in allowed:
            self._type_defs_dict["Mutation"].append(
                [
                    f"delete{first_cap(etn)}(id: Int!)",
                    # Separate model, as only id is returned
                    f"Delete{first_cap(etn)}",
                ]
            )
            self._query_dict["Mutation"].set_field(
                f"delete{first_cap(etn)}",
                self._delete_entity_resolver_wrapper(etn),
            )
            # self._query_dict[f"Delete{first_cap(etn)}"] = ariadne.ObjectType(f"Delete{first_cap(etn)}")
            self._type_defs_dict[f"Delete{first_cap(etn)}"] = [["id", "Int"]]

    def _add_get_relation_schema_parts(self) -> None:
        # TODO: cardinality
        # TODO: bidirectional relations
        allowed = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "relations",
            "data",
            "get",
        )
        for rtn, allowed_props in allowed.items():
            # Source
            if rtn == "_source_":
                continue
            domain_names = self._relation_types_config[rtn]["domain_names"]
            range_names = self._relation_types_config[rtn]["range_names"]

            for domain_name in domain_names:
                self._type_defs_dict[first_cap(domain_name)].append(
                    [f"r_{rtn}_s", f"[getR_{rtn}!]!"]
                )
                self._query_dict[first_cap(domain_name)].set_field(
                    f"r_{rtn}_s", self._get_relation_resolver_wrapper(rtn)
                )
            for range_name in range_names:
                self._type_defs_dict[first_cap(range_name)].append(
                    [f"ri_{rtn}_s", f"[getRi_{rtn}!]!"]
                )
                self._query_dict[first_cap(range_name)].set_field(
                    f"ri_{rtn}_s", self._get_relation_resolver_wrapper(rtn, True)
                )

            props = self._calc_props(
                "relation",
                rtn,
                allowed_props,
                add_id=True,
            )
            self._add_additional_props(props)
            if "Source_" in self._type_defs_dict:
                props.append(["_source_", "[Source_!]!"])

            self._unions.add(
                f'union Ri_{rtn}_domain = {" | ".join([first_cap(dn) for dn in domain_names])}'
            )
            self._unions.add(
                f'union R_{rtn}_range = {" | ".join([first_cap(rn) for rn in range_names])}'
            )

            self._type_defs_dict[f"getR_{rtn}"] = props + [["entity", f"R_{rtn}_range"]]
            self._type_defs_dict[f"getRi_{rtn}"] = props + [
                ["entity", f"Ri_{rtn}_domain"]
            ]

    def _add_post_put_entity_relation_schema_parts(self, perm) -> None:
        # TODO: fix so perm actually means relation permission, not entity permission
        allowed_relations = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "relations",
            "data",
            perm,
        )
        allowed_entities = allowed_entities_or_relations_and_properties(
            self._user,
            self._project_name,
            "entities",
            "data",
            perm,
        )

        if not allowed_relations or not allowed_entities:
            return

        for rtn in allowed_relations:
            # Source
            if rtn == "_source_":
                continue
            domain_names = self._relation_types_config[rtn]["domain_names"]
            range_names = self._relation_types_config[rtn]["range_names"]

            for domain_name in domain_names:
                if domain_name not in allowed_entities:
                    continue
                self._input_type_defs_dict[
                    f"{first_cap(perm)}{first_cap(domain_name)}Input"
                ].append(
                    [f"r_{rtn}_s", "String"],
                )
            for range_name in range_names:
                if range_name not in allowed_entities:
                    continue
                self._input_type_defs_dict[
                    f"{first_cap(perm)}{first_cap(range_name)}Input"
                ].append(
                    [f"ri_{rtn}_s", "String"],
                )

    # TODO: reset cache when project is updated or user permissions have been updated
    @aiocache.cached(key_builder=create_schema_key_builder)
    async def create_schema(self):
        self._entity_types_config = await self._config_manager.get_entity_types_config(
            self._project_name
        )
        self._relation_types_config = (
            await self._config_manager.get_relation_types_config(self._project_name)
        )

        self._type_defs_dict = {
            "Query": [],
            # Return object for post, put, delete operations
            "IdObject": [
                ["id", "Int!"],
            ],
        }
        self._input_type_defs_dict = {}
        self._unions = set()
        self._scalars = set()

        self._query_dict = {"Query": ariadne.QueryType()}

        self._additional_type_defs_dict = {
            "Geometry": [
                ["type", "String!"],
                ["coordinates", "[Float!]!"],
            ],
        }
        self._additional_input_type_defs_dict = {
            "GeometryInput": [
                ["type", "String!"],
                ["coordinates", "[Float!]!"],
            ],
        }

        # First add source parts: type_defs_dict['_Source'] is checked in other schema_pars adders
        self._add_get_source_schema_parts()

        # Then add entity parts: relations are later added to these
        self._add_get_entity_schema_parts()

        self._add_post_put_entity_schema_parts("post")
        self._add_post_put_entity_schema_parts("put")
        self._add_delete_entity_schema_parts()

        self._add_get_relation_schema_parts()

        self._add_post_put_entity_relation_schema_parts("post")
        self._add_post_put_entity_relation_schema_parts("put")

        type_defs_array = [
            construct_def("type", type, props)
            for type, props in self._type_defs_dict.items()
        ]
        input_type_defs_array = [
            construct_def("input", type, props)
            for type, props in self._input_type_defs_dict.items()
        ]
        type_defs = ariadne.gql(
            "\n".join(list(self._scalars))
            + "\n\n"
            + "\n".join(list(self._unions))
            + "\n\n"
            + "\n\n".join(input_type_defs_array)
            + "\n\n"
            + "\n\n".join(type_defs_array)
        )

        return ariadne.make_executable_schema(type_defs, *self._query_dict.values())
