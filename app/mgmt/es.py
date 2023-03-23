import datetime
import itertools
import typing

import aiocache
import elasticsearch
import fastapi
import roman
import starlette
import typing_extensions

from app.auth.permission import (has_entity_type_permission,
                                 require_entity_type_permission)
from app.cache.core import self_project_name_entity_type_name_key_builder
from app.config import ELASTICSEARCH
from app.es.base import AGG_SIZE, DEFAULT_FROM, DEFAULT_SIZE, MAX_INT
from app.mgmt.config import ConfigManager
from app.models.auth import UserWithPermissions
from app.utils import dtu


class ElasticsearchManager:
    def __init__(
        self,
        project_name: str,
        entity_type_name: str,
        request: starlette.requests.Request,
        user: UserWithPermissions = None,
    ):
        self._request = request
        self._project_name = project_name
        self._entity_type_name = entity_type_name
        self._config_manager = ConfigManager(request, user)
        self._user = user
        self._es = request.app.state.es

    @aiocache.cached(key_builder=self_project_name_entity_type_name_key_builder)
    async def _get_project_id(self):
        return await self._config_manager.get_project_id_by_name(self._project_name)

    @aiocache.cached(key_builder=self_project_name_entity_type_name_key_builder)
    async def _get_entity_type_id(self):
        return await self._config_manager.get_entity_type_id_by_name(
            self._project_name,
            self._entity_type_name,
        )

    @aiocache.cached(key_builder=self_project_name_entity_type_name_key_builder)
    async def _get_entity_types_config(self):
        return await self._config_manager.get_entity_types_config(self._project_name)

    @aiocache.cached(key_builder=self_project_name_entity_type_name_key_builder)
    async def _get_alias_name(self):
        return f'{ELASTICSEARCH["prefix"]}_{dtu(await self._get_entity_type_id())}'

    @aiocache.cached(key_builder=self_project_name_entity_type_name_key_builder)
    async def _get_es_config(self):
        entity_type_config = (await self._get_entity_types_config())[
            self._entity_type_name
        ]["config"]

        base_defs = {
            field["system_name"]: field
            for field in entity_type_config["es_data"]["fields"]
        }

        column_defs = {}
        filter_defs = {}
        if "es_display" in entity_type_config:
            for column_def in entity_type_config["es_display"]["columns"]:
                column_system_name = column_def["column"].replace("$", "")
                if "sub_field" in column_def:
                    column_defs[
                        f"{column_system_name}.{column_def['sub_field']}"
                    ] = column_def
                else:
                    column_defs[column_system_name] = column_def

            # TODO: filter view permissions
            for filter_section in entity_type_config["es_display"]["filters"]:
                for filter_def in filter_section["filters"]:
                    filter_system_name = filter_def["filter"].replace("$", "")
                    filter_defs[filter_system_name] = filter_def
        # Add edit_relation_title filter and column for entity overview pages without config
        else:
            column_defs["edit_relation_title"] = {
                "column": "$edit_relation_title",
                "display_name": "Id and title",
                "sortable": True,
                "main_link": True,
            }
            filter_defs["edit_relation_title"] = {
                "filter": "$edit_relation_title",
            }

        return {
            "base": base_defs,
            "columns": column_defs,
            "filters": filter_defs,
        }

    @staticmethod
    def _get_sorting(
        es_config: typing.Dict,
        sort_by: str = None,
        sort_order: typing_extensions.Literal["asc", "desc"] = "asc",
    ) -> typing.Dict[str, str]:
        # TODO: sorting on multiple columns
        if sort_by is None:
            for (key, value) in es_config["columns"].items():
                if value["sortable"]:
                    sort_by = key
                    break
        if sort_order is None:
            sort_order = "asc"

        return {
            "sort_by": sort_by,
            "sort_order": sort_order,
        }

    @staticmethod
    def _construct_sort(
        es_config: typing.Dict,
        sort_by: str,
        sort_order: typing_extensions.Literal["asc", "desc"],
    ) -> typing.List[typing.Dict[str, str]]:
        # TODO: sorting on multiple columns

        # Subfields
        if "." in sort_by:
            type = es_config["columns"][sort_by]["sub_field_type"]
        else:
            type = es_config["base"][sort_by]["type"]

        if type == "edtf":
            if sort_order == "asc":
                sort_by = f"{sort_by}.lower"
            else:
                sort_by = f"{sort_by}.upper"
            return [{sort_by: sort_order}]
        if type == "uncertain_centuries":
            if sort_order == "asc":
                mode = "min"
            else:
                mode = "max"
            return [
                {
                    f"{sort_by}.numeric": {
                        "mode": mode,
                        "order": sort_order,
                        "nested": {"path": sort_by},
                    }
                }
            ]
        if type == "nested" or type == "nested_multi_type" or type == "nested_flatten":
            if sort_order == "asc":
                mode = "min"
            else:
                mode = "max"
            return [
                {
                    f"{sort_by}.value.normalized_keyword": {
                        "mode": mode,
                        "order": sort_order,
                        "nested": {"path": sort_by},
                    }
                }
            ]
        if type == "text" or type == "[text]":
            return [{f"{sort_by}.normalized_keyword": sort_order}]

        return [{sort_by: sort_order}]

    @staticmethod
    def _construct_fields(
        es_config: typing.Dict,
    ) -> typing.List[str]:
        fields = []
        for system_name in es_config["columns"]:
            # Subfields
            if "." in system_name:
                type = es_config["columns"][system_name]["sub_field_type"]
            else:
                type = es_config["base"][system_name]["type"]
            if (
                type == "nested"
                or type == "nested_multi_type"
                or type == "nested_flatten"
            ):
                fields.append(f"{system_name}.entity_type_name")
                fields.append(f"{system_name}.id")
                fields.append(f"{system_name}.value")
                continue
            if type == "uncertain_centuries":
                fields.append(f"{system_name}.display")
                fields.append(f"{system_name}.withoutUncertain")
                fields.append(f"{system_name}.numeric")
                continue
            if type == "text" or type == "[text]":
                fields.append(system_name)
                continue
            if type == "edtf":
                fields.append(f"{system_name}.text")
                fields.append(f"{system_name}.lower")
                fields.append(f"{system_name}.upper")
                continue
            else:
                raise Exception(f"Field of type {type} not yet implemented")
        return fields

    @staticmethod
    def _construct_query(
        es_config: typing.Dict,
        filters: typing.Dict = None,
        global_aggs: bool = False,
        aggregation: str = None,
        suggest_field: str = None,
        suggest_value: str = None,
    ) -> typing.Dict:
        """
        Construct the query for an elasticsearch query.
        global_aggs indicate the query is being constructed for the retrieval of aggregations.
        In this case, nested filters, dropdown filters should be excluded as these will be added as filters to the separate aggregations to allow for a better doc_count.
        aggregation indicates the construction of a filter for an aggregation.
        All query parts included in the global query don't need to be added here. All other filters (except for the aggregation itself) should be added.
        suggest_field indicates the query is being contructed for the creation of a suggestion list of an aggregation dropdown menu.
        """
        query = None
        # TODO: more elegant fix for unwanted nested aggregations
        if suggest_field is not None:
            type = es_config["base"][suggest_field]["type"]
            if (
                type == "nested"
                or type == "nested_multi_type"
                or type == "nested_flatten"
            ):
                queryPart = {
                    "nested": {
                        "path": suggest_field,
                        "query": {
                            "match_bool_prefix": {
                                f"{suggest_field}.value.normalized_text": {
                                    "query": suggest_value,
                                    "operator": "and",
                                },
                            },
                        },
                    },
                }
            elif type == "uncertain_centuries":
                queryPart = {
                    "nested": {
                        "path": suggest_field,
                        "query": {
                            "match_bool_prefix": {
                                f"{suggest_field}.withoutUncertain": {
                                    "query": suggest_value,
                                    "operator": "and",
                                },
                            },
                        },
                    },
                }
            elif (type == "text" or type == "[text]") and es_config["filters"][
                suggest_field
            ]["type"] == "dropdown":
                queryPart = {
                    "match_bool_prefix": {
                        f"{suggest_field}.normalized_text": {
                            "query": suggest_value,
                            "operator": "and",
                        },
                    },
                }
            else:
                raise Exception(
                    f"Aggregation suggestion filter of type {type} not yet implemented"
                )

            query = {
                "bool": {
                    "filter": [
                        queryPart,
                    ],
                },
            }

        if filters is None:
            return query

        if query is None:
            query = {
                "bool": {},
            }

        for filter_key, filter_values in filters.items():
            type = es_config["base"][filter_key]["type"]

            if (
                type == "nested"
                and "type" in es_config["filters"][filter_key]
                and es_config["filters"][filter_key]["type"] == "nested_present"
            ):
                if global_aggs:
                    continue
                if aggregation == filter_key:
                    continue

                if filter_values["key"] == 0:
                    occur = "must_not"
                else:
                    occur = "should"
                queryPart = {
                    "bool": {
                        occur: {
                            "nested": {
                                "path": filter_key,
                                "query": {
                                    "exists": {
                                        "field": filter_key,
                                    },
                                },
                            }
                        }
                    }
                }
                if not "must" in query["bool"]:
                    query["bool"]["must"] = []
                query["bool"]["must"].append(queryPart)
                continue
            if (
                type == "nested"
                or type == "nested_multi_type"
                or type == "nested_flatten"
                or type == "uncertain_centuries"
            ):
                if global_aggs:
                    continue
                if aggregation == filter_key:
                    continue

                queryPart = {
                    "nested": {
                        "path": filter_key,
                        "query": {"terms": {}},
                    }
                }
                if type == "nested" or type == "nested_flatten":
                    queryPart["nested"]["query"]["terms"][
                        f"{filter_key}.id"
                    ] = filter_values
                elif type == "nested_multi_type":
                    queryPart["nested"]["query"]["terms"][
                        f"{filter_key}.type_id"
                    ] = filter_values
                elif type == "uncertain_centuries":
                    queryPart["nested"]["query"]["terms"][
                        f"{filter_key}.withoutUncertain"
                    ] = filter_values
                if not "filter" in query["bool"]:
                    query["bool"]["filter"] = []
                query["bool"]["filter"].append(queryPart)
                continue
            if type == "edtf" or type == "edtf_interval":
                # To be included in global filter for aggregations
                if aggregation is not None:
                    continue
                queryPart = {"range": {}}
                queryPart["range"][f"{filter_key}.year_range"] = {}
                if filter_values[0] is not None:
                    queryPart["range"][f"{filter_key}.year_range"][
                        "gte"
                    ] = filter_values[0]
                if filter_values[1] is not None:
                    queryPart["range"][f"{filter_key}.year_range"][
                        "lte"
                    ] = filter_values[1]
                if not "filter" in query["bool"]:
                    query["bool"]["filter"] = []
                query["bool"]["filter"].append(queryPart)
                continue
            if type == "text" or type == "[text]":
                if es_config["filters"][filter_key]["type"] == "dropdown":
                    if global_aggs:
                        continue
                    if aggregation == filter_key:
                        continue
                    queryPart = {"terms": {f"{filter_key}.keyword": filter_values}}
                    if not "filter" in query["bool"]:
                        query["bool"]["filter"] = []
                    query["bool"]["filter"].append(queryPart)
                    continue
                # To be included in global filter for aggregations
                if aggregation is not None:
                    continue
                queryPart = {
                    "match": {
                        filter_key: {
                            "query": filter_values,
                            "operator": "and",
                        }
                    }
                }
                if not "must" in query["bool"]:
                    query["bool"]["must"] = []
                query["bool"]["must"].append(queryPart)
                continue
            raise Exception(f"Filter of type {type} not yet implemented")

        if len(query["bool"].keys()) == 0:
            return None

        return query

    @staticmethod
    def _extract_results(
        es_config: typing.Dict,
        raw_result: typing.Dict,
        sorting: typing.Dict[str, str],
    ) -> typing.List:
        results = []
        for raw_result in raw_result["hits"]["hits"]:
            result = {"_id": raw_result["_id"]}
            fields = raw_result["fields"]
            # responses for fields always return array
            # https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-fields.html#search-fields-response
            for column_key in es_config["columns"].keys():
                # Subfields
                if "." in column_key:
                    type = es_config["columns"][column_key]["sub_field_type"]
                else:
                    type = es_config["base"][column_key]["type"]

                if type == "edtf":
                    if f"{column_key}.text" not in fields:
                        continue
                else:
                    if column_key not in fields:
                        continue
                if (
                    type == "nested"
                    or type == "nested_multi_type"
                    or type == "nested_flatten"
                ):
                    result[column_key] = [
                        {k: v[0] for k, v in value.items()}
                        for value in fields[column_key]
                    ]
                    if sorting["sort_by"] == column_key:
                        result[column_key].sort(
                            key=lambda item: item["value"],
                            reverse=sorting["sort_order"] == "desc",
                        )
                    continue
                if type == "uncertain_centuries":
                    result[column_key] = [
                        {k: v[0] for k, v in value.items()}
                        for value in fields[column_key]
                    ]
                    if sorting["sort_by"] == column_key:
                        result[column_key].sort(
                            key=lambda item: item["numeric"],
                            reverse=sorting["sort_order"] == "desc",
                        )
                    continue
                if type == "text":
                    result[column_key] = fields[column_key][0]
                    continue
                if type == "[text]":
                    result[column_key] = fields[column_key]
                    continue
                if type == "edtf":
                    result[column_key] = fields[f"{column_key}.text"][0]
                    continue
                else:
                    raise Exception(
                        f"Extraction of fields of type {type} not yet implemented"
                    )
            results.append(result)
        return results

    @staticmethod
    def _construct_full_range_aggs(
        es_config: typing.Dict,
    ) -> typing.Dict:
        aggs = {}
        for filter_key in es_config["filters"]:
            if (
                es_config["base"][filter_key]["type"] == "edtf"
                or es_config["base"][filter_key]["type"] == "edtf_interval"
            ):
                if es_config["filters"][filter_key]["type"] == "histogram_slider":
                    aggs[f"{filter_key}_min"] = {
                        "min": {"field": f"{filter_key}.lower"}
                    }
                    aggs[f"{filter_key}_max"] = {
                        "max": {"field": f"{filter_key}.upper"}
                    }

        return aggs

    @staticmethod
    def _extract_full_range_aggs(raw_result: typing.Dict = None) -> typing.Dict:
        aggregations = raw_result["aggregations"]
        results = {}
        for agg_key, agg_values in aggregations.items():
            if "value_as_string" in agg_values:
                results[agg_key] = datetime.datetime.strptime(
                    agg_values["value_as_string"], "%Y-%m-%dT%H:%M:%S.%f%z"
                ).year
            else:
                results[agg_key] = None

        return results

    @staticmethod
    def _construct_filter_agg(
        es_config: typing.Dict,
        filter_key: str,
        agg_construct: typing.Dict,
        filters: typing.Dict = None,
        suggest_field: str = None,
        suggest_value: str = None,
    ) -> typing.Dict:
        filter = ElasticsearchManager._construct_query(
            es_config,
            filters,
            aggregation=filter_key,
            suggest_field=suggest_field,
            suggest_value=suggest_value,
        )
        if filter:
            return {
                "filter": filter,
                "aggs": {
                    filter_key: agg_construct,
                },
            }
        return agg_construct

    @staticmethod
    def _construct_suggest_agg(
        es_config: typing.Dict,
        filters: typing.Dict = None,
        suggest_field: str = None,
        suggest_value: str = None,
    ):
        type = es_config["base"][suggest_field]["type"]
        if type == "nested" or type == "nested_flatten":
            agg_construct = {
                "nested": {
                    "path": suggest_field,
                },
                "aggs": {
                    "id_value": {
                        "terms": {
                            "field": f"{suggest_field}.id_value.keyword",
                            "size": AGG_SIZE,
                        },
                        "aggs": {
                            "normalized": {
                                "terms": {
                                    "field": f"{suggest_field}.value.normalized_keyword"
                                }
                            },
                            "reverse_nested": {
                                "reverse_nested": {},
                            },
                        },
                    },
                },
            }
            # TODO: find more generic way to do this
            if suggest_field == "edit_relation_title":
                agg_construct["aggs"]["id_value"]["terms"]["order"] = {"id": "asc"}
                agg_construct["aggs"]["id_value"]["aggs"]["id"] = {
                    "sum": {
                        "field": "edit_relation_title.id",
                    },
                }
            if suggest_value == "":
                return {suggest_field: agg_construct}
            return {
                suggest_field: ElasticsearchManager._construct_filter_agg(
                    es_config,
                    suggest_field,
                    agg_construct,
                    filters,
                    suggest_field,
                    suggest_value,
                ),
            }
        if type == "nested_multi_type":
            return {
                suggest_field: ElasticsearchManager._construct_filter_agg(
                    es_config,
                    suggest_field,
                    {
                        "nested": {
                            "path": suggest_field,
                        },
                        "aggs": {
                            "type_id_value": {
                                "terms": {
                                    "field": f"{suggest_field}.type_id_value.keyword",
                                    "size": AGG_SIZE,
                                },
                                "aggs": {
                                    "normalized": {
                                        "terms": {
                                            "field": f"{suggest_field}.value.normalized_keyword"
                                        }
                                    },
                                    "reverse_nested": {
                                        "reverse_nested": {},
                                    },
                                },
                            },
                        },
                    },
                    filters,
                    suggest_field,
                    suggest_value,
                ),
            }
        if type == "uncertain_centuries":
            return {
                suggest_field: ElasticsearchManager._construct_filter_agg(
                    es_config,
                    suggest_field,
                    {
                        "nested": {
                            "path": suggest_field,
                        },
                        "aggs": {
                            "withoutUncertain": {
                                "terms": {
                                    "field": f"{suggest_field}.withoutUncertain",
                                    "size": AGG_SIZE,
                                },
                                "aggs": {
                                    "normalized": {
                                        "terms": {
                                            "field": f"{suggest_field}.withoutUncertain.normalized_keyword"
                                        }
                                    },
                                    "reverse_nested": {
                                        "reverse_nested": {},
                                    },
                                },
                            },
                        },
                    },
                    filters,
                    suggest_field,
                    suggest_value,
                ),
            }
        if type == "text" or type == "[text]":
            return {
                suggest_field: ElasticsearchManager._construct_filter_agg(
                    es_config,
                    suggest_field,
                    {
                        "terms": {
                            "field": f"{suggest_field}.keyword",
                            "size": AGG_SIZE,
                        },
                        "aggs": {
                            "normalized": {
                                "terms": {
                                    "field": f"{suggest_field}.normalized_keyword"
                                }
                            }
                        },
                    },
                    filters,
                    suggest_field,
                    suggest_value,
                ),
            }

        raise Exception(f"Aggregations suggestion of type {type} not yet implemented")

    @staticmethod
    def _construct_aggs(
        es_config: typing.Dict,
        filters: typing.Dict = None,
        full_range_aggs: typing.Dict = None,
    ) -> typing.Dict:
        aggs = {}
        for filter_key in es_config["filters"]:
            type = es_config["base"][filter_key]["type"]
            if type == "nested" or type == "nested_flatten":
                if (
                    "type" in es_config["filters"][filter_key]
                    and es_config["filters"][filter_key]["type"] == "nested_present"
                ):
                    aggs[filter_key] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filter_key,
                        {
                            "nested": {
                                "path": filter_key,
                            },
                        },
                        filters,
                    )
                    aggs[
                        f"{filter_key}_missing"
                    ] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filter_key,
                        {
                            "missing": {
                                "field": filter_key,
                            },
                        },
                        filters,
                    )
                    continue
                nested_agg = {
                    "nested": {
                        "path": filter_key,
                    },
                    "aggs": {
                        "id_value": {
                            "terms": {
                                "field": f"{filter_key}.id_value.keyword",
                                "size": MAX_INT,
                                "min_doc_count": 0,
                            },
                            "aggs": {
                                "reverse_nested": {
                                    "reverse_nested": {},
                                },
                            },
                        },
                    },
                }
                aggs[filter_key] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filter_key,
                    nested_agg,
                    filters,
                )
                continue
            if type == "nested_multi_type":
                nested_multi_type_agg = {
                    "nested": {
                        "path": filter_key,
                    },
                    "aggs": {
                        "type_id_value": {
                            "terms": {
                                "field": f"{filter_key}.type_id_value.keyword",
                                "size": MAX_INT,
                                "min_doc_count": 0,
                            },
                            "aggs": {
                                "reverse_nested": {
                                    "reverse_nested": {},
                                },
                            },
                        },
                    },
                }
                aggs[filter_key] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filter_key,
                    nested_multi_type_agg,
                    filters,
                )
                continue
            if type == "uncertain_centuries":
                uncertain_centuries_agg = {
                    "nested": {
                        "path": filter_key,
                    },
                    "aggs": {
                        "withoutUncertain": {
                            "terms": {
                                "field": f"{filter_key}.withoutUncertain",
                                "size": MAX_INT,
                                "min_doc_count": 0,
                            },
                            "aggs": {
                                "reverse_nested": {
                                    "reverse_nested": {},
                                },
                            },
                        },
                    },
                }
                aggs[filter_key] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filter_key,
                    uncertain_centuries_agg,
                    filters,
                )
                continue
            if type == "text" or type == "[text]":
                if es_config["filters"][filter_key]["type"] == "dropdown":
                    dropdown_agg = {
                        "terms": {
                            "field": f"{filter_key}.keyword",
                            "size": MAX_INT,
                            "min_doc_count": 0,
                        },
                    }
                    aggs[filter_key] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filter_key,
                        dropdown_agg,
                        filters,
                    )
                    continue
                if es_config["filters"][filter_key]["type"] == "autocomplete":
                    continue
            if type == "edtf" or type == "edtf_interval":
                if es_config["filters"][filter_key]["type"] == "histogram_slider":
                    aggs[
                        f"{filter_key}_hist"
                    ] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filter_key,
                        {
                            "histogram": {
                                "field": f"{filter_key}.year_range",
                                "interval": es_config["filters"][filter_key][
                                    "interval"
                                ],
                                "extended_bounds": {
                                    "min": full_range_aggs[f"{filter_key}_min"],
                                    "max": full_range_aggs[f"{filter_key}_max"],
                                },
                                "min_doc_count": 0,
                            },
                        },
                        filters,
                    )
                    continue

            raise Exception(
                f"Filter {es_config['filters'][filter_key]['type']} of type {type} not yet implemented"
            )
        return aggs

    @staticmethod
    def _extract_agg(
        buckets: typing.List[typing.Dict],
        key_method: callable,
        value_method: callable,
        sort_value_method: callable = None,
        suggest: bool = False,
        filter_values: typing.List = None,
    ) -> typing.List[typing.Dict]:
        count_method = (
            lambda bucket: bucket["reverse_nested"]["doc_count"]
            if "reverse_nested" in bucket
            else bucket["doc_count"]
        )

        if sort_value_method is None:
            buckets = [
                {
                    "key": key_method(bucket),
                    "value": value_method(bucket),
                    "count": count_method(bucket),
                }
                for bucket in buckets
            ]
        else:
            buckets = [
                {
                    "key": key_method(bucket),
                    "value": value_method(bucket),
                    "count": count_method(bucket),
                    "sort_value": sort_value_method(bucket),
                }
                for bucket in buckets
            ]

        if sort_value_method:
            buckets.sort(key=lambda bucket: bucket["sort_value"])

        if suggest:
            return [
                {
                    "key": bucket["key"],
                    "value": bucket["value"],
                    "count": bucket["count"],
                }
                for bucket in buckets
            ]

        # Limit number of aggregations and place selected aggregations first
        if filter_values is None:
            filter_values = []
        number_of_filter_values = len(filter_values)
        number_of_additional_buckets = max(AGG_SIZE - number_of_filter_values, 0)
        filter_values_set = set(filter_values)
        selected_buckets = []
        additional_buckets = []
        skip_selected = False
        skip_additional = False
        for bucket in buckets:
            if not skip_selected:
                if bucket["key"] in filter_values_set:
                    selected_buckets.append(bucket)
                    if len(selected_buckets) == number_of_filter_values:
                        skip_selected = True
                elif not skip_additional:
                    if bucket["count"] > 0:
                        additional_buckets.append(bucket)
                        if len(additional_buckets) == number_of_additional_buckets:
                            skip_additional = True
            elif not skip_additional:
                # cannot be a filter value, since selected is being skipped
                if bucket["count"] > 0:
                    additional_buckets.append(bucket)
                    if len(additional_buckets) == number_of_additional_buckets:
                        skip_additional = True
            if skip_selected and skip_additional:
                break

        return [
            {
                "key": bucket["key"],
                "value": bucket["value"],
                "count": bucket["count"],
            }
            for bucket in itertools.chain(selected_buckets, additional_buckets)
        ]

    @staticmethod
    def _has_filtered_aggregation(
        es_config: typing.Dict,
        filter_key: str,
    ) -> bool:
        type = es_config["base"][filter_key]["type"]
        if type in [
            "nested",
            "nested_multi_type",
            "nested_flatten",
            "uncertain_centuries",
            "edtf",
            "edtf_interval",
        ]:
            return True
        if type == "text" or type == "[text]":
            if es_config["filters"][filter_key]["type"] == "dropdown":
                return True
        return False

    @staticmethod
    def _in_filtered_aggregation(
        es_config: typing.Dict,
        filter_key: str,
    ) -> bool:
        type = es_config["base"][filter_key]["type"]
        if type in [
            "nested",
            "nested_multi_type",
            "nested_flatten",
            "uncertain_centuries",
        ]:
            return True
        if type == "text" or type == "[text]":
            if es_config["filters"][filter_key]["type"] == "dropdown":
                return True
        return False

    @staticmethod
    def _filter_suggest_aggs(
        buckets: typing.List[typing.Dict],
        suggest_value: str,
    ) -> typing.List[typing.Dict]:
        # Filter out unwanted results (added because of nested aggregation)
        filtered_buckets = []
        suggest_split = suggest_value.split(" ")
        suggest_terms = suggest_split[:-1]
        suggest_prefix = suggest_split[-1]
        for bucket in buckets:
            skip = False
            result_split = bucket["normalized"]["buckets"][0]["key"].split(" ")
            # First words: full term search
            for suggest_term in suggest_terms:
                if suggest_term not in result_split:
                    skip = True
                    break
            if skip:
                continue

            skip = True
            for result_part in result_split:
                if result_part.startswith(suggest_prefix):
                    skip = False
                    break
            if skip:
                continue

            del bucket["normalized"]
            filtered_buckets.append(bucket)

        return filtered_buckets

    @staticmethod
    def _extract_aggs(
        es_config: typing.Dict,
        raw_result: typing.Dict,
        filters: typing.Dict = None,
        full_range_aggs: typing.Dict = None,
        suggest_field: str = None,
        suggest_value: str = None,
    ) -> typing.Dict[str, typing.List]:
        aggregations = raw_result["aggregations"]
        results = {}
        # TODO: find more generic way to do this
        if suggest_field == "edit_relation_title":
            es_filters = {"edit_relation_title": {"sort": "id"}}
        else:
            es_filters = es_config["filters"]
        for filter_key in es_filters.keys():
            if suggest_field and filter_key != suggest_field:
                continue

            type = es_config["base"][filter_key]["type"]
            if "sort" not in es_filters[filter_key]:
                sort = None
            else:
                sort = es_filters[filter_key]["sort"]

            agg_key = filter_key
            if type == "edtf" or type == "edtf_interval":
                agg_key = f"{agg_key}_hist"

            if agg_key not in aggregations:
                continue

            agg_values = aggregations[agg_key]

            # Exctract values from subaggregation in case of aggregation suggestion
            if suggest_field:
                if suggest_value != "":
                    agg_values = agg_values[filter_key]
            # Exctract values from subaggregation in case of filtered aggregations
            elif filters is not None and ElasticsearchManager._has_filtered_aggregation(
                es_config, filter_key
            ):
                for usedfilter_key in filters:
                    # These cases don't lead to a filtered aggregation
                    if not ElasticsearchManager._in_filtered_aggregation(
                        es_config, usedfilter_key
                    ):
                        continue
                    # Only filtered aggregation different from current key lead to an actual filtered aggregation
                    if usedfilter_key != filter_key:
                        agg_values = agg_values[filter_key]
                        break

            if type == "nested" or type == "nested_flatten":
                if (
                    "type" in es_filters[filter_key]
                    and es_filters[filter_key]["type"] == "nested_present"
                ):
                    results[filter_key] = [
                        {
                            "key": 0,
                            "value": "No",
                            "count": aggregations[f"{filter_key}_missing"]["doc_count"],
                        },
                        {
                            "key": 1,
                            "value": "Yes",
                            "count": agg_values["doc_count"],
                        },
                    ]
                    continue

                kwargs = {
                    "buckets": agg_values["id_value"]["buckets"],
                    "key_method": lambda bucket: bucket["key"].split("|", maxsplit=1)[
                        0
                    ],
                    "value_method": lambda bucket: bucket["key"].split("|", maxsplit=1)[
                        1
                    ],
                }

                if suggest_field:
                    kwargs["suggest"] = True
                    kwargs["buckets"] = ElasticsearchManager._filter_suggest_aggs(
                        kwargs["buckets"], suggest_value
                    )
                if sort:
                    if sort == "alphabetically":
                        kwargs["sort_value_method"] = lambda bucket: bucket[
                            "key"
                        ].split("|", maxsplit=1)[1]
                    elif sort == "id":
                        kwargs["sort_value_method"] = lambda bucket: int(
                            bucket["key"].split("|", maxsplit=1)[0]
                        )
                    else:
                        raise Exception(
                            f"Sorting {sort} of filters of type {type} not yet implemented"
                        )
                if filters and filter_key in filters:
                    kwargs["filter_values"] = filters[filter_key]

                results[filter_key] = ElasticsearchManager._extract_agg(**kwargs)
                continue
            if type == "nested_multi_type":
                kwargs = {
                    "buckets": agg_values["type_id_value"]["buckets"],
                    "key_method": lambda bucket: "|".join(
                        bucket["key"].split("|", maxsplit=2)[:2]
                    ),
                    "value_method": lambda bucket: bucket["key"].split("|", maxsplit=2)[
                        2
                    ],
                }

                if suggest_field:
                    kwargs["suggest"] = True
                    kwargs["buckets"] = ElasticsearchManager._filter_suggest_aggs(
                        kwargs["buckets"], suggest_value
                    )
                if sort:
                    if sort == "alphabetically":
                        kwargs["sort_value_method"] = lambda bucket: bucket[
                            "key"
                        ].split("|", maxsplit=2)[2]
                    else:
                        raise Exception(
                            f"Sorting {sort} of filters of type {type} not yet implemented"
                        )

                results[filter_key] = ElasticsearchManager._extract_agg(**kwargs)
                continue
            if type == "uncertain_centuries":
                kwargs = {
                    "buckets": agg_values["withoutUncertain"]["buckets"],
                    "key_method": lambda bucket: bucket["key"],
                    "value_method": lambda bucket: bucket["key"],
                }

                if suggest_field:
                    kwargs["suggest"] = True
                    kwargs["buckets"] = ElasticsearchManager._filter_suggest_aggs(
                        kwargs["buckets"], suggest_value
                    )
                if sort:
                    if sort == "chronologically":
                        kwargs["sort_value_method"] = lambda bucket: roman.fromRoman(
                            bucket["key"]
                        )
                    else:
                        raise Exception(
                            f"Sorting {sort} of filters of type {type} not yet implemented"
                        )

                results[filter_key] = ElasticsearchManager._extract_agg(**kwargs)
                continue
            if type == "text" or type == "[text]":
                kwargs = {
                    "buckets": agg_values["buckets"],
                    "key_method": lambda bucket: bucket["key"],
                    "value_method": lambda bucket: bucket["key"],
                }

                if suggest_field:
                    kwargs["suggest"] = True
                    kwargs["buckets"] = ElasticsearchManager._filter_suggest_aggs(
                        kwargs["buckets"], suggest_value
                    )

                if sort:
                    if sort == "alphabetically":
                        kwargs["sort_value_method"] = lambda bucket: bucket["key"]
                    else:
                        raise Exception(
                            f"Sorting {sort} of filters of type {type} not yet implemented"
                        )

                results[filter_key] = ElasticsearchManager._extract_agg(**kwargs)
                continue
            if type == "edtf" or type == "edtf_interval":
                results[f"{filter_key}_hist"] = agg_values["buckets"]
                results[f"{filter_key}_min"] = full_range_aggs[f"{filter_key}_min"]
                results[f"{filter_key}_max"] = full_range_aggs[f"{filter_key}_max"]
                continue

            if sort:
                exception_message = (
                    f"Sorting {sort} of filters of type {type} not yet implemented"
                )
            else:
                exception_message = (
                    f"Sorting of filters of type {type} not yet implemented"
                )

            raise Exception(exception_message)

        return results

    async def search(self, body: typing.Dict) -> typing.Dict:
        require_entity_type_permission(
            self._user,
            self._project_name,
            self._entity_type_name,
            "es_data",
            "view",
        )
        es_config = await self._get_es_config()

        # Clean filters
        if body["filters"]:
            filters = {
                key: value
                for (key, value) in body["filters"].items()
                if value is not None
            }
            if not len(filters):
                filters = None
        else:
            filters = None

        # Min and max data ranges
        full_range_aggs = {}
        request_full_range_aggs = self.__class__._construct_full_range_aggs(es_config)
        if request_full_range_aggs:
            raw_aggs = await self._es.search(
                index=await self._get_alias_name(),
                body={
                    # only aggregation
                    "size": 0,
                    "aggs": request_full_range_aggs,
                },
            )
            full_range_aggs = self.__class__._extract_full_range_aggs(raw_aggs)

        # Aggregations
        request_aggs = self.__class__._construct_aggs(
            es_config,
            filters,
            full_range_aggs=full_range_aggs,
        )
        if request_aggs is not None:
            request_body = {
                # only aggregation
                "size": 0,
                "aggs": request_aggs,
            }

            request_query = self.__class__._construct_query(
                es_config,
                filters,
                global_aggs=True,
            )
            if request_query is not None:
                request_body["query"] = request_query

            raw_aggs = await self._es.search(
                index=await self._get_alias_name(),
                body=request_body,
            )

            aggs = self._extract_aggs(es_config, raw_aggs, filters, full_range_aggs)
        else:
            aggs = {}

        # Data
        request_body = {
            "_source": False,
            "track_total_hits": True,
        }

        if body["size"]:
            request_body["size"] = body["size"]
        else:
            request_body["size"] = DEFAULT_SIZE

        if body["page"]:
            request_body["from"] = (body["page"] - 1) * request_body["size"]
        else:
            request_body["from"] = DEFAULT_FROM

        sorting = self._get_sorting(
            es_config,
            body["sortBy"],
            body["sortOrder"],
        )
        request_body["sort"] = self._construct_sort(
            es_config,
            sorting["sort_by"],
            sorting["sort_order"],
        )

        request_body["fields"] = self._construct_fields(es_config)

        request_query = self._construct_query(es_config, filters)
        if request_query is not None:
            request_body["query"] = request_query

        raw_result = await self._es.search(
            index=await self._get_alias_name(),
            body=request_body,
        )
        results = self._extract_results(es_config, raw_result, sorting)

        result = {
            "sortBy": sorting["sort_by"],
            "sortOrder": sorting["sort_order"],
            "total": raw_result["hits"]["total"]["value"],
            "aggs": aggs,
            "results": results,
            "from": request_body["from"] + 1,
            "to": request_body["from"] + len(results),
        }
        return result

    async def suggest(self, body: typing.Dict) -> typing.Dict:
        es_config = await self._get_es_config()

        if es_config["filters"][body["field"]]["type"] != "autocomplete":
            raise fastapi.exceptions.HTTPException(
                status_code=404, detail="Suggest field not found"
            )

        request_body = {
            "_source": False,
            "suggest": {
                "autocomplete": {
                    "prefix": body["value"],
                    "completion": {
                        "field": f"{body['field']}.completion",
                        "skip_duplicates": True,
                        "size": 10,
                    },
                }
            },
        }

        raw_result = await self._es.search(
            index=await self._get_alias_name(),
            body=request_body,
        )
        return [
            suggestion["text"]
            for suggestion in raw_result["suggest"]["autocomplete"][0]["options"]
        ]

    async def get_normalized_value(self, value: str) -> str:
        indices_client = elasticsearch.client.IndicesClient(self._es)
        normalized = await indices_client.analyze(
            index=await self._get_alias_name(),
            body={
                "normalizer": "icu_normalizer",
                "text": value,
            },
        )
        return normalized["tokens"][0]["token"]

    async def aggregation_suggest(self, body: typing.Dict) -> typing.Dict:
        es_config = await self._get_es_config()

        # Clean filters
        if body["filters"]:
            filters = {
                key: value
                for (key, value) in body["filters"].items()
                if value is not None
            }
            if not len(filters):
                filters = None
        else:
            filters = None

        # Aggregations
        request_aggs = self.__class__._construct_suggest_agg(
            es_config,
            filters,
            suggest_field=body["field"],
            suggest_value=body["value"],
        )

        if request_aggs is not None:
            request_body = {
                # only aggregation
                "size": 0,
                "aggs": request_aggs,
            }

            request_query = self.__class__._construct_query(
                es_config,
                filters,
                global_aggs=True,
            )
            if request_query is not None:
                request_body["query"] = request_query

            raw_aggs = await self._es.search(
                index=await self._get_alias_name(),
                body=request_body,
            )

            normalized_value = await self.get_normalized_value(
                body["value"],
            )

            aggs = self._extract_aggs(
                es_config,
                raw_aggs,
                suggest_field=body["field"],
                suggest_value=normalized_value,
            )

            return aggs[body["field"]]
        else:
            return []
