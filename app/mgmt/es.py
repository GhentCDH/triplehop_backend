import datetime
import typing

import aiocache
import fastapi
import roman
import starlette
import typing_extensions

from app.cache.core import self_project_name_entity_type_name_key_builder
from app.config import ELASTICSEARCH
from app.es.base import DEFAULT_FROM, DEFAULT_SIZE, MAX_INT
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
        for column_def in entity_type_config["es_display"]["columns"]:
            column_system_name = column_def["column"].replace("$", "")
            if "sub_field" in column_def:
                column_defs[
                    f"{column_system_name}.{column_def['sub_field']}"
                ] = column_def
            else:
                column_defs[column_system_name] = column_def

        # TODO: filter view permissions
        filter_defs = {}
        for filter_section in entity_type_config["es_display"]["filters"]:
            for filter_def in filter_section["filters"]:
                filter_system_name = filter_def["filter"].replace("$", "")
                filter_defs[filter_system_name] = filter_def

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
        if type == "nested" or type == "nested_multi_type":
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
            if type == "nested" or type == "nested_multi_type":
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
    ) -> typing.Dict:
        """
        Construct the query for an elasticsearch query.
        global_aggs indicate the query is being constructed for the retrieval of aggregations.
        In this case, nested filters, dropdown filters should be excluded as these will be added as filters to the separate aggregations to allow for a better doc_count.
        aggregation indicates the construction of a filter for an aggregation.
        All query parts included in the global query don't need to be added here. All other filters (except for the aggregation itself) should be added.
        """
        if filters is None:
            return None

        query = {"bool": {}}
        for filterKey, filterValues in filters.items():
            type = es_config["base"][filterKey]["type"]
            if (
                type == "nested"
                or type == "nested_multi_type"
                or type == "uncertain_centuries"
            ):
                if (
                    type == "nested"
                    and "type" in es_config["filters"][filterKey]
                    and es_config["filters"][filterKey]["type"] == "nested_present"
                ):
                    # To be included in global filter for aggregations
                    if aggregation is not None:
                        continue
                    if filterValues["key"] == 0:
                        occur = "must_not"
                    else:
                        occur = "should"
                    queryPart = {
                        "bool": {
                            occur: {
                                "nested": {
                                    "path": filterKey,
                                    "query": {"exists": {"field": filterKey}},
                                }
                            }
                        }
                    }
                    if not "must" in query["bool"]:
                        query["bool"]["must"] = []
                    query["bool"]["must"].append(queryPart)
                    continue

                if global_aggs:
                    continue
                if aggregation == filterKey:
                    continue

                queryPart = {
                    "nested": {
                        "path": filterKey,
                        "query": {"terms": {}},
                    }
                }
                if type == "nested":
                    queryPart["nested"]["query"]["terms"][
                        f"{filterKey}.id"
                    ] = filterValues
                elif type == "nested_multi_type":
                    queryPart["nested"]["query"]["terms"][
                        f"{filterKey}.type_id"
                    ] = filterValues
                elif type == "uncertain_centuries":
                    queryPart["nested"]["query"]["terms"][
                        f"{filterKey}.withoutUncertain"
                    ] = filterValues
                if not "filter" in query["bool"]:
                    query["bool"]["filter"] = []
                query["bool"]["filter"].append(queryPart)
                continue
            if type == "edtf" or type == "edtf_interval":
                # To be included in global filter for aggregations
                if aggregation is not None:
                    continue
                queryPart = {"range": {}}
                queryPart["range"][f"{filterKey}.year_range"] = {}
                if filterValues[0] is not None:
                    queryPart["range"][f"{filterKey}.year_range"]["gte"] = filterValues[
                        0
                    ]
                if filterValues[1] is not None:
                    queryPart["range"][f"{filterKey}.year_range"]["lte"] = filterValues[
                        1
                    ]
                if not "filter" in query["bool"]:
                    query["bool"]["filter"] = []
                query["bool"]["filter"].append(queryPart)
                continue
            if type == "text" or type == "[text]":
                if es_config["filters"][filterKey]["type"] == "dropdown":
                    if global_aggs:
                        continue
                    if aggregation == filterKey:
                        continue
                    queryPart = {"terms": {f"{filterKey}.keyword": filterValues}}
                    if not "filter" in query["bool"]:
                        query["bool"]["filter"] = []
                    query["bool"]["filter"].append(queryPart)
                    continue
                # To be included in global filter for aggregations
                if aggregation is not None:
                    continue
                queryPart = {
                    "match": {
                        filterKey: {
                            "query": filterValues,
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
                if type == "nested" or type == "nested_multi_type":
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
        for filterKey in es_config["filters"]:
            if (
                es_config["base"][filterKey]["type"] == "edtf"
                or es_config["base"][filterKey]["type"] == "edtf_interval"
            ):
                if es_config["filters"][filterKey]["type"] == "histogram_slider":
                    aggs[f"{filterKey}_min"] = {"min": {"field": f"{filterKey}.lower"}}
                    aggs[f"{filterKey}_max"] = {"max": {"field": f"{filterKey}.upper"}}

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
        filterKey: str,
        agg_construct: typing.Dict,
        filters: typing.Dict = None,
    ) -> typing.Dict:
        filter = ElasticsearchManager._construct_query(
            es_config, filters, aggregation=filterKey
        )
        if filter:
            return {
                "filter": filter,
                "aggs": {
                    filterKey: agg_construct,
                },
            }
        return agg_construct

    @staticmethod
    def _construct_aggs(
        es_config: typing.Dict,
        filters: typing.Dict = None,
        full_range_aggs: typing.Dict = None,
    ) -> typing.Dict:
        aggs = {}
        for filterKey in es_config["filters"]:
            type = es_config["base"][filterKey]["type"]
            if type == "nested":
                if (
                    "type" in es_config["filters"][filterKey]
                    and es_config["filters"][filterKey]["type"] == "nested_present"
                ):
                    aggs[filterKey] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filterKey,
                        {
                            "nested": {
                                "path": filterKey,
                            },
                        },
                        filters,
                    )
                    aggs[
                        f"{filterKey}_missing"
                    ] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filterKey,
                        {
                            "missing": {
                                "field": filterKey,
                            },
                        },
                        filters,
                    )
                    continue
                aggs[filterKey] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filterKey,
                    {
                        "nested": {
                            "path": filterKey,
                        },
                        "aggs": {
                            "id_value": {
                                "terms": {
                                    "field": f"{filterKey}.id_value.keyword",
                                    "size": MAX_INT,
                                    "min_doc_count": 0,
                                }
                            },
                        },
                    },
                    filters,
                )
                continue
            if type == "nested_multi_type":
                aggs[filterKey] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filterKey,
                    {
                        "nested": {
                            "path": filterKey,
                        },
                        "aggs": {
                            "type_id_value": {
                                "terms": {
                                    "field": f"{filterKey}.type_id_value.keyword",
                                    "size": MAX_INT,
                                    "min_doc_count": 0,
                                }
                            },
                        },
                    },
                    filters,
                )
                continue
            if type == "uncertain_centuries":
                aggs[filterKey] = ElasticsearchManager._construct_filter_agg(
                    es_config,
                    filterKey,
                    {
                        "nested": {
                            "path": filterKey,
                        },
                        "aggs": {
                            "withoutUncertain": {
                                "terms": {
                                    "field": f"{filterKey}.withoutUncertain",
                                    "size": MAX_INT,
                                    "min_doc_count": 0,
                                }
                            },
                        },
                    },
                    filters,
                )
                continue
            if type == "text" or type == "[text]":
                if es_config["filters"][filterKey]["type"] == "dropdown":
                    aggs[filterKey] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filterKey,
                        {
                            "terms": {
                                "field": f"{filterKey}.keyword",
                                "size": MAX_INT,
                                "min_doc_count": 0,
                            },
                        },
                        filters,
                    )
                    continue
                if es_config["filters"][filterKey]["type"] == "autocomplete":
                    continue
            if type == "edtf" or type == "edtf_interval":
                if es_config["filters"][filterKey]["type"] == "histogram_slider":
                    aggs[
                        f"{filterKey}_hist"
                    ] = ElasticsearchManager._construct_filter_agg(
                        es_config,
                        filterKey,
                        {
                            "histogram": {
                                "field": f"{filterKey}.year_range",
                                "interval": es_config["filters"][filterKey]["interval"],
                                "extended_bounds": {
                                    "min": full_range_aggs[f"{filterKey}_min"],
                                    "max": full_range_aggs[f"{filterKey}_max"],
                                },
                                "min_doc_count": 0,
                            },
                        },
                        filters,
                    )
                    continue

            raise Exception(
                f"Filter {es_config['filters'][filterKey]['type']} of type {type} not yet implemented"
            )
        return aggs

    @staticmethod
    def _extract_agg(
        buckets: typing.List[typing.Dict],
        key_method: callable,
        value_method: callable,
        sort_value_method: callable = None,
        filter_count_0: bool = False,
        filter_values: typing.List = None,
    ) -> typing.List[typing.Dict]:
        if filter_values is None:
            filter_values_set = set()
        else:
            filter_values_set = set(filter_values)

        if sort_value_method is None:
            buckets = [
                {
                    "key": key_method(bucket),
                    "value": value_method(bucket),
                    "count": bucket["doc_count"],
                }
                for bucket in buckets
            ]
        else:
            buckets = [
                {
                    "key": key_method(bucket),
                    "value": value_method(bucket),
                    "count": bucket["doc_count"],
                    "sort_value": sort_value_method(bucket),
                }
                for bucket in buckets
            ]

        if filter_count_0:
            buckets = [
                bucket
                for bucket in buckets
                if bucket["count"] != 0 or bucket["key"] in filter_values_set
            ]

        if sort_value_method:
            buckets.sort(key=lambda bucket: bucket["sort_value"])

        return [
            {
                "key": bucket["key"],
                "value": bucket["value"],
                "count": bucket["count"],
            }
            for bucket in buckets
        ]

    @staticmethod
    def _has_filtered_aggregation(
        es_config: typing.Dict,
        filterKey: str,
    ) -> bool:
        type = es_config["base"][filterKey]["type"]
        if type in [
            "nested",
            "nested_multi_type",
            "uncertain_centuries",
            "edtf",
            "edtf_interval",
        ]:
            if (
                type == "nested"
                and "type" in es_config["filters"][filterKey]
                and es_config["filters"][filterKey]["type"] == "nested_present"
            ):
                return False
            return True
        if type == "text" or type == "[text]":
            if es_config["filters"][filterKey]["type"] == "dropdown":
                return True
        return False

    @staticmethod
    def _in_filtered_aggregation(
        es_config: typing.Dict,
        filterKey: str,
    ) -> bool:
        type = es_config["base"][filterKey]["type"]
        if type in [
            "nested",
            "nested_multi_type",
            "uncertain_centuries",
        ]:
            if (
                type == "nested"
                and "type" in es_config["filters"][filterKey]
                and es_config["filters"][filterKey]["type"] == "nested_present"
            ):
                return False
            return True
        if type == "text" or type == "[text]":
            if es_config["filters"][filterKey]["type"] == "dropdown":
                return True
        return False

    @staticmethod
    def _extract_aggs(
        es_config: typing.Dict,
        raw_result: typing.Dict,
        filters: typing.Dict = None,
        full_range_aggs: typing.Dict = None,
    ) -> typing.Dict[str, typing.List]:
        aggregations = raw_result["aggregations"]
        results = {}
        for filterKey in es_config["filters"].keys():
            type = es_config["base"][filterKey]["type"]
            if "sort" not in es_config["filters"][filterKey]:
                sort = None
            else:
                sort = es_config["filters"][filterKey]["sort"]

            agg_key = filterKey
            if type == "edtf" or type == "edtf_interval":
                agg_key = f"{agg_key}_hist"

            if agg_key not in aggregations:
                continue

            agg_values = aggregations[agg_key]

            # Exctract values from subaggregation in case of filtered aggregations
            if filters is not None and ElasticsearchManager._has_filtered_aggregation(
                es_config, filterKey
            ):
                for usedFilterKey in filters:
                    # These cases don't lead to a filtered aggregation
                    if not ElasticsearchManager._in_filtered_aggregation(
                        es_config, usedFilterKey
                    ):
                        continue
                    # Only filtered aggregation different from current key lead to an actual filtered aggregation
                    if usedFilterKey != filterKey:
                        agg_values = agg_values[filterKey]
                        break

            if filters is not None and filterKey in filters:
                filter_values = filters[filterKey]
            else:
                filter_values = []

            if type == "nested":
                if (
                    "type" in es_config["filters"][filterKey]
                    and es_config["filters"][filterKey]["type"] == "nested_present"
                ):
                    results[filterKey] = [
                        {
                            "key": 0,
                            "value": "No",
                            "count": aggregations[f"{filterKey}_missing"]["doc_count"],
                        },
                        {
                            "key": 1,
                            "value": "Yes",
                            "count": agg_values["doc_count"],
                        },
                    ]
                    continue
                if not sort:
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["id_value"]["buckets"],
                        key_method=lambda bucket: bucket["key"].split("|", maxsplit=1)[
                            0
                        ],
                        value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=1
                        )[1],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
                if sort == "alphabetically":
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["id_value"]["buckets"],
                        key_method=lambda bucket: bucket["key"].split("|", maxsplit=1)[
                            0
                        ],
                        value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=1
                        )[1],
                        sort_value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=1
                        )[1],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
            if type == "nested_multi_type":
                if not sort:
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["type_id_value"]["buckets"],
                        key_method=lambda bucket: "|".join(
                            bucket["key"].split("|", maxsplit=2)[:2]
                        ),
                        value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=2
                        )[2],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
                if sort == "alphabetically":
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["type_id_value"]["buckets"],
                        key_method=lambda bucket: "|".join(
                            bucket["key"].split("|", maxsplit=2)[:2]
                        ),
                        value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=2
                        )[2],
                        sort_value_method=lambda bucket: bucket["key"].split(
                            "|", maxsplit=2
                        )[2],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
            if type == "uncertain_centuries":
                if not sort:
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["withoutUncertain"]["buckets"],
                        key_method=lambda bucket: bucket["key"],
                        value_method=lambda bucket: bucket["key"],
                        sort_method=lambda bucket: roman.fromRoman(bucket["key"]),
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
                if sort == "chronologically":
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["withoutUncertain"]["buckets"],
                        key_method=lambda bucket: bucket["key"],
                        value_method=lambda bucket: bucket["key"],
                        sort_value_method=lambda bucket: roman.fromRoman(bucket["key"]),
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
            if type == "text" or type == "[text]":
                if not sort:
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["buckets"],
                        key_method=lambda bucket: bucket["key"],
                        value_method=lambda bucket: bucket["key"],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
                if sort == "alphabetically":
                    results[filterKey] = ElasticsearchManager._extract_agg(
                        agg_values["buckets"],
                        key_method=lambda bucket: bucket["key"],
                        value_method=lambda bucket: bucket["key"],
                        sort_value_method=lambda bucket: bucket["key"],
                        filter_count_0=True,
                        filter_values=filter_values,
                    )
                    continue
            if type == "edtf" or type == "edtf_interval":
                results[f"{filterKey}_hist"] = agg_values["buckets"]
                results[f"{filterKey}_min"] = full_range_aggs[f"{filterKey}_min"]
                results[f"{filterKey}_max"] = full_range_aggs[f"{filterKey}_max"]
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
