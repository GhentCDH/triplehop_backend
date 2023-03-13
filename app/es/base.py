import json
import re
import time
import typing
import uuid
from datetime import date, datetime

import edtf
import elasticsearch
import roman
from elasticsearch.helpers import async_bulk

from app.config import ELASTICSEARCH
from app.utils import RE_FIELD_CONVERSION, dtu

MAX_RESULT_WINDOW = 10000
MAX_INT = 2147483647
DEFAULT_FROM = 0
DEFAULT_SIZE = 25
AGG_SIZE = 200

# https://stackoverflow.com/questions/3838242/minimum-date-in-java
# https://github.com/elastic/elasticsearch/issues/43966
DATE_MIN = "-999999999"
DATE_MAX = "999999999"

RE_YYYY_MM_DD = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
RE_YYYY = re.compile(r"^[0-9]{4}$")


class BaseElasticsearch:
    def __init__(self, es: elasticsearch.AsyncElasticsearch) -> None:
        self._es = es

    @staticmethod
    def extract_query_from_es_data_config(es_data_config: typing.List) -> typing.Dict:
        # TODO: document triplehop_query format
        # get all requested fields
        requested_fields = set()
        for es_field_conf in es_data_config:
            if (
                es_field_conf["type"] == "nested"
                or es_field_conf["type"] == "nested_multi_type"
                or es_field_conf["type"] == "nested_flatten"
            ):
                for part in es_field_conf["parts"].values():
                    if part[0] == ".":
                        requested_fields.add(f"{es_field_conf['base']}{part}")
                    else:
                        requested_fields.add(f"{es_field_conf['base']}->{part}")
                if "filter" in es_field_conf:
                    if es_field_conf["filter"][0] == ".":
                        requested_fields.add(
                            f"{es_field_conf['base']}{es_field_conf['filter']}"
                        )
                    else:
                        requested_fields.add(
                            f"{es_field_conf['base']}->{es_field_conf['filter']}"
                        )
            elif es_field_conf["type"] == "edtf_interval":
                requested_fields.add(es_field_conf["start"])
                requested_fields.add(es_field_conf["end"])
            else:
                requested_fields.add(es_field_conf["selector_value"])

        # construct the query to retrieve the data to construct all requested fields
        query = {
            "e_props": set(),
            "relations": {},
        }

        for requested_field in requested_fields:
            for match in RE_FIELD_CONVERSION.finditer(requested_field):
                current_level = query
                # remove all dollar signs
                path = [p.replace("$", "") for p in match.group(0).split("->")]
                for i, p in enumerate(path):
                    # last element => p = relation.r_prop or e_prop
                    if i == len(path) - 1:
                        # relation property
                        if "." in p:
                            (relation, r_prop) = p.split(".")
                            if relation not in current_level["relations"]:
                                current_level["relations"][relation] = {
                                    "e_props": set(),
                                    "r_props": set(),
                                    "relations": {},
                                }
                            current_level["relations"][relation]["r_props"].add(r_prop)
                        # entity property
                        else:
                            current_level["e_props"].add(p)
                    # not last element => p = relation => travel
                    else:
                        if p not in current_level["relations"]:
                            current_level["relations"][p] = {
                                "e_props": set(),
                                "r_props": set(),
                                "relations": {},
                            }
                        current_level = current_level["relations"][p]

        return query

    @staticmethod
    def find_common_base_path(matches: typing.List[str]) -> typing.List:
        split_matches = [match.replace(".", "->.").split("->") for match in matches]
        common = []
        while len(split_matches[0]) > 1:
            common_to_test = split_matches[0][0]
            is_common = True
            for split_match in split_matches:
                if split_match[0] != common_to_test:
                    is_common = False
                    break
            if not is_common:
                break
            common.append(common_to_test)
            for split_match in split_matches:
                split_match.pop(0)
        return [
            "->".join(common),
            [
                "->".join(split_match).replace("->.", ".")
                for split_match in split_matches
            ],
        ]

    @staticmethod
    def replace(
        entity_types_config: typing.Dict,
        entity_type_names: typing.Dict,
        input: str,
        data: typing.Dict,
        display_not_available: bool = False,
    ) -> typing.List[str]:
        """
        Always returns an array of strings because of the usage of str.replace().
        """
        # Split concatenate cases
        if " $||$ " in input:
            return [
                result
                for input_part in input.split(" $||$ ")
                for result in BaseElasticsearch.replace(
                    entity_types_config,
                    entity_type_names,
                    input_part,
                    data,
                    display_not_available,
                )
            ]

        results = [input]

        matches = RE_FIELD_CONVERSION.findall(input)

        # Find common base in matches, preventing multiplying current_level numbers
        if len(matches) > 1:
            (base, based_matches) = BaseElasticsearch.find_common_base_path(matches)
            if base != "":
                for i, match in enumerate(matches):
                    input = input.replace(match, based_matches[i], 1)
                return [
                    result
                    for data in BaseElasticsearch.get_datas_for_base(base, data)
                    for result in BaseElasticsearch.replace(
                        entity_types_config,
                        entity_type_names,
                        input,
                        data,
                        display_not_available,
                    )
                ]

        for match in matches:
            if not results:
                break

            if not match:
                continue

            current_levels = [data]
            path = [p.replace("$", "") for p in match.split("->")]

            for i, p in enumerate(path):
                if i == len(path) - 1:
                    # relation property
                    if "." in p:
                        (rel_type_id, r_prop) = p.split(".")
                        key = "id" if r_prop == "id" else f"p_{dtu(r_prop)}"
                        new_results = []
                        for result in results:
                            for current_level in current_levels:
                                if rel_type_id == "":
                                    if key not in current_level["r_props"]:
                                        continue
                                    new_results.append(
                                        result.replace(
                                            match, str(current_level["r_props"][key])
                                        )
                                    )
                                else:
                                    if "relations" not in current_level:
                                        continue
                                    if rel_type_id not in current_level["relations"]:
                                        continue
                                    for relation in current_level["relations"][
                                        rel_type_id
                                    ].values():
                                        if key not in relation["r_props"]:
                                            continue
                                        new_results.append(
                                            result.replace(
                                                match, str(relation["r_props"][key])
                                            )
                                        )
                        results = new_results
                        break
                    # entity property
                    if p == "display_name":
                        results = [
                            result.replace(
                                match,
                                entity_types_config[
                                    entity_type_names[current_level["entity_type_id"]]
                                ]["display_name"],
                            )
                            for result in results
                            for current_level in current_levels
                        ]
                        break
                    if p == "entity_type_name":
                        results = [
                            result.replace(
                                match,
                                entity_type_names[current_level["entity_type_id"]],
                            )
                            for result in results
                            for current_level in current_levels
                        ]
                        break
                    key = "id" if p == "id" else f"p_{dtu(p)}"
                    new_results = [
                        result.replace(match, str(current_level["e_props"][key]))
                        for result in results
                        for current_level in current_levels
                        if key in current_level["e_props"]
                    ]
                    if new_results:
                        results = new_results
                        break
                    if not new_results and display_not_available:
                        results = [result.replace(match, "N/A") for result in results]
                        break
                # not last element => p = relation => travel
                new_current_levels = []
                for current_level in current_levels:
                    if (
                        "relations" not in current_level
                        or p not in current_level["relations"]
                    ):
                        continue
                    for new_current_level in current_level["relations"][p].values():
                        new_current_levels.append(new_current_level)
                current_levels = new_current_levels
                if not current_levels:
                    results = []
                    break

        # Replace single quotes with double quotes so lists can be loaded as json
        results = [result.replace("'", '"') for result in results]

        return results

    @staticmethod
    def get_datas_for_base(base: str, data: typing.Dict) -> typing.List[typing.Dict]:
        """
        Given a base path, return the data at this base level.
        As there might be multiple relations reaching to this level, there might be multiple results.
        """
        if base == "":
            return [data]
        current_levels = [data]
        path = [p.replace("$", "") for p in base.split("->")]
        for p in path:
            new_current_levels = []
            for current_level in current_levels:
                if "relations" not in current_level:
                    continue
                if p not in current_level["relations"]:
                    continue
                for related in current_level["relations"][p].values():
                    new_current_levels.append(related)
            current_levels = new_current_levels
        return current_levels

    @staticmethod
    def convert_field(
        entity_types_config: typing.Dict,
        entity_type_names: typing.Dict,
        es_field_conf: typing.Dict,
        data: typing.Dict,
    ) -> typing.Any:
        if es_field_conf["type"] == "integer":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
            )
            if len(str_values) > 1:
                raise Exception("Not implemented")
            if not str_values:
                return None
            return int(str_values[0])

        if es_field_conf["type"] == "[text]":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
            )
            if not str_values:
                return None

            try:
                # try if values are a list of json encoded lists
                flattened_values = [
                    val for vals in str_values for val in json.loads(vals)
                ]
                unique_values = list(set(flattened_values))
            except (json.decoder.JSONDecodeError, TypeError):
                # process as a list of text strings if impossible to decode as Json
                # or impossible to iterate (integers can be decoded as Json)
                unique_values = list(set(str_values))

            return unique_values

        if es_field_conf["type"] == "text":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
                es_field_conf.get("display_not_available"),
            )
            if not str_values:
                return None
            return str_values[0]

        if es_field_conf["type"] == "text_flatten":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
            )
            if not str_values:
                return None

            try:
                # try if values are a list of json encoded lists
                flattened_values = [
                    val for vals in str_values for val in json.loads(vals)
                ]
                unique_values = list(set(flattened_values))
            except (json.decoder.JSONDecodeError, TypeError):
                # process as a list of text strings if impossible to decode as Json
                # or impossible to iterate (integers can be decoded as Json)
                unique_values = list(set(str_values))

            return ", ".join(unique_values)

        if es_field_conf["type"] == "edtf":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
            )
            if len(str_values) > 1:
                raise Exception("Not implemented")
            if not str_values:
                return None
            str_value = str_values[0]

            # Unknown
            if str_value == "":
                return None

            # Open ending for intervals
            if str_value == "..":
                if es_field_conf["interval_position"] == "start":
                    return {
                        "text": str_value,
                        "lower": DATE_MIN,
                        "upper": DATE_MIN,
                    }
                else:
                    return {
                        "text": str_value,
                        "lower": DATE_MAX,
                        "upper": DATE_MAX,
                    }

            if RE_YYYY_MM_DD.match(str_value):
                # parse as date to validate format and to retrieve year
                yyyy_mm_dd = date.fromisoformat(str_value)

                result = {
                    "text": str_value,
                    "lower": str_value,
                    "upper": str_value,
                }
                # only for edtf, not for edtf_interval
                if "interval_position" not in es_field_conf:
                    result["year_range"] = {
                        "gte": yyyy_mm_dd.year,
                        "lte": yyyy_mm_dd.year,
                    }
                return result

            if RE_YYYY.match(str_value):
                # parse as date to validate format and to retrieve year
                yyyy = datetime.strptime(str_value, "%Y")

                result = {
                    "text": str_value,
                    "lower": f"{str_value}-01-01",
                    "upper": f"{str_value}-12-31",
                }
                # only for edtf, not for edtf_interval
                if "interval_position" not in es_field_conf:
                    result["year_range"] = {
                        "gte": yyyy.year,
                        "lte": yyyy.year,
                    }
                return result

            try:
                # edtf module needs to be updated to the newest revision
                # https://github.com/ixc/python-edtf/issues/24
                old_edtf_text = str_value.replace("X", "u")
                edtf_date = edtf.parse_edtf(old_edtf_text)
            except edtf.parser.edtf_exceptions.EDTFParseException:
                raise Exception(f"EDTF parser cannot parse {old_edtf_text}")
            result = {
                "text": str_value,
                "lower": time.strftime("%Y-%m-%d", edtf_date.lower_strict()),
                "upper": time.strftime("%Y-%m-%d", edtf_date.upper_strict()),
            }
            # only for edtf, not for edtf_interval
            if "interval_position" not in es_field_conf:
                year_lower = edtf_date.lower_strict()[0]
                year_upper = edtf_date.upper_strict()[0]
                result["year_range"] = {
                    "gte": year_lower,
                    "lte": year_upper,
                }

            return result

        if es_field_conf["type"] == "edtf_interval":
            result = {
                key: BaseElasticsearch.convert_field(
                    entity_types_config,
                    entity_type_names,
                    {
                        "type": "edtf",
                        "selector_value": es_field_conf[key],
                        "interval_position": key,
                    },
                    data,
                )
                for key in ["start", "end"]
            }

            if (
                result["start"] is None
                or result["start"]["text"] is None
                or result["start"]["text"] == ".."
            ):
                if (
                    result["end"] is None
                    or result["end"]["text"] is None
                    or result["end"]["text"] == ".."
                ):
                    result["year_range"] = None
                    return result

                # start is not set or open => take end.lower for year_lower
                result["lower"] = result["end"]["lower"]
                year_lower = int(
                    time.strftime(
                        "%Y", time.strptime(result["end"]["lower"], "%Y-%m-%d")
                    )
                )
            else:
                result["lower"] = result["start"]["lower"]
                year_lower = int(
                    time.strftime(
                        "%Y", time.strptime(result["start"]["lower"], "%Y-%m-%d")
                    )
                )

            if result["end"] is None or result["end"]["text"] is None:
                # end is not set => take start.upper for year_upper
                result["upper"] = result["start"]["upper"]
                year_upper = int(
                    time.strftime(
                        "%Y", time.strptime(result["start"]["upper"], "%Y-%m-%d")
                    )
                )
            elif result["end"]["text"] == "..":
                # end is open => take current year
                result["upper"] = time.strftime("%Y-%m-%d")
                year_upper = time.strftime("%Y")
            else:
                result["upper"] = result["end"]["upper"]
                year_upper = int(
                    time.strftime(
                        "%Y", time.strptime(result["end"]["upper"], "%Y-%m-%d")
                    )
                )

            result["year_range"] = {
                "gte": year_lower,
                "lte": year_upper,
            }

            return result

        if es_field_conf["type"] == "uncertain_centuries":
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf["selector_value"],
                data,
            )
            if len(str_values) > 1:
                raise Exception("Not implemented")
            if not str_values:
                return None

            flattened_values = [val for vals in str_values for val in json.loads(vals)]
            unique_values = list(set(flattened_values))

            return [
                {
                    "display": roman_val,
                    "withoutUncertain": roman_val.replace("?", ""),
                    "numeric": roman.fromRoman(roman_val.replace("?", "")),
                }
                for roman_val in unique_values
            ]

        if (
            es_field_conf["type"] == "nested"
            or es_field_conf["type"] == "nested_multi_type"
            or es_field_conf["type"] == "nested_flatten"
        ):
            datas = BaseElasticsearch.get_datas_for_base(
                es_field_conf["base"],
                data,
            )
            if "filter" in es_field_conf:
                (filter_value, comp_value) = es_field_conf["filter"].split(" == ")
                datas = [
                    data
                    for data in datas
                    if BaseElasticsearch.replace(
                        entity_types_config,
                        entity_type_names,
                        (filter_value),
                        data,
                    )[0]
                    == comp_value
                ]
            results = []
            for data in datas:
                result = {
                    "entity_type_name": BaseElasticsearch.convert_field(
                        entity_types_config,
                        entity_type_names,
                        {
                            "selector_value": es_field_conf["parts"][
                                "entity_type_name"
                            ],
                            "type": "text",
                        },
                        data,
                    ),
                    "id": BaseElasticsearch.convert_field(
                        entity_types_config,
                        entity_type_names,
                        {
                            "selector_value": es_field_conf["parts"]["id"],
                            "type": "integer",
                        },
                        data,
                    ),
                    "value": BaseElasticsearch.convert_field(
                        entity_types_config,
                        entity_type_names,
                        {
                            "selector_value": es_field_conf["parts"]["selector_value"],
                            "type": "text_flatten"
                            if es_field_conf["type"] == "nested_flatten"
                            else "text",
                            "display_not_available": es_field_conf.get(
                                "display_not_available"
                            ),
                        },
                        data,
                    ),
                }
                if (
                    es_field_conf["type"] == "nested"
                    or es_field_conf["type"] == "nested_flatten"
                ):
                    result["id_value"] = f"{result['id']}|{result['value']}"
                elif es_field_conf["type"] == "nested_multi_type":
                    result["type_id"] = f"{result['entity_type_name']}|{result['id']}"
                    result[
                        "type_id_value"
                    ] = f"{result['entity_type_name']}|{result['id']}|{result['value']}"

                results.append(result)
            return results
        raise Exception(f'Elastic type {es_field_conf["type"]} is not yet implemented')

    @staticmethod
    def convert_entities_to_docs(
        entity_types_config: typing.Dict,
        es_data_config: typing.List,
        entities: typing.Dict,
    ) -> typing.Dict:
        entity_type_names = {
            et_config["id"]: et_name
            for et_name, et_config in entity_types_config.items()
        }

        docs = {}
        for entity_id, entity in entities.items():
            doc = {}
            for es_field_conf in es_data_config:
                doc[es_field_conf["system_name"]] = BaseElasticsearch.convert_field(
                    entity_types_config,
                    entity_type_names,
                    es_field_conf,
                    entity,
                )

            docs[entity_id] = doc
        return docs

    async def create_new_index(self, es_data_config: typing.List) -> str:
        new_index_name = f'{ELASTICSEARCH["prefix"]}_{dtu(str(uuid.uuid4()))}'
        body = {
            "mappings": {
                "dynamic": "strict",
                "properties": {},
            },
            "settings": {
                "analysis": {
                    "char_filter": {
                        "remove_special": {
                            "type": "pattern_replace",
                            "pattern": "[\\p{Punct}]",
                            "replacement": "",
                        },
                        "numbers_last": {
                            "type": "pattern_replace",
                            "pattern": "([0-9])",
                            "replacement": "zzz$1",
                        },
                    },
                    "normalizer": {
                        "icu_normalizer": {
                            "char_filter": [
                                "remove_special",
                                "numbers_last",
                            ],
                            "filter": [
                                "icu_folding",
                                "lowercase",
                            ],
                        },
                    },
                },
            },
        }

        for es_field_conf in es_data_config:
            mapping = {}
            # TODO: do the keywords and completion need to be added for all text fields?
            if es_field_conf["type"] in ["text", "[text]"]:
                mapping["type"] = "text"
                mapping["fields"] = {
                    "keyword": {
                        "type": "keyword",
                    },
                    "normalized_keyword": {
                        "type": "keyword",
                        "normalizer": "icu_normalizer",
                    },
                    "normalized_text": {
                        "type": "text",
                        "analyzer": "icu_analyzer",
                    },
                    "completion": {"type": "completion"},
                }
            elif es_field_conf["type"] == "integer":
                mapping["type"] = "integer"
            # TODO: does year_range need to be added for all edtf fields?
            elif es_field_conf["type"] == "edtf":
                mapping["type"] = "object"
                mapping["properties"] = {
                    "text": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                            },
                        },
                    },
                    "lower": {
                        "type": "date",
                    },
                    "upper": {
                        "type": "date",
                    },
                    "year_range": {
                        "type": "integer_range",
                    },
                }
            # TODO: does year_range need to be added for all edtf_interval fields?
            elif es_field_conf["type"] == "edtf_interval":
                mapping["type"] = "object"
                mapping["properties"] = {
                    "start": {
                        "type": "object",
                        "properties": {
                            "text": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                    },
                                },
                            },
                            "lower": {
                                "type": "date",
                            },
                            "upper": {
                                "type": "date",
                            },
                        },
                    },
                    "end": {
                        "type": "object",
                        "properties": {
                            "text": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                    },
                                },
                            },
                            "lower": {
                                "type": "date",
                            },
                            "upper": {
                                "type": "date",
                            },
                        },
                    },
                    "lower": {
                        "type": "date",
                    },
                    "upper": {
                        "type": "date",
                    },
                    "year_range": {
                        "type": "integer_range",
                    },
                }
            elif es_field_conf["type"] == "uncertain_centuries":
                mapping["type"] = "nested"
                mapping["properties"] = {
                    "display": {
                        "type": "text",
                    },
                    "withoutUncertain": {
                        "type": "keyword",
                        "fields": {
                            "normalized_keyword": {
                                "type": "keyword",
                                "normalizer": "icu_normalizer",
                            },
                        },
                    },
                    "numeric": {
                        "type": "integer",
                    },
                }
            elif (
                es_field_conf["type"] == "nested"
                or es_field_conf["type"] == "nested_multi_type"
                or es_field_conf["type"] == "nested_flatten"
            ):
                mapping["type"] = "nested"
                mapping["properties"] = {
                    "entity_type_name": {
                        "type": "text",
                    },
                    "id": {
                        "type": "integer",
                    },
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                            },
                            # TODO: check if the normalized keyword can be retrieved
                            # to aid sorting multiple values in a single nested field in clients
                            "normalized_keyword": {
                                "type": "keyword",
                                "normalizer": "icu_normalizer",
                            },
                            "normalized_text": {
                                "type": "text",
                                "analyzer": "icu_analyzer",
                            },
                        },
                    },
                }
                if (
                    es_field_conf["type"] == "nested"
                    or es_field_conf["type"] == "nested_flatten"
                ):
                    mapping["properties"]["id_value"] = {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                            },
                        },
                    }
                elif es_field_conf["type"] == "nested_multi_type":
                    mapping["properties"]["type_id"] = {
                        "type": "keyword",
                    }
                    mapping["properties"]["type_id_value"] = {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                            },
                        },
                    }
            else:
                raise Exception(
                    f'Elastic type {es_field_conf["type"]} is not yet implemented'
                )
            body["mappings"]["properties"][es_field_conf["system_name"]] = mapping

        response = await self._es.indices.create(
            index=new_index_name,
            body=body,
        )

        if "acknowledged" in response and response["acknowledged"] is True:
            return new_index_name

        raise Exception(response["error"]["root_cause"])

    async def switch_to_new_index(
        self, new_index_name: str, entity_type_id: str
    ) -> None:
        alias_name = f'{ELASTICSEARCH["prefix"]}_{dtu(entity_type_id)}'
        body = {
            "actions": [
                {
                    "add": {
                        "index": new_index_name,
                        "alias": alias_name,
                    },
                },
            ],
        }

        try:
            response = await self._es.indices.get(alias_name)
            for old_index_name in response.keys():
                body["actions"].append(
                    {
                        "remove_index": {
                            "index": old_index_name,
                        },
                    }
                )
        except elasticsearch.exceptions.NotFoundError:
            pass

        response = await self._es.indices.update_aliases(
            body=body,
        )

        if "acknowledged" in response and response["acknowledged"] is True:
            return

        raise Exception(response["error"]["root_cause"])

    async def add_bulk(self, index_name: str, data: typing.Dict) -> None:
        actions = [
            {
                "_index": index_name,
                "_id": i,
                "_source": v,
            }
            for i, v in data.items()
        ]
        await async_bulk(self._es, actions)

    async def op_bulk(
        self, entity_type_id: str, data: typing.Dict, operation: str = None
    ) -> None:
        alias_name = f'{ELASTICSEARCH["prefix"]}_{dtu(entity_type_id)}'
        common = {
            "_index": alias_name,
        }
        if operation:
            common["_op_type"] = operation

        actions = []
        for i, v in data.items():
            action = {
                **common,
                "_id": i,
            }
            if operation == "index":
                action["_source"] = v
            if operation == "update":
                action["doc"] = v
            actions.append(action)
        await async_bulk(self._es, actions, refresh=True)
