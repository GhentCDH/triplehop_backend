import edtf
import elasticsearch
from elasticsearch.helpers import async_bulk
import time
import typing
import uuid

from app.config import ELASTICSEARCH
from app.utils import dtu, RE_FIELD_CONVERSION

MAX_RESULT_WINDOW = 10000
DEFAULT_FROM = 0
DEFAULT_SIZE = 10
SCROLL_SIZE = 1000

# https://stackoverflow.com/questions/3838242/minimum-date-in-java
# https://github.com/elastic/elasticsearch/issues/43966
DATE_MIN = '-999999999'
DATE_MAX = '999999999'


class BaseElasticsearch:
    def __init__(self, es: elasticsearch.AsyncElasticsearch) -> None:
        self._es = es

    @staticmethod
    def extract_query_from_es_data_config(es_data_config: typing.List) -> typing.Dict:
        # TODO: document crdb_query format
        # get all requested fields
        requested_fields = set()
        for es_field_conf in es_data_config:
            if es_field_conf['type'] == 'nested':
                for part in es_field_conf['parts'].values():
                    requested_fields.add(part['selector_value'])
            else:
                requested_fields.add(es_field_conf['selector_value'])

        # construct the query to retrieve the data to construct all requested fields
        query = {
            'e_props': set(),
            'relations': {},
        }

        for requested_field in requested_fields:
            for match in RE_FIELD_CONVERSION.finditer(requested_field):
                current_level = query
                # remove all dollar signs
                path = [p.replace('$', '') for p in match.group(0).split('->')]
                for i, p in enumerate(path):
                    # last element => p = relation.r_prop or e_prop
                    if i == len(path) - 1:
                        # relation property
                        if '.' in p:
                            (relation, r_prop) = p.split('.')
                            if relation not in current_level['relations']:
                                current_level['relations'][relation] = {
                                    'e_props': set(),
                                    'r_props': set(),
                                    'relations': {},
                                }
                            current_level['relations'][relation]['r_props'].add(r_prop)
                        # entity property
                        else:
                            current_level['e_props'].add(p)
                    # not last element => p = relation => travel
                    else:
                        if p not in current_level['relations']:
                            current_level['relations'][p] = {
                                'e_props': set(),
                                'r_props': set(),
                                'relations': {},
                            }
                        current_level = current_level['relations'][p]

        return query

    @staticmethod
    def find_common_base_path(
        matches: typing.List[str]
    ) -> typing.List:
        split_matches = [match.replace('.', '->.').split('->') for match in matches]
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
            '->'.join(common),
            [
                '->'.join(split_match).replace('->.', '.')
                for split_match in split_matches
            ]
        ]

    @staticmethod
    def replace(
        entity_types_config: typing.Dict,
        entity_type_names: typing.Dict,
        input: str,
        data: typing.Dict,
    ) -> typing.List[str]:
        '''
        Always returns a string because of the usage of str.replace().
        '''
        # Split concatenate cases
        if ' $||$ ' in input:
            return [
                result
                for input_part in input.split(' $||$ ')
                for result in BaseElasticsearch.replace(
                    entity_types_config,
                    entity_type_names,
                    input_part,
                    data,
                )
            ]

        results = [input]

        matches = RE_FIELD_CONVERSION.findall(input)

        # Find common base in matches, preventing multiplying current_level numbers
        if len(matches) > 1:
            (base, based_matches) = BaseElasticsearch.find_common_base_path(matches)
            if base != '':
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
                    )
                ]

        for match in matches:
            if not match:
                continue

            current_levels = [data]
            path = [p.replace('$', '') for p in match.split('->')]
            for i, p in enumerate(path):
                if i == len(path) - 1:
                    # relation property
                    if '.' in p:
                        (rel_type_id, r_prop) = p.split('.')
                        key = 'id' if r_prop == 'id' else f'p_{dtu(r_prop)}'
                        new_results = []
                        for result in results:
                            for current_level in current_levels:
                                if rel_type_id == '':
                                    if key not in current_level['r_props']:
                                        continue
                                    new_results.append(
                                        result.replace(
                                            match,
                                            str(current_level['r_props'][key])
                                        )
                                    )
                                else:
                                    if 'relations' not in current_level:
                                        continue
                                    if rel_type_id not in current_level['relations']:
                                        continue
                                    for relation in current_level['relations'][rel_type_id].values():
                                        if key not in relation['r_props']:
                                            continue
                                        new_results.append(
                                            result.replace(
                                                match,
                                                str(relation['r_props'][key])
                                            )
                                        )
                        results = new_results
                        break
                    # entity property
                    if p == 'display_name':
                        results = [
                            result.replace(
                                match,
                                entity_types_config[entity_type_names[current_level['entity_type_id']]]['display_name']
                            )
                            for result in results
                            for current_level in current_levels
                        ]
                        break
                    key = 'id' if p == 'id' else f'p_{dtu(p)}'
                    results = [
                        result.replace(
                            match,
                            str(current_level['e_props'][key])
                        )
                        for result in results
                        for current_level in current_levels
                        if key in current_level['e_props']
                    ]
                    break
                # not last element => p = relation => travel
                new_current_levels = []
                for current_level in current_levels:
                    if 'relations' not in current_level or p not in current_level['relations']:
                        continue
                    for new_current_level in current_level['relations'][p].values():
                        new_current_levels.append(new_current_level)
                current_levels = new_current_levels

        return results

    @staticmethod
    def get_datas_for_base(base: str, data: typing.Dict) -> typing.List[typing.Dict]:
        '''
        Given a base path, return the data at this base level.
        As there might be multiple relations reaching to this level, there might be multiple results.
        '''
        current_levels = [data]
        path = [p.replace('$', '') for p in base.split('->')]
        for p in path:
            new_current_levels = []
            for current_level in current_levels:
                if 'relations' not in current_level:
                    continue
                if p not in current_level['relations']:
                    continue
                for related in current_level['relations'][p].values():
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
        if es_field_conf['type'] == 'integer':
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf['selector_value'],
                data,
            )
            if len(str_values) > 1:
                raise Exception('Not implemented')
            if not str_values:
                return None
            return int(str_values[0])

        if es_field_conf['type'] == '[text]':
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf['selector_value'],
                data,
            )
            if not str_values:
                return None
            return list(set(str_values))

        if es_field_conf['type'] == 'text':
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf['selector_value'],
                data,
            )
            if not str_values:
                return None
            return str_values[0]

        if es_field_conf['type'] == 'edtf':
            str_values = BaseElasticsearch.replace(
                entity_types_config,
                entity_type_names,
                es_field_conf['selector_value'],
                data,
            )
            if len(str_values) > 1:
                raise Exception('Not implemented')
            if not str_values:
                return None
            str_value = str_values[0]

            # Open ending
            if str_value == '..':
                if es_field_conf['subtype'] == 'start':
                    return {
                        'text': str_value,
                        'lower': DATE_MIN,
                        'upper': DATE_MIN,
                    }
                else:
                    return {
                        'text': str_value,
                        'lower': DATE_MAX,
                        'upper': DATE_MAX,
                    }

            # Unknown
            if str_value == '':
                return {
                    'text': None,
                    'lower': None,
                    'upper': None,
                }

            try:
                # edtf module needs to be updated to the newest revision
                old_edtf_text = str_value.replace('X', 'u')
                edtf_date = edtf.parse_edtf(old_edtf_text)
            except edtf.parser.edtf_exceptions.EDTFParseException:
                raise Exception(f'EDTF parser cannot parse {old_edtf_text}')
            return {
                'text': str_value,
                'lower': time.strftime(
                    '%Y-%m-%d',
                    edtf_date.lower_strict()
                ),
                'upper': time.strftime(
                    '%Y-%m-%d',
                    edtf_date.upper_strict()
                )
            }

        if es_field_conf['type'] == 'nested':
            # TODO: relation properties of base?
            datas = BaseElasticsearch.get_datas_for_base(
                es_field_conf['base'],
                data,
            )
            if 'filter' in es_field_conf:
                (filter_value, comp_value) = es_field_conf['filter'].split(' == ')
                datas = [
                    data
                    for data in datas
                    if BaseElasticsearch.replace(
                        entity_types_config,
                        entity_type_names,
                        (
                            filter_value
                            .replace(f'{es_field_conf["base"]}->', '')
                            .replace(f'{es_field_conf["base"]}.', '.')
                        ),
                        data,
                    )[0] == comp_value
                ]
            results = []
            for data in datas:
                result = {
                    'entity_type_name': entity_type_names[data['entity_type_id']]
                }
                for key, part_def in es_field_conf['parts'].items():
                    part_def['selector_value'] = (
                        part_def['selector_value']
                        .replace(f'{es_field_conf["base"]}->', '')
                        .replace(f'{es_field_conf["base"]}.', '.')
                    )
                    result[key] = BaseElasticsearch.convert_field(
                        entity_types_config,
                        entity_type_names,
                        part_def,
                        data,
                    )
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
            et_config['id']: et_name
            for et_name, et_config in entity_types_config.items()
        }

        docs = {}
        for entity_id, entity in entities.items():
            doc = {}
            for es_field_conf in es_data_config:
                doc[es_field_conf['system_name']] = BaseElasticsearch.convert_field(
                    entity_types_config,
                    entity_type_names,
                    es_field_conf,
                    entity,
                )

            docs[entity_id] = doc
        return docs

    async def create_new_index(self, entity_type_name: str, es_data_config: typing.List) -> str:
        new_index_name = f'{ELASTICSEARCH["prefix"]}_{dtu(str(uuid.uuid4()))}'
        body = {
            'mappings': {
                'dynamic': 'strict',
                'properties': {},
            },
            'settings': {
                'analysis': {
                    'char_filter': {
                        'remove_special': {
                            'type': 'pattern_replace',
                            'pattern': '[\\p{Punct}]',
                            'replacement': '',
                        },
                        'numbers_last': {
                            'type': 'pattern_replace',
                            'pattern': '([0-9])',
                            'replacement': 'zzz$1',
                        },
                    },
                    'normalizer': {
                        'icu_normalizer': {
                            'char_filter': [
                                'remove_special',
                                'numbers_last',
                            ],
                            'filter': [
                                'icu_folding',
                                'lowercase',
                            ],
                        },
                    },
                },
            },
        }

        for es_field_conf in es_data_config:
            mapping = {}
            # TODO: do the keywords and completion need to be added for all text fields?
            if es_field_conf['type'] in ['text', '[text]']:
                mapping['type'] = 'text'
                mapping['fields'] = {
                    'keyword': {
                        'type': 'keyword',
                    },
                    'normalized_keyword': {
                        'type': 'keyword',
                        'normalizer': 'icu_normalizer',
                    },
                    'completion': {
                        'type': 'completion'
                    },
                }
            elif es_field_conf['type'] == 'integer':
                mapping['type'] = 'integer'
            elif es_field_conf['type'] == 'edtf':
                mapping['type'] = 'object'
                mapping['properties'] = {
                    'text': {
                        'type': 'text',
                        'fields': {
                            'keyword': {
                                'type': 'keyword',
                            },
                        },
                    },
                    'lower': {
                        'type': 'date',
                    },
                    'upper': {
                        'type': 'date',
                    },
                }
            elif es_field_conf['type'] == 'nested':
                mapping['type'] = 'nested'
                mapping['properties'] = {
                    'entity_type_name': {
                        'type': 'text',
                    },
                }
                for key, part_def in es_field_conf['parts'].items():
                    mapping['properties'][key] = {
                        'type': part_def['type'],
                    }
                    if mapping['properties'][key]['type'] == 'text':
                        mapping['properties'][key]['fields'] = {
                            'keyword': {
                                'type': 'keyword',
                            },
                            'normalized_keyword': {
                                'type': 'keyword',
                                'normalizer': 'icu_normalizer',
                            },
                        }
                    # TODO: check if the normalized keyword can be retrieved
                    # to aid sorting multiple values in a single nested field in clients
            else:
                raise Exception(f'Elastic type {es_field_conf["type"]} is not yet implemented')
            body['mappings']['properties'][es_field_conf['system_name']] = mapping

        response = await self._es.indices.create(
            index=new_index_name,
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return new_index_name

        raise Exception(response['error']['root_cause'])

    async def switch_to_new_index(self, new_index_name: str, entity_type_id: str) -> None:
        alias_name = f'{ELASTICSEARCH["prefix"]}_{dtu(entity_type_id)}'
        body = {
            'actions': [
                {
                    'add': {
                        'index': new_index_name,
                        'alias': alias_name,
                    },
                },
            ],
        }

        try:
            response = await self._es.indices.get(alias_name)
            for old_index_name in response.keys():
                body['actions'].append(
                    {
                        'remove_index': {
                            'index': old_index_name,
                        },
                    }
                )
        except elasticsearch.exceptions.NotFoundError:
            pass

        response = await self._es.indices.update_aliases(
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return

        raise Exception(response['error']['root_cause'])

    async def add_bulk(self, index_name: str, entity_type_name: str, data: typing.Dict) -> None:
        actions = [
            {
                '_index': index_name,
                '_id': i,
                '_source': v,
            }
            for i, v in data.items()
        ]
        await async_bulk(self._es, actions)

    async def search(self, entity_type_id: str, body: typing.Dict) -> typing.Dict:
        alias_name = f'{ELASTICSEARCH["prefix"]}_{dtu(entity_type_id)}'
        body = {k: v for (k, v) in body.items() if v is not None}

        es_from = body['from'] if 'from' in body else DEFAULT_FROM
        es_size = body['size'] if 'size' in body else DEFAULT_SIZE
        if es_from + es_size <= MAX_RESULT_WINDOW:
            results = await self._es.search(
                index=alias_name,
                body=body,
            )
            return results
        else:
            # Use scroll API
            results = {
                'hits': {
                    'hits': []
                }
            }

            if 'from' in body:
                del body['from']
            body['size'] = SCROLL_SIZE

            data = await self._es.search(
                index=alias_name,
                body=body,
                scroll='1m',
            )
            results['hits']['total'] = data['hits']['total']

            current_from = 0
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

            while scroll_size > 0:
                if es_from < current_from + SCROLL_SIZE:
                    start = max(es_from-current_from, 0)
                    if es_from + es_size < current_from + SCROLL_SIZE:
                        results['hits']['hits'] += data['hits']['hits'][start:start + es_size]
                        break
                    else:
                        results['hits']['hits'] += data['hits']['hits'][start:]

                data = await self._es.scroll(scroll_id=sid, scroll='1m')
                sid = data['_scroll_id']
                scroll_size = len(data['hits']['hits'])
                current_from += SCROLL_SIZE

            return results
