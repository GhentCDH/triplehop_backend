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
    def replace(input: str, data: typing.Dict) -> str:
        '''
        Always returns a string because of the usage of str.replace().
        '''
        result = input
        for match in RE_FIELD_CONVERSION.finditer(input):
            if not match:
                continue

            current_level = data
            path = [p.replace('$', '') for p in match.group(0).split('->')]
            for i, p in enumerate(path):
                # last element => p = relation.r_prop or e_prop
                if i == len(path) - 1:
                    # relation property
                    if '.' in p:
                        (rel_type_id, r_prop) = p.split('.')
                        if len(current_level['relations'][rel_type_id].values()) > 1:
                            raise Exception('Not implemented')
                        key = 'id' if r_prop == 'id' else f'p_{dtu(r_prop)}'
                        result = result.replace(
                            match.group(0),
                            str(list(current_level['relations'][rel_type_id].values())[0]['r_props'].get(key, ''))
                        )
                        break
                    # entity property
                    key = 'id' if p == 'id' else f'p_{dtu(p)}'
                    result = result.replace(
                        match.group(0),
                        str(current_level['e_props'].get(key, ''))
                    )
                    break
                # not last element => p = relation => travel
                if len(current_level['relations'][p].values()) > 1:
                    raise Exception('Not implemented')
                current_level = list(current_level['relations'][p].values())[0]

        return result

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
                for related in current_level['relations'][p].values():
                    new_current_levels.append(related)
            current_levels = new_current_levels
        return current_levels

    @staticmethod
    def convert_field(
        entity_type_names: typing.Dict,
        es_field_conf: str,
        data: typing.Dict,
    ) -> typing.Any:
        if es_field_conf['type'] == 'integer':
            return int(
                BaseElasticsearch.replace(
                    es_field_conf['selector_value'],
                    data,
                )
            )
        if es_field_conf['type'] == 'text':
            return BaseElasticsearch.replace(
                es_field_conf['selector_value'],
                data,
            )
        if es_field_conf['type'] == 'edtf':
            edtf_text = BaseElasticsearch.replace(
                    es_field_conf['selector_value'],
                    data,
                )
            if edtf_text == '..':
                if es_field_conf['subtype'] == 'start':
                    return {
                        'text': edtf_text,
                        'lower': DATE_MIN,
                        'upper': DATE_MIN,
                    }
                else:
                    return {
                        'text': edtf_text,
                        'lower': DATE_MAX,
                        'upper': DATE_MAX,
                    }
            if edtf_text == '':
                return {
                    'text': edtf_text,
                    'lower': None,
                    'upper': None,
                }
            try:
                # edtf needs to be updated to the newest revision
                old_edtf_text = edtf_text.replace('X', 'u')
                edtf_date = edtf.parse_edtf(old_edtf_text)
            except edtf.parser.edtf_exceptions.EDTFParseException:
                raise Exception(f'EDTF parser cannot parse {old_edtf_text}')
            return {
                'text': edtf_text,
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
            results = []
            for data in datas:
                result = {
                    'entity_type_name': entity_type_names[data['entity_type_id']]
                }
                for key, part_def in es_field_conf['parts'].items():
                    part_def['selector_value'] = part_def['selector_value'].replace(f'{es_field_conf["base"]}->', '')
                    result[key] = BaseElasticsearch.convert_field(
                        entity_type_names,
                        part_def,
                        data,
                    )
                results.append(result)
            return results
        raise Exception(f'Elastic type {es_field_conf["type"]} is not yet implemented')

    @staticmethod
    def convert_entities_to_docs(
        entity_type_names: typing.Dict,
        es_data_config: typing.List,
        entities: typing.Dict,
    ) -> typing.Dict:
        docs = {}
        for entity_id, entity in entities.items():
            doc = {}
            for es_field_conf in es_data_config:
                doc[es_field_conf['system_name']] = BaseElasticsearch.convert_field(
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
            # TODO: does this need to be added for all text fields?
            if es_field_conf['type'] == 'text':
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