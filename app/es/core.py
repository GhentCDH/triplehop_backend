import elasticsearch
# import json
import typing
from uuid import uuid4

from app.config import ELASTICSEARCH
from app.utils import dtu, RE_FIELD_CONVERSION

MAX_RESULT_WINDOW = 10000
DEFAULT_FROM = 0
DEFAULT_SIZE = 10
SCROLL_SIZE = 1000


class Elasticsearch():
    def __init__(self) -> None:
        self.es = elasticsearch.AsyncElasticsearch(**ELASTICSEARCH)

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
    def construct_value(field_def: str, type: str, data: typing.Dict[str, typing.Any]) -> typing.Any:
        """Construct the elasticsearch field data from the field definition and entity data."""
        # print(data)
        str_repr = RE_FIELD_DEF_CONVERSION.sub(
            lambda m: json.dumps(data[m.group(1)]),
            field_def,
        )
        return json.loads(str_repr)
        return 'test'

    @staticmethod
    def construct_nested_value(
        parts: typing.Dict[str, typing.Dict[str, str]],
        data: typing.Dict
    ) -> typing.List[typing.Dict[str, typing.Any]]:
        # # print(parts)
        # results = []
        # if relation in data:
        #     for relation_item in data[relation]:
        #         result = {
        #             'entity_type_name': relation_item['entity_type_name'],
        #         }
        #         for key, part_def in parts.items():
        #             str_repr = RE_FIELD_DEF_REL_ENT_CONVERSION.sub(
        #                 lambda m: json.dumps(relation_item['e_props'][m.group(2)]),
        #                 part_def['selector_value']
        #             )
        #             result[key] = json.loads(str_repr)
        #         results.append(result)
        # return results
        return []

    @staticmethod
    def convert_entities_to_docs(es_data_config: typing.List, entities: typing.Dict) -> typing.Dict:
        docs = {}
        print(entities)
        for entity_id, entity in entities.items():
            print(entity)
            doc = {}
            for es_field_conf in es_data_config:
                # print(es_field_conf)
                if es_field_conf['type'] == 'nested':
                    doc[es_field_conf['system_name']] = Elasticsearch.construct_nested_value(
                        es_field_conf['parts'],
                        entity,
                    )
                else:
                    doc[es_field_conf['system_name']] = Elasticsearch.construct_value(
                        es_field_conf['selector_value'],
                        es_field_conf['type'],
                        entity,
                    )

            docs[entity_id] = doc
        return docs

    def create_new_index(self, entity_type_name: str, es_data_config: typing.Dict) -> str:
        new_index_name = f'{ELASTICSEARCH["prefix"]}_{dtu(str(uuid4()))}'
        body = {
            'mappings': {
                entity_type_name: {
                    'dynamic': 'strict',
                    'properties': {},
                },
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

        for es_field_conf in es_data_config.values():
            mapping = {
                'type': es_field_conf['type'],
            }
            # TODO: does this need to be added for all text fields?
            if es_field_conf['type'] == 'text':
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
            elif es_field_conf['type'] == 'nested':
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
            body['mappings'][entity_type_name]['properties'][es_field_conf['system_name']] = mapping

        response = self.es.indices.create(
            index=new_index_name,
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return new_index_name

        raise Exception(response['error']['root_cause'])

    def switch_to_new_index(self, new_index_name: str, entity_type_id: str) -> None:
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
            response = self.es.indices.get(alias_name)
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

        response = self.es.indices.update_aliases(
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return

        raise Exception(response['error']['root_cause'])

    async def add_bulk(self, index_name: str, entity_type_name: str, data: typing.Dict) -> None:
        actions = [
            {
                '_index': index_name,
                '_type': entity_type_name,
                '_id': i,
                '_source': v,
            }
            for i, v in data.items()
        ]
        await elasticsearch.helpers.async_bulk(self.es, actions)

    def search(self, entity_type_id: str, body: typing.Dict) -> typing.Dict:
        alias_name = f'{ELASTICSEARCH["prefix"]}_{dtu(entity_type_id)}'
        body = {k: v for (k, v) in body.items() if v is not None}

        es_from = body['from'] if 'from' in body else DEFAULT_FROM
        es_size = body['size'] if 'size' in body else DEFAULT_SIZE
        if es_from + es_size <= MAX_RESULT_WINDOW:
            return self.es.search(
                index=alias_name,
                body=body,
            )
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

            data = self.es.search(
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

                data = self.es.scroll(scroll_id=sid, scroll='1m')
                sid = data['_scroll_id']
                scroll_size = len(data['hits']['hits'])
                current_from += SCROLL_SIZE

            return results
