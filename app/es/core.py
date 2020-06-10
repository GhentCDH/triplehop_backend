from typing import Any, Dict, List

from elasticsearch import Elasticsearch as ES
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
from uuid import uuid4

from app.config import ELASTICSEARCH
from app.utils import dtu, RE_FIELD_DEF_CONVERSION, RE_FIELD_DEF_REL_ENT_CONVERSION

# TODO: use async elasticsearch lib
# problem with elasticsearch-py-async: no support for bulk operations
# https://github.com/elastic/elasticsearch-py-async/issues/5

MAX_RESULT_WINDOW = 10000
DEFAULT_FROM = 0
DEFAULT_SIZE = 10
SCROLL_SIZE = 1000


class Elasticsearch():
    def __init__(self) -> None:
        self.es = ES(ELASTICSEARCH['hosts'])

    @staticmethod
    def extract_query_from_es_data_config(es_data_config: Dict) -> Dict:
        combined_fields = set()
        for es_field_conf in es_data_config.values():
            if es_field_conf['type'] == 'nested':
                for part in es_field_conf['parts'].values():
                    combined_fields.add(part['selector_value'])
            else:
                combined_fields.add(es_field_conf['selector_value'])

        es_query = {
            'props': set(),
            'relations': {},
        }

        # TODO: correctly extract fields, a.o. multiple original data fields in a single es field
        # (e.g., "$first_name $last_name")
        # TODO: more relation levels
        # TODO: props on relation itself
        for combined_field in combined_fields:
            p_split = combined_field.split('.')
            pe_split = combined_field.split('->')
            if len(p_split) == 1 and len(pe_split) == 1:
                es_query['props'].add(combined_field[1:])
            elif len(pe_split) == 2:
                relation = pe_split[0][1:]
                if relation not in es_query['relations']:
                    es_query['relations'][relation] = {
                        'r_props': set(),
                        'e_props': set(),
                    }
                es_query['relations'][relation]['e_props'].add(pe_split[1][1:])

        return es_query

    @staticmethod
    def str_value(data_item: Any) -> str:
        if data_item is None:
            return ''
        return str(data_item)

    @staticmethod
    def cast(type: str, str_repr: str) -> Any:
        if str_repr == '':
            return None
        if type == 'integer':
            return int(str_repr)
        return str_repr

    @staticmethod
    def construct_value(field_def: str, type: str, data: Dict[str, Any]) -> Any:
        """Construct the elasticsearch field data from the field definition and entity date."""
        str_repr = RE_FIELD_DEF_CONVERSION.sub(
            lambda m: Elasticsearch.str_value(data[m.group(1)]),
            field_def,
        )
        return Elasticsearch.cast(type, str_repr)

    @staticmethod
    def construct_nested_value(relation: str, parts: Dict[str, Dict[str, str]], data: Dict) -> List[Dict[str, Any]]:
        results = []
        if relation in data:
            for relation_item in data[relation]:
                result = {
                    'entity_type_name': relation_item['entity_type_name'],
                }
                for key, part_def in parts.items():
                    str_repr = RE_FIELD_DEF_REL_ENT_CONVERSION.sub(
                        lambda m: Elasticsearch.str_value(relation_item['e_props'][m.group(2)]),
                        part_def['selector_value']
                    )
                    result[key] = Elasticsearch.cast(part_def['type'], str_repr)
                results.append(result)
        return results

    @staticmethod
    def convert_entities_to_docs(es_data_config: Dict, entities: Dict) -> Dict:
        docs = {}
        for entity_id, entity in entities.items():
            doc = {}
            for es_field_conf in es_data_config.values():
                # TODO: correctly extract fields, a.o. multiple original data fields in a single es field
                # (e.g., "$first_name $last_name")
                # TODO: more relation levels
                # TODO: props on relation itself
                if es_field_conf['type'] == 'nested':
                    doc[es_field_conf['system_name']] = Elasticsearch.construct_nested_value(
                        es_field_conf['relation'],
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

    def create_new_index(self, entity_type_name: str, es_data_config: Dict) -> str:
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
                    'normalizer': {
                        'icu_normalizer': {
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
                    }
                }
            if es_field_conf['type'] == 'nested':
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
        except NotFoundError:
            pass

        response = self.es.indices.update_aliases(
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return

        raise Exception(response['error']['root_cause'])

    def add_bulk(self, index_name: str, entity_type_name: str, data: Dict) -> None:
        actions = [
            {
                '_index': index_name,
                '_type': entity_type_name,
                '_id': i,
                '_source': v,
            }
            for i, v in data.items()
        ]
        bulk(self.es, actions)

    def search(self, entity_type_id: str, body: Dict) -> Dict:
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
                print(current_from)
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
