from typing import Dict

from elasticsearch import Elasticsearch as ES
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
# from elasticsearch_dsl import Document, Nested, Text
# from elasticsearch_dsl.connections import connections
from uuid import uuid4

from app.config import ELASTICSEARCH
from app.utils import dtu, RE_FIELD_DEF_CONVERSION, RE_FIELD_DEF_REL_ENT_CONVERSION

# TODO: use async elasticsearch lib
# problem with elasticsearch-py-async: no support for bulk operations
# https://github.com/elastic/elasticsearch-py-async/issues/5


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
    def convert_value(field_def: str, data: Dict) -> str:
        return RE_FIELD_DEF_CONVERSION.sub(
            lambda m: data[m.group(1)],
            field_def,
        )

    @staticmethod
    def convert_nested_value(relation: str, parts: Dict[str, Dict[str, str]], data: Dict) -> str:
        results = []
        if relation in data:
            for relation_item in data[relation]:
                result = {}
                for key, part_def in parts.items():
                    result[key] = RE_FIELD_DEF_REL_ENT_CONVERSION.sub(
                        lambda m: relation_item['e_props'][m.group(2)],
                        part_def['selector_value']
                    )
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
                # TODO: index integers as integers
                # TODO: index text without surrounding double quotes
                if es_field_conf['type'] == 'nested':
                    doc[es_field_conf['system_name']] = Elasticsearch.convert_nested_value(
                        es_field_conf['relation'],
                        es_field_conf['parts'],
                        entity,
                    )
                else:
                    doc[es_field_conf['system_name']] = Elasticsearch.convert_value(
                        es_field_conf['selector_value'],
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
        }

        for es_field_conf in es_data_config.values():
            mapping = {
                'type': es_field_conf['type'],
            }
            if es_field_conf['type'] == 'nested':
                mapping['properties'] = {}
                for key, part_def in es_field_conf['parts'].items():
                    mapping['properties'][key] = {
                        'type': part_def['type'],
                    }
            body['mappings'][entity_type_name]['properties'][es_field_conf['system_name']] = mapping

        response = self.es.indices.create(
            index=new_index_name,
            body=body,
        )

        if 'acknowledged' in response and response['acknowledged'] is True:
            return new_index_name

        raise Exception(response['error']['root_cause'])

    def switch_to_new_index(self, new_index_name: str, entity_type_id: int) -> None:
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
