import json
import os
import re
import uuid

RE_PROPERTY_VALUE = re.compile(r'(?<![$])[$]([a-z_]+(?:->(?<![$])[$]([a-z_]+))*)')


def replace(project_config: dict, entity_name: str, input: str):
    def replacer(match):
        # todo: test properties through relations
        # todo: relations with multiple domains / ranges?
        current_entity = entity_name
        parts = match.group(0).split('->')
        result = []
        for part in parts:
            # strip leading $
            part = part[1:]
            # relation
            if part[:2] == 'r_':
                result.append(f'${project_config["relation"][part[2:]["id"]]}')

    return RE_PROPERTY_VALUE.sub(replacer, input)


for project_folder in os.listdir('human_readable_config'):
    project_config = {
        'entity': {},
        'relation': {},
    }
    # first iteration: data
    for er in ['entity', 'relation']:
        for name in os.listdir(f'human_readable_config/{project_folder}/{er}'):
            project_config[er][name] = {}
            with open(f'human_readable_config/{project_folder}/{er}/{name}') as f:
                config = json.load(f)
            if 'data' in config:
                project_config[er][name]['data'] = {}
                project_config[er][name]['lookup'] = {}
                for data_field in config['data']:
                    id = str(uuid.uuid4())
                    project_config[er][name]['data'][id] = data_field
                    project_config[er][name]['lookup'][data_field['system_name']] = id
        # for relation_name in os.listdir(f'human_readable_config/{project_folder}/relation'):
        #     with open(f'human_readable_config/{project_folder}/relation/{relation_name}') as f:
        #         relation_config = json.load(f)
        #     if 'data' in relation_config:
        #         project_config['relation']['data'] = {}
        #         for data_field in relation_config['data']:
        #             id = str(uuid.uuid4())
        #             project_config['relation']['data'][id] = data_field
        #             project_config['relation']['lookup'][data_field['system_name']] = id

        # # second iteraton: display
        # for entity_name in os.listdir(f'human_readable_config/{project_folder}/entity'):
        #     with open(f'human_readable_config/{project_folder}/entity/{entity_name}') as f:
        #         entity_config = json.load(f)
        #     if 'display' in entity_config:
        #         project_config['entity']['display'] = {
        #             'title': replace(project_config, entity_name, entity_config['display']['title'])
        #         }


        #         for display in entity_config['data']:
        #             project_config['entity']['data'][str(uuid.uuid4())] = data_field
        # for relation_name in os.listdir(f'human_readable_config/{project_folder}/relation'):
        #     relation_config = json.load(f'human_readable_config/{project_folder}/relation/{relation_name}')
        #     if 'data' in relation_config:
        #         project_config['entity']['data'] = {}
        #     for data_field in entity_config['data']:
        #         project_config['entity']['data'][str(uuid.uuid4())] = data_field

