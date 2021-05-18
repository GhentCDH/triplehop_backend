import copy
import json
import os
import re
import uuid

RE_PROPERTY_VALUE = re.compile(r'(?<![$])[$]([a-z_]+(?:->(?<![$])[$]([a-z_]+))*)')


def replace(project_config: dict, er: str, er_name: str, input: str):
    def replacer(match):
        # todo: more advanced properties on relations?
        # todo: relations with multiple domains / ranges?
        current_er = er_name
        parts = match.group(0).split('->')
        result = []
        for part in parts:
            # strip leading $
            part = part[1:]
            # relation
            if part[:2] == 'r_':
                if 'relations_base' not in project_config:
                    raise Exception('Relations ids, domains, ranges not available')
                relation_name = part[2:]
                result.append(f'$r_{project_config["relations_base"][relation_name]["id"]}')
                current_er = project_config["relations_base"][relation_name]['range']
            elif part[:3] == 'ri_':
                if 'relations_base' not in project_config:
                    raise Exception('Relations ids, domains, ranges not available')
                relation_name = part[3:]
                result.append(f'$ri_{project_config["relations_base"][relation_name]["id"]}')
                current_er = project_config["relations_base"][relation_name]['domain']
            else:
                result.append(f'${project_config[er][current_er]["lookup"][part]}')
        return '->'.join(result)

    return RE_PROPERTY_VALUE.sub(replacer, input)


for project_folder in os.listdir('human_readable_config'):
    project_config = {
        'entity': {},
        'relation': {},
    }
    # Load relation config: ids, domains and ranges might be needed when replacing
    if os.path.exists(f'human_readable_config/{project_folder}/relations.json'):
        with open(f'human_readable_config/{project_folder}/relations.json') as f:
            project_config['relations_base'] = json.load(f)
    # first iteration: data
    for er in ['entity', 'relation']:
        for fn in os.listdir(f'human_readable_config/{project_folder}/{er}'):
            name = fn.split('.')[0]
            project_config[er][name] = {}
            prev_field_lookup = {}
            with open(f'human_readable_config/{project_folder}/{er}/{fn}') as f:
                config = json.load(f)
            if os.path.exists(f'config/{project_folder}/{er}/{fn}'):
                with open(f'config/{project_folder}/{er}/{fn}') as f:
                    prev_config = json.load(f)
                    if 'data' in prev_config:
                        for field in prev_config['data']:
                            prev_field_lookup[prev_config['data'][field]['system_name']] = field
            if 'data' in config:
                project_config[er][name]['data'] = {}
                project_config[er][name]['lookup'] = {
                    'id': 'id',
                }
                for field in config['data']:
                    if field['system_name'] in prev_field_lookup:
                        id = prev_field_lookup[field['system_name']]
                    else:
                        id = str(uuid.uuid4())
                    project_config[er][name]['data'][id] = field
                    project_config[er][name]['lookup'][field['system_name']] = id

    # second iteraton: display
    for er in ['entity', 'relation']:
        for fn in os.listdir(f'human_readable_config/{project_folder}/{er}'):
            name = fn.split('.')[0]
            with open(f'human_readable_config/{project_folder}/{er}/{fn}') as f:
                config = json.load(f)
            if 'display' in config:
                project_config[er][name]['display'] = copy.deepcopy(config['display'])
                display = project_config[er][name]['display']
                if 'title' in display:
                    display['title'] = replace(project_config, er, name, display['title'])
                if 'layout' in display:
                    for layout in display['layout']:
                        if 'label' in layout:
                            layout['label'] = replace(project_config, er, name, layout['label'])
                        if 'fields' in layout:
                            for field in layout['fields']:
                                field['field'] = replace(project_config, er, name, field['field'])
    # third iteration: es_data, es_display
    for er in ['entity', 'relation']:
        for fn in os.listdir(f'human_readable_config/{project_folder}/{er}'):
            name = fn.split('.')[0]
            with open(f'human_readable_config/{project_folder}/{er}/{fn}') as f:
                config = json.load(f)
            if 'es_data' in config:
                project_config[er][name]['es_data'] = copy.deepcopy(config['es_data'])
                es_data = project_config[er][name]['es_data']
                for field in es_data:
                    if field['type'] == 'nested':
                        field['base'] = replace(project_config, er, name, field['base'])
                        for part in field['parts'].values():
                            part['selector_value'] = replace(project_config, er, name, part['selector_value'])
                    else:
                        field['selector_value'] = replace(project_config, er, name, field['selector_value'])
            if 'es_display' in config:
                project_config[er][name]['es_display'] = copy.deepcopy(config['es_display'])


    # write out config
    for er in ['entity', 'relation']:
        if not os.path.exists(f'config/{project_folder}/{er}'):
            os.makedirs(f'config/{project_folder}/{er}')
        for name in project_config[er]:
            with open(f'config/{project_folder}/{er}/{name}.json', 'w') as f:
                config = {}
                for conf in ['data', 'display', 'es_data', 'es_display']:
                    if conf in project_config[er][name]:
                        config[conf] = project_config[er][name][conf]
                json.dump(config, f, indent=4)

