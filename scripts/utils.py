from typing import Callable, Dict, List, Tuple

from tqdm import tqdm


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def batch_process(cur, data: List, initial_parameters: Dict, method: Callable, *args, **kwargs):
    counter = 0
    batch_query = []
    batch_params = initial_parameters.copy()

    for row in tqdm(data):
        counter += 1
        result = method(initial_parameters, counter, row, *args)
        batch_query.append(result['query'])
        batch_params.update(result['params'])

        # execute queries in batches
        if not counter % 500:
            cur.execute(
                '\n'.join(batch_query),
                batch_params
            )
            batch_query = []
            batch_params = initial_parameters.copy()

    # execute remaining queries
    if len(batch_query):
        cur.execute(
            '\n'.join(batch_query),
            batch_params
        )


def add_entity(initial_parameters: Dict, counter: int, row: Tuple, prop_conf: Dict):
    query = []
    params = {}

    # Create entity and initial revision

    properties = []
    if 'id' in prop_conf:
        query.append(
            '''
                UPDATE app.entity_count
                SET current_id = GREATEST(current_id, {prop_id})
                WHERE id = %(entity_type_id)s;
            '''.format(
                prop_id=int(row[prop_conf['id'][1]]),
            )
        )
        properties.append(f'id: %(value_{counter}_id)s')
    else:
        # Get id and update entity_count
        query.append(
            '''
                UPDATE app.entity_count
                SET current_id = current_id + 1
                WHERE id = %(entity_type_id)s;
            '''
        )
        properties.append('id: (SELECT entity_count.current_id FROM app.entity_count WHERE entity_count.id = %(entity_type_id)s)')

    for (key, indices) in prop_conf.items():
        if key == 'id':
            continue
        if len(indices) == 3 and indices[2] == 'point':
            if row[indices[1][0]] != '' and row[indices[1][1]] != '':
                properties.append(f'p_{dtu(initial_parameters["entity_type_id"])}_%(property_id_{counter}_{indices[0]})s: ST_SetSRID(ST_MakePoint(%(value_{counter}_{indices[0]}_lon)s, %(value_{counter}_{indices[0]}_lat)s),4326)')
        elif len(indices) == 3 and indices[2] == 'array':
            # TODO
            raise Exception('Not yet implemented')
        elif row[indices[1]] != '':
            properties.append(f'p_{dtu(initial_parameters["entity_type_id"])}_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s')

    query.append(
        '''
            CREATE
                (ve_{counter}:v_{entity_type_id} {{{properties}}})
                -[:e_revision]->
                (vr_{counter}:v_revision {{user_id: %(user_id)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))}});
        '''.format(
            counter=counter,
            entity_type_id=dtu(initial_parameters['entity_type_id']),
            properties=', '.join(properties),
        )
    )

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        if key == 'id':
            params[f'value_{counter}_id'] = int(row[indices[1]])
            continue

        valid = False
        if len(indices) == 3 and indices[2] == 'point':
            if row[indices[1][0]] != '' and row[indices[1][1]] != '':
                valid = True
                # query.append(
                #     '''
                #         CREATE
                #             (ve_{counter})
                #             -[:e_property]->
                #             (vp_{counter}_%(property_id_{counter}_{id})s:v_{entity_type_id}_%(property_id_{counter}_{id})s {{value: ST_SetSRID(ST_MakePoint(%(value_{counter}_{id}_lon)s, %(value_{counter}_{id}_lat)s),4326)}})
                #     '''.format(
                #         counter=counter,
                #         entity_type_id=dtu(initial_parameters['entity_type_id']),
                #         id=indices[0],
                #     )
                # )
                params[f'value_{counter}_{indices[0]}_lon'] = float(row[indices[1][0]])
                params[f'value_{counter}_{indices[0]}_lat'] = float(row[indices[1][1]])

        elif len(indices) == 3 and indices[2] == 'array':
            # TODO
            raise Exception('Not yet implemented')

        elif row[indices[1]] != '':
            valid = True
            # query.append(
            #     '''
            #         CREATE
            #             (ve_{counter})
            #             -[:e_property]->
            #             (vp_{counter}_%(property_id_{counter}_{id})s:v_{entity_type_id}_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            #     '''.format(
            #         counter=counter,
            #         entity_type_id=dtu(initial_parameters['entity_type_id']),
            #         id=indices[0],
            #     )
            # )
            if len(indices) == 3 and indices[2] == 'int':
                params[f'value_{counter}_{indices[0]}'] = int(row[indices[1]])
            else:
                params[f'value_{counter}_{indices[0]}'] = row[indices[1]].replace('\n', '\\n')

        if valid:
            # query.append(
            #     '''
            #         CREATE
            #             (vp_{counter}_%(property_id_{counter}_{id})s)
            #             -[:e_revision]->
            #             (vr_{counter});
            #     '''.format(
            #         counter=counter,
            #         id=indices[0],
            #     )
            # )
            params[f'property_id_{counter}_{indices[0]}'] = indices[0]

    # remove semicolons (present for code readibility only, including the last one, which is re-added later)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    return {
        'query': '\n'.join(query) + ';',
        'params': params
    }


def update_entity(initial_parameters: Dict, counter: int, row: Tuple, prop_conf: Dict):
    query = []
    params = {}

    properties = []

    for (key, indices) in prop_conf.items():
        if key == 'id':
            continue
        if len(indices) == 3 and indices[2] == 'point' and row[indices[1][0]] != '' and row[indices[1][1]] != '':
            properties.append({
                'name': f'p_{dtu(initial_parameters["entity_type_id"])}_%(property_id_{counter}_{indices[0]})s',
                'value': f'ST_SetSRID(ST_MakePoint(%(value_{counter}_{indices[0]}_lon)s, %(value_{counter}_{indices[0]}_lat)s),4326)'
            })
        elif len(indices) == 3 and indices[2] == 'array' and row[indices[1]] != '':
            properties.append({
                'name': f'p_{dtu(initial_parameters["entity_type_id"])}_%(property_id_{counter}_{indices[0]})s',
                'value': f'%(value_{counter}_{indices[0]})s',
                'type': 'array',
            })
        elif row[indices[1]] != '':
            properties.append({
                'name': f'p_{dtu(initial_parameters["entity_type_id"])}_%(property_id_{counter}_{indices[0]})s',
                'value': f'%(value_{counter}_{indices[0]})s',
            })

    queryPart = '''
        MATCH
            (ve_{counter}:v_{entity_type_id} {{id: %(value_{counter}_{id_prop_id})s}})
    '''.format(
        counter=counter,
        entity_type_id=dtu(initial_parameters['entity_type_id']),
        id_prop_id=prop_conf['id'][0]
    )
    for prop in properties:
        if 'type' in prop and prop['type'] == 'array':
            queryPart += '''
                SET ve_{counter}.{property_name} = COALESCE(ve_{counter}.{property_name}, []) + {property_value}
            '''.format(
                counter=counter,
                property_name=prop['name'],
                property_value=prop['value'],
            )
        else:
            queryPart += '''
                SET ve_{counter}.{property_name} = {property_value}
            '''.format(
                counter=counter,
                property_name=prop['name'],
                property_value=prop['value'],
            )

    query.append(queryPart)

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        if key == 'id':
            params[f'value_{counter}_{indices[0]}'] = int(row[indices[1]])

        valid = False
        if len(indices) == 3 and indices[2] == 'point' and row[indices[1][0]] != '' and row[indices[1][1]] != '':
            valid = True
            # query.append(
            #     '''
            #         CREATE
            #             (ve_{counter})
            #             -[:e_property]->
            #             (vp_{counter}_%(property_id_{counter}_{id})s:v_{entity_type_id}_%(property_id_{counter}_{id})s {{value: ST_SetSRID(ST_MakePoint(%(value_{counter}_{id}_lon)s, %(value_{counter}_{id}_lat)s),4326)}})
            #     '''.format(
            #         counter=counter,
            #         entity_type_id=dtu(initial_parameters['entity_type_id']),
            #         id=indices[0],
            #     )
            # )
            params[f'value_{counter}_{indices[0]}_lon'] = float(row[indices[1][0]])
            params[f'value_{counter}_{indices[0]}_lat'] = float(row[indices[1][1]])

        elif len(indices) == 3 and indices[2] == 'array' and row[indices[1]] != '':
            valid = True
            # TODO: add properties and corresponding relations
            # query.append(
            #     '''
            #         CREATE
            #             (ve_{counter})
            #             -[:e_property]->
            #             (vp_{counter}_%(property_id_{counter}_{id})s:v_{entity_type_id}_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            #     '''.format(
            #         counter=counter,
            #         entity_type_id=dtu(initial_parameters['entity_type_id']),
            #         id=indices[0],
            #     )
            # )
            params[f'value_{counter}_{indices[0]}'] = row[indices[1]].replace('\n', '\\n')

        elif row[indices[1]] != '':
            valid = True
            # query.append(
            #     '''
            #         CREATE
            #             (ve_{counter})
            #             -[:e_property]->
            #             (vp_{counter}_%(property_id_{counter}_{id})s:v_{entity_type_id}_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            #     '''.format(
            #         counter=counter,
            #         entity_type_id=dtu(initial_parameters['entity_type_id']),
            #         id=indices[0],
            #     )
            # )
            if len(indices) == 3 and indices[2] == 'int':
                params[f'value_{counter}_{indices[0]}'] = int(row[indices[1]])
            else:
                params[f'value_{counter}_{indices[0]}'] = row[indices[1]].replace('\n', '\\n')

        if valid:
            # query.append(
            #     '''
            #         CREATE
            #             (vp_{counter}_%(property_id_{counter}_{id})s)
            #             -[:e_revision]->
            #             (vr_{counter});
            #     '''.format(
            #         counter=counter,
            #         id=indices[0],
            #     )
            # )
            params[f'property_id_{counter}_{indices[0]}'] = indices[0]

    # remove semicolons (present for code readibility only, including the last one, which is re-added later)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    return {
        'query': '\n'.join(query) + ';',
        'params': params
    }


def add_relation(initial_parameters: Dict, counter: int, row: Tuple, relation_conf: List, prop_conf: Dict):
    query = []
    params = {}

    # Get id and update entity_count
    query.append(
        '''
            UPDATE app.relation_count
            SET current_id = current_id + 1
            WHERE id = %(relation_type_id)s;
        '''
    )

    # Create relation, relation node and initial revision
    properties = ['id: (SELECT current_id FROM app.relation_count WHERE id = %(relation_type_id)s)']
    for (key, indices) in prop_conf.items():
        if row[indices[1]] != '':
            properties.append(f'p_{dtu(initial_parameters["relation_type_id"])}_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s')

    query.append(
        '''
            MATCH
                (d_{counter}:v_{domain_type_id} {{%(domain_prop)s: %(domain_id_{counter})s}}),
                (r_{counter}:v_{range_type_id} {{%(range_prop)s: %(range_id_{counter})s}})
            CREATE
                (d_{counter})-[:e_{relation_type_id} {{{properties}}}]->(r_{counter});
        '''.format(
            counter=counter,
            domain_type_id=dtu(initial_parameters['domain_type_id']),
            range_type_id=dtu(initial_parameters['range_type_id']),
            relation_type_id=dtu(initial_parameters['relation_type_id']),
            properties=', '.join(properties),
        )
    )

    if len(relation_conf[0]) == 2 and relation_conf[0][1] == 'int':
        params[f'domain_id_{counter}'] = int(row[relation_conf[0][0]])
    else:
        params[f'domain_id_{counter}'] = row[relation_conf[0][0]]
    if len(relation_conf[1]) == 2 and relation_conf[1][1] == 'int':
        params[f'range_id_{counter}'] = int(row[relation_conf[1][0]])
    else:
        params[f'range_id_{counter}'] = row[relation_conf[1][0]]

    # TODO: add relation node, add revision, add properties

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        valid = False
        if row[indices[1]] != '':
            valid = True
            # query.append('''
            # CREATE
            #     (ve_{counter})
            #     -[:e_property]->
            #     (vp_{counter}_%(property_id_{counter}_{id})s:v%(entity_type_id)s_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            # CREATE
            #     (vp_{counter}_%(property_id_{counter}_{id})s)
            #     -[:e_revision]->
            #     (vr_{counter});
            # '''.format(counter=counter, id=indices[0]))
            # params[f'domain_id_{counter}'] = relation_conf[0]
            # params[f'range_id_{counter}'] = relation_conf[1]
            if len(indices) == 3 and indices[2] == 'int':
                params[f'value_{counter}_{indices[0]}'] = int(row[indices[1]])
            else:
                params[f'value_{counter}_{indices[0]}'] = row[indices[1]].replace('\n', '\\n')

        if valid:
            # query.append(
            #     '''
            #         CREATE
            #             (vp_{counter}_%(property_id_{counter}_{id})s)
            #             -[:e_revision]->
            #             (vr_{counter});
            #     '''.format(
            #         counter=counter,
            #         id=indices[0],
            #     )
            # )
            params[f'property_id_{counter}_{indices[0]}'] = indices[0]

    # remove semicolons (present for code readibility only, except for the last one, which is re-added later)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    return {
        'query': '\n'.join(query) + ';',
        'params': params
    }
