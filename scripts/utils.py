from typing import Callable, Dict, List, Tuple

from tqdm import tqdm


def batch_process(cur, data: List, initial_parameters: Dict, method: Callable, *args, **kwargs):
    counter = 0
    batch_query = []
    batch_params = initial_parameters.copy()

    for row in tqdm(data):
        counter += 1
        result = method(counter, row, *args)
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


def add_entity(counter: int, row: Tuple, prop_conf: Dict):
    query = []
    params = {}
    # Get id and update entity_count
    query.append('''
    UPDATE app.entity_count
    SET current_id = current_id + 1
    WHERE id = %(entity_type_id)s;
    ''')

    # Create entity and initial revision
    properties = ['id: (SELECT current_id FROM app.entity_count WHERE id = %(entity_type_id)s)']
    for (key, indices) in prop_conf.items():
        if len(indices) == 3 and indices[2] == 'point':
            properties.append(f'p%(entity_type_id)s_%(property_id_{counter}_{indices[0]})s: ST_SetSRID(ST_MakePoint(%(value_{counter}_{indices[0]}_lon)s, %(value_{counter}_{indices[0]}_lat)s),4326)')
        elif row[indices[1]] != '':
            properties.append(f'p%(entity_type_id)s_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s')

    query.append('''
    CREATE
        (ve_{counter}:v%(entity_type_id)s {{{properties}}})
        -[:erevision]->
        (vr_{counter}:vrevision {{user_id: %(user_id)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))}});
    '''.format(counter=counter, properties=', '.join(properties)))

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        if isinstance(indices[1], list) or row[indices[1]] != '':
            query.append('''
            CREATE
                (ve_{counter})
                -[:eproperty]->
                (vp_{counter}_%(property_id_{counter}_{id})s:v%(entity_type_id)s_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            CREATE
                (vp_{counter}_%(property_id_{counter}_{id})s)
                -[:erevision]->
                (vr_{counter});
            '''.format(counter=counter, id=indices[0]))
            params[f'property_id_{counter}_{indices[0]}'] = indices[0]
            if len(indices) == 3:
                if indices[2] == 'int':
                    params[f'value_{counter}_{indices[0]}'] = int(row[indices[1]])
                elif indices[2] == 'point':
                    params[f'value_{counter}_{indices[0]}_lon'] = float(row[indices[1][0]])
                    params[f'value_{counter}_{indices[0]}_lat'] = float(row[indices[1][1]])
            else:
                params[f'value_{counter}_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only, including the last one, which is re-added later)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    return {
        'query': '\n'.join(query) + ';',
        'params': params
    }


def add_relation(counter: int, row: Tuple, relation_conf: List, prop_conf: Dict):
    query = []
    params = {}
    # Get id and update entity_count
    query.append('''
    UPDATE app.relation_count
    SET current_id = current_id + 1
    WHERE id = %(relation_type_id)s;
    ''')

    # Create relation, relation node and initial revision
    properties = ['id: (SELECT current_id FROM app.relation_count WHERE id = %(relation_type_id)s)']
    # for (key, indices) in prop_conf.items():
    #     if row[indices[1]] != '':
    #         properties.append(f'p%(entity_type_id)s_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s')

    query.append('''
    MATCH
        (d_{counter}:v%(domain_type_id)s {{%(domain_prop)s: %(domain_id_{counter})s}}),
        (r_{counter}:v%(range_type_id)s {{%(range_prop)s: %(range_id_{counter})s}})
    CREATE
        (d_{counter})-[:e%(relation_type_id)s {{{properties}}}]->(r_{counter});
    '''.format(counter=counter, properties=', '.join(properties)))

    params[f'domain_id_{counter}'] = int(row[relation_conf[0]])
    params[f'range_id_{counter}'] = int(row[relation_conf[1]])

    # TODO: add relation node, add revision, add properties

    # # Add properties and corresponding relations
    # for (key, indices) in prop_conf.items():
    #     if row[indices[1]] != '':
    #         query.append('''
    #         CREATE
    #             (ve_{counter})
    #             -[:eproperty]->
    #             (vp_{counter}_%(property_id_{counter}_{id})s:v%(entity_type_id)s_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
    #         CREATE
    #             (vp_{counter}_%(property_id_{counter}_{id})s)
    #             -[:erevision]->
    #             (vr_{counter});
    #         '''.format(counter=counter, id=indices[0]))
    #         params[f'domain_id_{counter}'] = relation_conf[0]
    #         params[f'range_id_{counter}'] = relation_conf[1]
    #         # params[f'value_{counter}_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only, except for the last one, which is re-added later)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    return {
        'query': '\n'.join(query) + ';',
        'params': params
    }
