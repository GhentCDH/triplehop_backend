from typing import Callable, Dict, List, Tuple


def batch_process(cur, data: List, initial_parameters: Dict, method: Callable, *args, **kwargs):
    counter = 0
    batch_query = []
    batch_params = initial_parameters.copy()

    for row in data:
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
    entity_query = []
    params = {}
    # Get id and update entity_count
    entity_query.append('''
    UPDATE app.entity_count
    SET current_id = current_id + 1
    WHERE id = %(entity_type_id)s;
    ''')

    # Create entity and initial revision
    properties = ['id: (SELECT current_id FROM app.entity_count WHERE id = %(entity_type_id)s)']
    for (key, indices) in prop_conf.items():
        if row[indices[1]] != '':
            properties.append(f'p%(entity_type_id)s_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s')

    entity_query.append('''
    CREATE
        (ve_{counter}:v%(entity_type_id)s {{{properties}}})
        -[:erevision]->
        (vr_{counter}:vrevision {{user_id: %(user_id)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))}});
    '''.format(counter=counter, properties=', '.join(properties)))

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        if row[indices[1]] != '':
            entity_query.append('''
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
            params[f'property_name_{counter}_{indices[0]}'] = key
            params[f'value_{counter}_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only, except for the last one, which is re-added later)
    entity_query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(entity_query)]

    return {
        'query': '\n'.join(entity_query) + ';',
        'params': params
    }
