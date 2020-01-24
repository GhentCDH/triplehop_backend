from typing import Dict, List, Tuple

def add_entity(batch_query: List, params: Dict, prop_conf: Dict, row: Tuple, counter: int):
    entity_query = []
    # Get id and update entity_count
    entity_query.append('''
    UPDATE app.entity_count
    SET current_id = current_id + 1
    WHERE id = %(entity_id)s;
    ''')

    # Create entity and initial revision
    properties = ''
    for (key, indices) in prop_conf.items():
        if row[indices[1]] != '':
            properties += f', p%(entity_id)s_%(property_id_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s'

    entity_query.append('''
    CREATE
        (ve_{counter}:v%(entity_id)s {{id: (SELECT current_id FROM app.entity_count WHERE id = %(entity_id)s){properties}}})
        -[:erevision]->
        (vr_{counter}:vrevision {{user_id: %(user_id)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))}});
    '''.format(counter=counter,properties=properties))

    # Add properties and corresponding relations
    for (key, indices) in prop_conf.items():
        if row[indices[1]] != '':
            entity_query.append('''
            CREATE
                (ve_{counter})
                -[:eproperty]->
                (vp_{counter}_%(property_id_{counter}_{id})s:v%(entity_id)s_%(property_id_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            CREATE
                (vp_{counter}_%(property_id_{counter}_{id})s)
                -[:erevision]->
                (vr_{counter});
            '''.format(counter=counter,id=indices[0]))
            params[f'property_id_{counter}_{indices[0]}'] = indices[0]
            params[f'property_name_{counter}_{indices[0]}'] = key
            params[f'value_{counter}_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only)
    entity_query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(entity_query)]

    batch_query.append('\n'.join(entity_query) + ';')
