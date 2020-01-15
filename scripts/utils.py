from typing import Dict, List, Tuple

def stringOrNone(input: str):
    '''
    Returns the input string if it is not empty, else None.
    '''
    if input == '':
        return None

    return input

def intOrNone(input: str):
    '''
    Returns an int cast of the input string if it is not empty, else None.
    '''
    if input == '':
        return None
        
    return int(input)

def addEntity(batchQuery: List, params: Dict, propConf: Dict, row: Tuple, counter: int):
    entityQuery = []
    # Get id and update entityCount
    entityQuery.append('''
    UPDATE app.entityCount
    SET currentId = currentId + 1
    WHERE id = %(entityId)s;
    ''')

    # Create entity and initial revision
    properties = ''
    for (key, indices) in propConf.items():
        if row[indices[1]] != '':
            properties += f', %(propertyName_{counter}_{indices[0]})s: %(value_{counter}_{indices[0]})s'

    entityQuery.append('''
    CREATE 
        (ve_{counter}:v%(entityId)s {{id: (SELECT currentId FROM app.entityCount WHERE id = %(entityId)s){properties}}})
        -[:erevision]->
        (vr_{counter}:vrevision {{user_id: %(userId)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))}});
    '''.format(counter=counter,properties=properties))

    # Add properties and corresponding relations
    for (key, indices) in propConf.items():
        if row[indices[1]] != '':
            entityQuery.append('''
            CREATE
                (ve_{counter})
                -[:eproperty]->
                (vp_{counter}_%(propertyId_{counter}_{id})s:v%(entityId)s_%(propertyId_{counter}_{id})s {{value: %(value_{counter}_{id})s}})
            CREATE
                (vp_{counter}_%(propertyId_{counter}_{id})s)
                -[:erevision]->
                (vr_{counter});
            '''.format(counter=counter,id=indices[0]))
            params[f'propertyId_{counter}_{indices[0]}'] = indices[0]
            params[f'propertyName_{counter}_{indices[0]}'] = key
            params[f'value_{counter}_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only)
    entityQuery = [q.replace(';', '') if i > 0 else q for i, q in enumerate(entityQuery)]

    batchQuery.append('\n'.join(entityQuery) + ';')