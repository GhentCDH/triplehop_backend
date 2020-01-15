from typing import Dict, Tuple

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

def addEntity(cur, propConf: Dict, userId: int, entityId: int, row: Tuple):
    query = []
    params = {
        'entityId': entityId,
        'userId': userId,
    }

    # Get id and update entityCount
    query.append('''
    UPDATE app.entityCount
    SET currentId = currentId + 1
    WHERE id = %(entityId)s;
    ''')

    # Create entity and initial revision
    query.append('''
    CREATE 
        (ve:v%(entityId)s {id: (SELECT currentId FROM app.entityCount WHERE id = %(entityId)s)})
        -[:erevision]->
        (vr:vrevision {user_id: %(userId)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))});
    ''')

    # Add properties and corresponding relations
    for (key, indices) in propConf.items():
        if row[indices[1]] != '':
            query.append('''
            SET ve.%(propertyName_{id})s = %(value_{id})s
            CREATE (ve)-[:eproperty]->(vp_%(propertyId_{id})s:v%(entityId)s_%(propertyId_{id})s {{value: %(value_{id})s}})
            CREATE (vp_%(propertyId_{id})s)-[:erevision]->(vr);
            '''.format(id=indices[0]))
            params[f'propertyId_{indices[0]}'] = indices[0]
            params[f'propertyName_{indices[0]}'] = key
            params[f'value_{indices[0]}'] = row[indices[1]]

    # remove semicolons (present for code readibility only)
    query = [q.replace(';', '') if i > 0 else q for i, q in enumerate(query)]

    cur.execute(
        '\n'.join(query) + ';',
        params
    )