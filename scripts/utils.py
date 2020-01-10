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
    # Get id and update entityCount
    cur.execute('''
    SELECT entityCount.currentId
    FROM app.entityCount
    WHERE entityCount.id = %(entityId)s;
    ''', {
        'entityId': entityId,
    })
    newId = cur.fetchone()[0] + 1

    cur.execute('''
    UPDATE app.entityCount
    SET currentId = %(id)s
    WHERE id = %(entityId)s;
    ''', {
        'entityId': entityId,
        'id': newId,
    })

    # Verify the entity doen't exist
    cur.execute('''
    MATCH (ve:v%(entityId)s {id: %(id)s}) RETURN ve;
    ''', {
        'entityId': entityId,
        'id': newId,
    })
    if cur.rowcount > 0:
        # TODO: create a more usefull error message
        raise Exception('Entity already exists.')
    
    # Create entity and initial revision
    cur.execute('''
    CREATE 
        (:v%(entityId)s {id: %(id)s})
        -[:erevision]->
        (:vrevision {user_id: %(userId)s, revision_id: 1, timestamp: (SELECT EXTRACT(EPOCH FROM NOW()))})
    ;
    ''', {
        'entityId': entityId,
        'id': newId,
        'userId': userId,
    })

    # Add properties and corresponding relations
    for (key, indices) in propConf.items():
        if row[indices[1]] != '':
            cur.execute('''
            MATCH (ve:v%(entityId)s {id: %(id)s})
                SET ve.%(propertyName)s = %(value)s
            ;
            MATCH (ve:v%(entityId)s {id: %(id)s})
                CREATE (ve)-[:eproperty]->(:v%(entityId)s_%(propertyId)s {value: %(value)s})
            ;
            
            MATCH
                (ve:v%(entityId)s {id: %(id)s})-[:eproperty]->(vp:v%(entityId)s_%(propertyId)s),
                (ve:v%(entityId)s {id: %(id)s})-[:erevision]->(vr:vrevision)
                CREATE (vp)-[:erevision]->(vr)
            ;
            ''', {
                'id': newId,
                'entityId': entityId,
                'propertyId': indices[0],
                'propertyName': key,
                'value': row[indices[1]],
            })