import aiocache
import asyncpg
import buildpg
import json
import re
import typing

RE_PROPERTY_VALUE = re.compile(r'(?<![$])[$]([0-9a-z_]+)')
RENDERER = buildpg.main.Renderer(regex=r'(?<![a-z\\:]):([a-z][a-z0-9_]*)', sep='__')


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def read_config_from_file(project_name: str, type: str, name: str):
    with open(f'./config/{project_name}/{type}/{name}.json') as config_file:
        return config_file.read()


def render(query_template: str, params: typing.Dict[str, typing.Any] = None):
    if params is None:
        query, args = RENDERER(query_template)
    else:
        query, args = RENDERER(query_template, **params)
    query = query.replace('\\:', ':')
    return [query, args]


async def execute(
    conn: asyncpg.connection.Connection,
    query_template,
    params: typing.Dict[str, typing.Any] = None,
):
    query, args = render(query_template, params)
    await conn.execute(query, *args)


async def executemany(
    conn: asyncpg.connection.Connection,
    query_template,
    params: typing.Dict[str, typing.Any] = None,
):
    query, _ = render(query_template, params[0])
    args = [render(query_template, p)[1] for p in params]
    await conn.executemany(query, args)


async def fetchval(
    conn: asyncpg.connection.Connection,
    query_template,
    params: typing.Dict[str, typing.Any] = None,
):
    query, args = render(query_template, params)
    return await conn.fetchval(query, *args)


@aiocache.cached()
async def get_project_id(conn: asyncpg.connection.Connection, project_name: str) -> str:
    return await fetchval(
        conn,
        '''
            SELECT project.id::text
            FROM app.project
            WHERE project.system_name = :project_name;
        ''',
        {
            'project_name': project_name,
        }
    )


@aiocache.cached()
async def get_entity_type_id(conn: asyncpg.connection.Connection, project_name: str, entity_type_name: str) -> str:
    return await fetchval(
        conn,
        '''
            SELECT entity.id::text
            FROM app.entity
            INNER JOIN app.project
                ON entity.project_id = project.id
            WHERE project.system_name = :project_name
                AND entity.system_name = :entity_type_name;
        ''',
        {
            'project_name': project_name,
            'entity_type_name': entity_type_name,
        }
    )


@aiocache.cached()
async def get_relation_type_id(conn: asyncpg.connection.Connection, project_name: str, relation_type_name: str) -> str:
    return await fetchval(
        conn,
        '''
            SELECT relation.id::text
            FROM app.relation
            INNER JOIN app.project
                ON relation.project_id = project.id
            WHERE project.system_name = :project_name
                AND relation.system_name = :relation_type_name;
        ''',
        {
            'project_name': project_name,
            'relation_type_name': relation_type_name,
        }
    )


@aiocache.cached()
async def get_user_id(conn: asyncpg.connection.Connection, username: str) -> str:
    return await fetchval(
        conn,
        '''
            SELECT "user".id
            FROM app.user
            WHERE "user".username = :username;
        ''',
        {
            'username': username,
        }
    )


async def create_project_config(
    conn: asyncpg.connection.Connection,
    system_name: str,
    display_name: str,
    username: str,
):
    await execute(
        conn,
        '''
            INSERT INTO app.project (system_name, display_name, user_id)
            VALUES (
                :system_name,
                :display_name,
                (SELECT "user".id FROM app.user WHERE "user".username = :username)
            )
            ON CONFLICT DO NOTHING;
        ''',
        {
            'system_name': system_name,
            'display_name': display_name,
            'username': username,
        }
    )


async def create_entity_config(
    conn: asyncpg.connection.Connection,
    project_name: str,
    username: str,
    system_name: str,
    display_name: str,
    config: typing.Dict
):
    await execute(
        conn,
        '''
            INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
            VALUES (
                (SELECT project.id FROM app.project WHERE system_name = :project_name),
                :system_name,
                :display_name,
                :config,
                (SELECT "user".id FROM app.user WHERE "user".username = :username)
            )
            ON CONFLICT (project_id, system_name) DO UPDATE
            SET config = EXCLUDED.config;
        ''',
        {
            'project_name': project_name,
            'system_name': system_name,
            'display_name': display_name,
            'config': config,
            'username': username,
        }
    )
    await execute(
        conn,
        '''
            INSERT INTO app.entity_count (id)
            VALUES (
                (SELECT entity.id FROM app.entity WHERE system_name = :system_name)
            )
            ON CONFLICT (id) DO UPDATE
            SET current_id = 0;
        ''',
        {
            'system_name': system_name,
        }
    )


async def create_relation_config(
    conn: asyncpg.connection.Connection,
    project_name: str,
    username: str,
    system_name: str,
    display_name: str,
    config: typing.Dict,
    domains: typing.List,
    ranges: typing.List
):
    await execute(
        conn,
        '''
            INSERT INTO app.relation (project_id, system_name, display_name, config, user_id)
            VALUES (
                (SELECT project.id FROM app.project WHERE system_name = :project_name),
                :system_name,
                :display_name,
                :config,
                (SELECT "user".id FROM app.user WHERE "user".username = :username)
            )
            ON CONFLICT (project_id, system_name) DO UPDATE
            SET config = EXCLUDED.config;
        ''',
        {
            'project_name': project_name,
            'system_name': system_name,
            'display_name': display_name,
            'config': config,
            'username': username,
        }
    )
    for entity_type_name in domains:
        await execute(
            conn,
            '''
                INSERT INTO app.relation_domain (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT relation.id FROM app.relation WHERE system_name = :relation_name),
                    (SELECT entity.id FROM app.entity WHERE system_name = :entity_type_name),
                    (SELECT "user".id FROM app.user WHERE "user".username = :username)
                )
                ON CONFLICT DO NOTHING;
            ''',
            {
                'relation_name': system_name,
                'entity_type_name': entity_type_name,
                'username': username,
            }
        )
    for entity_type_name in ranges:
        await execute(
            conn,
            '''
                INSERT INTO app.relation_range (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT relation.id FROM app.relation WHERE system_name = :relation_name),
                    (SELECT entity.id FROM app.entity WHERE system_name = :entity_type_name),
                    (SELECT "user".id FROM app.user WHERE "user".username = :username)
                )
                ON CONFLICT DO NOTHING;
            ''',
            {
                'relation_name': system_name,
                'entity_type_name': entity_type_name,
                'username': username,
            }
        )
    await execute(
        conn,
        '''
            INSERT INTO app.relation_count (id)
            VALUES (
                (SELECT relation.id FROM app.relation WHERE system_name = :system_name)
            )
            ON CONFLICT (id) DO UPDATE
            SET current_id = 0;
        ''',
        {
            'system_name': system_name,
        }
    )


async def get_entity_props_lookup(
    conn: asyncpg.connection.Connection,
    project_name: str,
    entity_type_name: str,
) -> typing.Dict:
    result = await fetchval(
        conn,
        '''
            SELECT entity.config->'data'
            FROM app.entity
            INNER JOIN app.project
                ON entity.project_id = project.id
            WHERE project.system_name = :project_name
                AND entity.system_name = :entity_type_name;
        ''',
        {
            'project_name': project_name,
            'entity_type_name': entity_type_name,
        }
    )
    if result:
        return {v['system_name']: k for (k, v) in json.loads(result).items()}
    return {}


async def get_relation_props_lookup(
    conn: asyncpg.connection.Connection,
    project_name: str,
    relation_type_name: str,
) -> typing.Dict:
    result = await fetchval(
        conn,
        '''
            SELECT relation.config->'data'
            FROM app.relation
            INNER JOIN app.project
                ON relation.project_id = project.id
            WHERE project.system_name = :project_name
                AND relation.system_name = :relation_type_name;
        ''',
        {
            'project_name': project_name,
            'relation_type_name': relation_type_name,
        }
    )
    if result:
        return {v['system_name']: k for (k, v) in json.loads(result).items()}
    return {}


async def init_age(conn: asyncpg.connection.Connection):
    await execute(
        conn,
        '''
            SET search_path = ag_catalog, "$user", public;
        '''
    )
    await execute(
        conn,
        '''
            LOAD '$libdir/plugins/age';
        '''
    )


async def drop_project_graph(conn: asyncpg.connection.Connection, project_name: str):
    await execute(
        conn,
        '''
            SELECT drop_graph(
                (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text,
                true
            );
        ''',
        {
            'project_name': project_name,
        }
    )


async def create_project_graph(conn: asyncpg.connection.Connection, project_name: str):
    await execute(
        conn,
        '''
            SELECT create_graph(
                (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text
            );
        ''',
        {
            'project_name': project_name,
        }
    )


# If prefix is set, the placeholders should be prefixed, not the property keys themselves
def age_format_properties(properties: typing.Dict, prefix: str = ''):
    formatted_properties = {}
    prefix_id = 'id'
    if prefix != '':
        prefix_id = f'{prefix}_id'
    for (key, value) in properties.items():
        value_type = value['type']
        value_value = value['value']
        if key == 'id':
            if value_type == 'int':
                formatted_properties[prefix_id] = int(value_value)
                continue
            else:
                raise Exception('Non-int ids are not yet implemented')
        else:
            key = dtu(key)
        # TODO: postgis (https://github.com/apache/incubator-age/issues/48)
        if value_type in ['int', 'string', 'edtf', 'array', 'geometry']:
            formatted_properties[f'p_{key}'] = value_value
            continue
        raise Exception(f'Value type {value_type} is not yet implemented')
    return [
        ', '.join([f'{k if k != prefix_id else "id"}: ${k}' for k in formatted_properties.keys()]),
        formatted_properties,
    ]


def create_properties(
    row: typing.List,
    db_props_lookup: typing.Dict,
    file_header_lookup: typing.Dict,
    prop_conf: typing.Dict,
):
    properties = {}
    for (key, conf) in prop_conf.items():
        if key == 'id':
            db_key = 'id'
        else:
            db_key = db_props_lookup[key]
        if conf[0] == 'int':
            value = row[file_header_lookup[conf[1]]]
            if value in ['']:
                continue
            properties[db_key] = {
                'type': 'int',
                'value': int(value),
            }
            continue
        if conf[0] == 'string':
            value = row[file_header_lookup[conf[1]]]
            if value in ['']:
                continue
            properties[db_key] = {
                'type': 'string',
                'value': value,
            }
            continue
        if conf[0] == 'edtf':
            value = row[file_header_lookup[conf[1]]]
            if value in ['']:
                continue
            properties[db_key] = {
                'type': 'edtf',
                'value': value,
            }
            continue
        if conf[0] == 'array[string]':
            value = row[file_header_lookup[conf[1]]]
            if value in ['']:
                continue
            properties[db_key] = {
                'type': 'array',
                'value': value.split(conf[2]),
            }
            continue
        if conf[0] == 'geometry':
            value = row[file_header_lookup[conf[1]]]
            if value in ['']:
                continue
            properties[db_key] = {
                'type': 'geometry',
                'value': json.loads(value),
            }
            continue
        else:
            raise Exception(f'Type {conf[0]} has not yet been implemented')
    return properties


async def create_entity(
    conn: asyncpg.connection.Connection,
    row: typing.List,
    params: typing.Dict,
    db_props_lookup: typing.Dict,
    file_header_lookup: typing.Dict,
    prop_conf: typing.Dict
) -> None:
    project_id = await get_project_id(conn, params['project_name'])
    entity_type_id = await get_entity_type_id(conn, params['project_name'], params['entity_type_name'])
    properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)
    if 'id' in prop_conf:
        await execute(
            conn,
            '''
                UPDATE app.entity_count
                SET current_id = GREATEST(current_id, :entity_id)
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_id': properties['id']['value'],
                'entity_type_id': entity_type_id,
            }
        )
    else:
        await execute(
            conn,
            '''
                UPDATE app.entity_count
                SET current_id = current_id + 1
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id,
            }
        )
        id = await fetchval(
            conn,
            '''
                SELECT current_id
                FROM app.entity_count
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id,
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': id,
        }

    props = age_format_properties(properties)
    await execute(
        conn,
        (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$CREATE (\\:n_{dtu(entity_type_id)} {{{props[0]}}})$$, :params'
            f') as (a agtype);'
        ),
        {
            'params': json.dumps(props[1]),
        }
    )

    # TODO: revision, property relations


async def create_entities(
    conn: asyncpg.connection.Connection,
    params: typing.Dict,
    db_props_lookup: typing.Dict,
    file_header_lookup: typing.Dict,
    prop_conf: typing.Dict,
    batch: typing.List,
) -> None:
    project_id = await get_project_id(conn, params['project_name'])
    entity_type_id = await get_entity_type_id(conn, params['project_name'], params['entity_type_name'])

    if 'id' not in prop_conf:
        id = await fetchval(
            conn,
            '''
                SELECT current_id
                FROM app.entity_count
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id
            }
        )
    max_id = 0
    # key: placeholder string
    # value: typing.List with corresponding parameters
    props_collection = {}

    for row in batch:
        properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)
        if 'id' in prop_conf:
            max_id = max(max_id, properties['id']['value'])
        else:
            id += 1
            max_id = id
            properties['id'] = {
                'type': 'int',
                'value': id,
            }

        props = age_format_properties(properties)
        if props[0] in props_collection:
            props_collection[props[0]].append(props[1])
        else:
            props_collection[props[0]] = [props[1]]

    # GREATEST is needed when id in prop_conf
    await execute(
        conn,
        '''
            UPDATE app.entity_count
            SET current_id = GREATEST(current_id, :entity_id)
            WHERE id = :entity_type_id;
        ''',
        {
            'entity_id': max_id,
            'entity_type_id': entity_type_id
        }
    )

    for placeholder in props_collection:
        await executemany(
            conn,
            (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$CREATE (\\:n_{dtu(entity_type_id)} {{{placeholder}}})$$, :params'
                f') as (a agtype);'
            ),
            [{'params': json.dumps(params)} for params in props_collection[placeholder]]
        )

    # TODO: revision, property relations


# def age_format_properties_set(vertex: str, properties: typing.Dict):
#     formatted_properties = []
#     for (key, value) in properties.items():
#         value_type = value['type']
#         value_value = value['value']
#         if value_type == 'int':
#             formatted_properties.append(f'SET {vertex}.p_{key} = {value_value}')
#         elif value_type == 'string':
#             value_value = value_value.replace("'", "\\'")
#             formatted_properties.append(f"SET {vertex}.p_{key} = '{value_value}'")
#         elif value_type == 'array':
#             # https://github.com/apache/incubator-age/issues/44
#             raise Exception('Not implemented')
#         else:
#             raise Exception('Not implemented')
#     return ' '.join(formatted_properties)

#     return ' '.join({f'SET {vertex}.p_{k} = {v}' for (k, v) in properties.items()})


# def age_update_entity(project_id: str, entity_type_id: str, id: int, properties: typing.Dict):
#     return (
#         f'SELECT * FROM cypher('
#         f'\'{project_id}\', '
#         f'$$MATCH (n:e_{dtu(entity_type_id)} {{id: {id}}}) {age_format_properties_set("n", properties)}$$'
#         f') as (a agtype);'
#     )


# async def update_entity(
#     conn: asyncpg.connection.Connection,
#     row: typing.List,
#     params: typing.Dict,
#     db_props_lookup: typing.Dict,
#     file_header_lookup: typing.Dict,
#     prop_conf: typing.Dict
# ) -> None:
#     project_id = await get_project_id(conn, params['project_name'])
#     entity_type_id = await get_entity_type_id(conn, params['project_name'], params['entity_type_name'])
#     properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)

#     if properties:
#         entity_id = int(row[file_header_lookup[prop_conf['id'][0]]])
#         # https://github.com/apache/incubator-age/issues/43
#         # try SELECT * FROM cypher('testgraph', $$CREATE (:label $properties)$$, $1) as (a agtype);
#         # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
#         # databases.Database().execute leads to a prepared statement
#         # (see https://github.com/encode/databases/blob/master/databases/backends/postgres.py#L189)
#         # => use execute directly on asyncpg connection
#         async with db.connection() as conn:
#             await conn.raw_connection.execute(
#                 age_update_entity(project_id, dtu(entity_type_id), entity_id, properties)
#             )

#     # TODO: revision, property relations


async def create_relation(
    conn: asyncpg.connection.Connection,
    row: typing.List,
    params: typing.Dict,
    db_domain_props_lookup: typing.Dict,
    db_range_props_lookup: typing.Dict,
    db_props_lookup: typing.Dict,
    file_header_lookup: typing.Dict,
    domain_conf: typing.Dict,
    range_conf: typing.Dict,
    prop_conf: typing.Dict
) -> None:
    project_id = await get_project_id(conn, params['project_name'])
    relation_type_id = await get_relation_type_id(conn, params['project_name'], params['relation_type_name'])
    domain_type_id = await get_entity_type_id(conn, params['project_name'], params['domain_type_name'])
    range_type_id = await get_entity_type_id(conn, params['project_name'], params['range_type_name'])

    domain_properties = create_properties(row, db_domain_props_lookup, file_header_lookup, domain_conf)
    range_properties = create_properties(row, db_range_props_lookup, file_header_lookup, range_conf)
    properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)

    if 'id' in prop_conf:
        await execute(
            conn,
            '''
                UPDATE app.relation_count
                SET current_id = GREATEST(current_id, :relation_id)
                WHERE id = :relation_type_id;
            ''',
            {
                'entity_id': properties['id']['value'],
                'relation_type_id': relation_type_id,
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': int(row[file_header_lookup[prop_conf['id'][1]]]),
        }
    else:
        await execute(
            conn,
            '''
                UPDATE app.relation_count
                SET current_id = current_id + 1
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id,
            }
        )
        id = await fetchval(
            conn,
            '''
                SELECT current_id
                FROM app.relation_count
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id,
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': id,
        }

    domain_props = age_format_properties(domain_properties, 'domain')
    range_props = age_format_properties(range_properties, 'range')
    props = age_format_properties(properties)

    await execute(
        conn,
        (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH'
            f'        (d:n_{dtu(domain_type_id)} {{{domain_props[0]}}}),'
            f'        (r:n_{dtu(range_type_id)} {{{range_props[0]}}})'
            f' '
            f'CREATE'
            f'(d)-[\\:e_{dtu(relation_type_id)} {{{props[0]}}}]->(r)$$, :params'
            f') as (a agtype);'
        ),
        {
            'params': json.dumps({**domain_props[1], **range_props[1], **props[1]})
        }
    )


async def create_relations(
    conn: asyncpg.connection.Connection,
    params: typing.Dict,
    db_domain_props_lookup: typing.Dict,
    db_range_props_lookup: typing.Dict,
    db_props_lookup: typing.Dict,
    file_header_lookup: typing.Dict,
    domain_conf: typing.Dict,
    range_conf: typing.Dict,
    prop_conf: typing.Dict,
    batch: typing.List,
) -> None:
    project_id = await get_project_id(conn, params['project_name'])
    relation_type_id = await get_relation_type_id(conn, params['project_name'], params['relation_type_name'])
    domain_type_id = await get_entity_type_id(conn, params['project_name'], params['domain_type_name'])
    range_type_id = await get_entity_type_id(conn, params['project_name'], params['range_type_name'])

    if 'id' not in prop_conf:
        id = await fetchval(
            conn,
            '''
                SELECT current_id
                FROM app.relation_count
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id
            }
        )
    max_id = 0
    # key: placeholder strings separated by | (domain_placeholder|range_placeholder|placeholder)
    # value: typing.List with corresponding parameters
    props_collection = {}

    for row in batch:
        domain_properties = create_properties(row, db_domain_props_lookup, file_header_lookup, domain_conf)
        range_properties = create_properties(row, db_range_props_lookup, file_header_lookup, range_conf)
        properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)

        if 'id' in prop_conf:
            max_id = max(max_id, properties['id']['value'])
        else:
            id += 1
            max_id = id
            properties['id'] = {
                'type': 'int',
                'value': id,
            }

        domain_props = age_format_properties(domain_properties, 'domain')
        range_props = age_format_properties(range_properties, 'range')
        props = age_format_properties(properties)

        key = f'{domain_props[0]}|{range_props[0]}|{props[0]}'
        value = {**domain_props[1], **range_props[1], **props[1]}
        if key in props_collection:
            props_collection[key].append(value)
        else:
            props_collection[key] = [value]

    # GREATEST is needed when id in prop_conf
    await execute(
        conn,
        '''
            UPDATE app.relation_count
            SET current_id = GREATEST(current_id, :relation_id)
            WHERE id = :relation_type_id;
        ''',
        {
            'relation_id': max_id,
            'relation_type_id': relation_type_id
        }
    )

    for placeholder in props_collection:
        split = placeholder.split('|')
        await executemany(
            conn,
            (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$MATCH'
                f'        (d:n_{dtu(domain_type_id)} {{{split[0]}}}),'
                f'        (r:n_{dtu(range_type_id)} {{{split[1]}}})'
                f' '
                f'CREATE'
                f'(d)-[\\:e_{dtu(relation_type_id)} {{{split[2]}}}]->(r)$$, :params'
                f') as (a agtype);'
            ),
            [{'params': json.dumps(params)} for params in props_collection[placeholder]]
        )

    # TODO: relation entity, revision, property relations
