from typing import Dict, List
from databases import Database

import aiocache
import json
import re

RE_PROPERTY_VALUE = re.compile(r'(?<![$])[$]([0-9a-z_]+)')


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def read_config_from_file(project_name: str, type: str, name: str):
    with open(f'./config/{project_name}/{type}/{name}.json') as config_file:
        return config_file.read()


@aiocache.cached()
async def get_project_id(db: Database, project_name: str) -> str:
    return await db.fetch_val(
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
async def get_entity_type_id(db: Database, project_name: str, entity_type_name: str) -> str:
    return await db.fetch_val(
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
async def get_relation_type_id(db: Database, project_name: str, relation_type_name: str) -> str:
    return await db.fetch_val(
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
async def get_user_id(db: Database, username: str) -> str:
    return await db.fetch_val(
        '''
            SELECT "user".id
            FROM app.user
            WHERE "user".username = :username;
        ''',
        {
            'username': username,
        }
    )


async def create_project_config(db: Database, system_name: str, display_name: str, username: str):
    await db.execute(
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
    db: Database,
    project_name: str,
    username: str,
    system_name: str,
    display_name: str,
    config: Dict
):
    await db.execute(
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
    await db.execute(
        '''
            INSERT INTO app.entity_count (id)
            VALUES (
                (SELECT entity.id FROM app.entity WHERE system_name = :system_name)
            )
            ON CONFLICT DO NOTHING;
        ''',
        {
            'system_name': system_name,
        }
    )


async def create_relation_config(
    db: Database,
    project_name: str,
    username: str,
    system_name: str,
    display_name: str,
    config: Dict,
    domains: List,
    ranges: List
):
    await db.execute(
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
        await db.execute(
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
        await db.execute(
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
    await db.execute(
        '''
            INSERT INTO app.relation_count (id)
            VALUES (
                (SELECT relation.id FROM app.relation WHERE system_name = :system_name)
            )
            ON CONFLICT DO NOTHING;
        ''',
        {
            'system_name': system_name,
        }
    )


async def get_entity_props_lookup(db: Database, project_name: str, entity_type_name: str) -> Dict:
    config = json.loads(await db.fetch_val(
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
    ))
    return {config[k]['system_name']: k for k in config.keys()}


async def get_relation_props_lookup(db: Database, project_name: str, relation_type_name: str) -> Dict:
    config = json.loads(await db.fetch_val(
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
    ))
    return {config[k]['system_name']: k for k in config.keys()}


async def init_age(db: Database):
    await db.execute(
        '''
            SET search_path = ag_catalog, "$user", public;
        '''
    )
    await db.execute(
        '''
            LOAD '$libdir/plugins/age';
        '''
    )


async def drop_project_graph(db: Database, project_name: str):
    await db.execute(
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


async def create_project_graph(db: Database, project_name: str):
    await db.execute(
        '''
            SELECT create_graph(
                (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text
            );
        ''',
        {
            'project_name': project_name,
        }
    )


def age_format_properties(properties: Dict):
    formatted_properties = []
    for (key, value) in properties.items():
        value_type = value['type']
        value_value = value['value']
        if key == 'id':
            formatted_properties.append(f'id: {value_value}')
        elif value_type == 'int':
            formatted_properties.append(f'p_{key}: {value_value}')
        elif value_type == 'string':
            value_value = value_value.replace("'", "\\'")
            formatted_properties.append(f"p_{key}: '{value_value}'")
        # https://github.com/apache/incubator-age/issues/48
        # elif value_type == 'point':
        #     formatted_properties.append(f"p_{key}: ST_SetSRID(ST_MakePoint({', '.join(value_value)}),4326)")
        else:
            raise Exception('Not implemented')
    return ', '.join(formatted_properties)


def age_create_entity(project_id: str, entity_type_id: str, properties: Dict):
    return (
        f'SELECT * FROM cypher('
        f'\'{project_id}\', '
        f'$$CREATE (:e_{dtu(entity_type_id)} {{{age_format_properties(properties)}}})$$'
        f') as (a agtype);'
    )


def create_properties(row: List, db_props_lookup: Dict, file_header_lookup: Dict, prop_conf: Dict):
    properties = {}
    for (key, conf) in prop_conf.items():
        if key == 'id':
            db_key = 'id'
        else:
            db_key = db_props_lookup[key]
        if conf[0] == 'int':
            value = row[file_header_lookup[conf[1]]]
            if value in ['', 'N/A']:
                continue
            properties[db_key] = {
                'type': 'int',
                'value': int(value),
            }
        elif conf[0] == 'string':
            value = row[file_header_lookup[conf[1]]]
            if value in ['', 'N/A']:
                continue
            properties[db_key] = {
                'type': 'string',
                'value': value,
            }
        # https://github.com/apache/incubator-age/issues/48
        # elif conf[0] == 'point':
        #     value = row[file_header_lookup[conf[1]]]
        #     if value[0] in ['', 'N/A']:
        #         continue
        #     properties[db_key] = {
        #         'type': 'point',
        #         'value': value.split(', '),
        #     }
        else:
            raise Exception('Not implemented')
    return properties


async def create_entity(
    db: Database,
    row: List,
    params: Dict,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    prop_conf: Dict
) -> None:
    project_id = await get_project_id(db, params['project_name'])
    entity_type_id = await get_entity_type_id(db, params['project_name'], params['entity_type_name'])
    properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)
    if 'id' in prop_conf:
        await db.execute(
            '''
                UPDATE app.entity_count
                SET current_id = GREATEST(current_id, :entity_id)
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id
            }
        )
    else:
        await db.execute(
            '''
                UPDATE app.entity_count
                SET current_id = current_id + 1
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id
            }
        )
        id = await db.fetch_val(
            '''
                SELECT current_id
                FROM app.entity_count
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': entity_type_id
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': id,
        }

    # https://github.com/apache/incubator-age/issues/43
    # try SELECT * FROM cypher('testgraph', $$CREATE (:label $properties)$$, $1) as (a agtype);
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    # databases.Database().execute leads to a prepared statement
    # (see https://github.com/encode/databases/blob/master/databases/backends/postgres.py#L189)
    # => use execute directly on asyncpg connection
    async with db.connection() as conn:
        await conn.raw_connection.execute(
            age_create_entity(project_id, entity_type_id, properties)
        )

    # TODO: revision, property relations


def age_format_properties_set(vertex: str, properties: Dict):
    formatted_properties = []
    for (key, value) in properties.items():
        value_type = value['type']
        value_value = value['value']
        if value_type == 'int':
            formatted_properties.append(f'SET {vertex}.p_{key} = {value_value}')
        elif value_type == 'string':
            value_value = value_value.replace("'", "\\'")
            formatted_properties.append(f"SET {vertex}.p_{key} = '{value_value}'")
        elif value_type == 'array':
            # https://github.com/apache/incubator-age/issues/44
            raise Exception('Not implemented')
        else:
            raise Exception('Not implemented')
    return ' '.join(formatted_properties)

    return ' '.join({f'SET {vertex}.p_{k} = {v}' for (k, v) in properties.items()})


def age_update_entity(project_id: str, entity_type_id: str, id: int, properties: Dict):
    return (
        f'SELECT * FROM cypher('
        f'\'{project_id}\', '
        f'$$MATCH (n:e_{dtu(entity_type_id)} {{id: {id}}}) {age_format_properties_set("n", properties)}$$'
        f') as (a agtype);'
    )


async def update_entity(
    db: Database,
    row: List,
    params: Dict,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    prop_conf: Dict
) -> None:
    project_id = await get_project_id(db, params['project_name'])
    entity_type_id = await get_entity_type_id(db, params['project_name'], params['entity_type_name'])
    properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)

    if properties:
        entity_id = int(row[file_header_lookup[prop_conf['id'][0]]])
        # https://github.com/apache/incubator-age/issues/43
        # try SELECT * FROM cypher('testgraph', $$CREATE (:label $properties)$$, $1) as (a agtype);
        # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
        # databases.Database().execute leads to a prepared statement
        # (see https://github.com/encode/databases/blob/master/databases/backends/postgres.py#L189)
        # => use execute directly on asyncpg connection
        async with db.connection() as conn:
            await conn.raw_connection.execute(
                age_update_entity(project_id, dtu(entity_type_id), entity_id, properties)
            )

    # TODO: revision, property relations


def age_create_relation(
    project_id: str,
    relation_type_id: str,
    domain_type_id: str,
    domain_properties: Dict,
    range_type_id: str,
    range_properties: Dict,
    properties: Dict
):
    domain_properties
    return (
        f'SELECT * FROM cypher('
        f'\'{project_id}\', '
        f'$$MATCH'
        f'        (d:e_{dtu(domain_type_id)} {{{age_format_properties(domain_properties)}}}),'
        f'        (r:e_{dtu(range_type_id)} {{{age_format_properties(range_properties)}}})'
        f' '
        f'CREATE'
        f'(d)-[:r_{dtu(relation_type_id)} {{{age_format_properties(properties)}}}]->(r)$$'
        f') as (a agtype);'
    )

    # TODO: match on other props


async def create_relation(
    db: Database,
    row: List,
    params: Dict,
    db_domain_props_lookup: Dict,
    db_range_props_lookup: Dict,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    domain_conf: Dict,
    range_conf: Dict,
    prop_conf: Dict
) -> None:
    project_id = await get_project_id(db, params['project_name'])
    relation_type_id = await get_relation_type_id(db, params['project_name'], params['relation_type_name'])
    domain_type_id = await get_entity_type_id(db, params['project_name'], params['domain_type_name'])
    range_type_id = await get_entity_type_id(db, params['project_name'], params['range_type_name'])

    domain_properties = create_properties(row, db_domain_props_lookup, file_header_lookup, domain_conf)
    range_properties = create_properties(row, db_range_props_lookup, file_header_lookup, range_conf)
    properties = create_properties(row, db_props_lookup, file_header_lookup, prop_conf)

    if 'id' in prop_conf:
        await db.execute(
            '''
                UPDATE app.relation_count
                SET current_id = GREATEST(current_id, :entity_id)
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': int(row[file_header_lookup[prop_conf['id'][1]]]),
        }
    else:
        await db.execute(
            '''
                UPDATE app.relation_count
                SET current_id = current_id + 1
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id
            }
        )
        id = await db.fetch_val(
            '''
                SELECT current_id
                FROM app.relation_count
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id
            }
        )
        properties['id'] = {
            'type': 'int',
            'value': id,
        }

    # https://github.com/apache/incubator-age/issues/43
    # try SELECT * FROM cypher('testgraph', $$CREATE (:label $properties)$$, $1) as (a agtype);
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    # databases.Database().execute leads to a prepared statement
    # (see https://github.com/encode/databases/blob/master/databases/backends/postgres.py#L189)
    # => use execute directly on asyncpg connection
    async with db.connection() as conn:
        await conn.raw_connection.execute(
            age_create_relation(
                project_id,
                relation_type_id,
                domain_type_id,
                domain_properties,
                range_type_id,
                range_properties,
                properties
            )
        )

    # TODO: relation entity, revision, property relations
