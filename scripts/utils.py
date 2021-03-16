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


async def get_props_lookup(db: Database, project_name: str, entity_type_name: str) -> Dict:
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


def age_format_properties(properties: Dict):
    formatted_properties = []
    for (key, value) in properties.items():
        if key == 'id':
            formatted_properties.append(f'id: {value}')
        else:
            formatted_properties.append(f'p_{key}: {value}')
    return ', '.join(formatted_properties)


def age_create(graphname: str, label: str, properties: Dict):
    return (
        f'SELECT * FROM cypher('
        f'\'{graphname}\', '
        f'$$CREATE (:l_{label} {{{age_format_properties(properties)}}})$$'
        f') as (a agtype);'
    )


def create_properties(row: List, db_props_lookup: Dict, file_header_lookup: Dict, prop_conf: Dict):
    properties = {}
    for (key, conf) in prop_conf.items():
        if key == 'id':
            continue
        value = row[file_header_lookup[conf[0]]]
        if len(conf) == 2 and conf[1] == 'int':
            if value not in ['', 'N/A']:
                properties[db_props_lookup[key]] = value
        else:
            if value != '':
                properties[db_props_lookup[key]] = "'" + value.replace("'", "\\'") + "'"
    return properties


async def create_entity(
    db: Database,
    row: List,
    params: List,
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
        properties['id'] = int(row[file_header_lookup[prop_conf['id'][0]]])
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
        properties['id'] = id

    # https://github.com/apache/incubator-age/issues/43
    # try SELECT * FROM cypher('testgraph', $$CREATE (:label $properties)$$, $1) as (a agtype);
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    # databases.Database().execute leads to a prepared statement
    # (see https://github.com/encode/databases/blob/master/databases/backends/postgres.py#L189)
    # => use execute directly on asyncpg connection
    async with db.connection() as conn:
        await conn.raw_connection.execute(
            age_create(project_id, dtu(entity_type_id), properties)
        )

    # TODO: revision, property relations


def age_format_properties_set(vertex: str, properties: Dict):
    return ' '.join({f'SET {vertex}.p_{k} = {v}' for (k, v) in properties.items()})


def age_update(graphname: str, label: str, id: int, properties: Dict):
    vertex = 've'
    return (
        f'SELECT * FROM cypher('
        f'\'{graphname}\', '
        f'$$MATCH ({vertex}:l_{label} {{id: {id}}}) {age_format_properties_set(vertex, properties)}$$'
        f') as (a agtype);'
    )


async def update_entity(
    db: Database,
    row: List,
    params: List,
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
                age_update(project_id, dtu(entity_type_id), entity_id, properties)
            )

    # TODO: revision, property relations
