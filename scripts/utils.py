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
    result = await db.fetch_val(
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
        return {json.load(result)[k]['system_name']: k for k in config.keys()}
    return {}


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
    formatted_properties = {}
    for (key, value) in properties.items():
        value_type = value['type']
        value_value = value['value']
        if key == 'id':
            if value_type == 'int':
                formatted_properties['id'] = int(value_value)
                continue
            else:
                raise Exception('Non-int ids are not yet implemented')
        else:
            key = dtu(key)
        if value_type == 'int':
            formatted_properties[f'p_{key}'] = int(value_value)
            continue
        if value_type == 'string':
            formatted_properties[f'p_{key}'] = value_value
            continue
        # https://github.com/apache/incubator-age/issues/48
        # if value_type == 'point':
        #     formatted_properties.append(f"p_{key}: ST_SetSRID(ST_MakePoint({', '.join(value_value)}),4326)")
        #     continue
        raise Exception(f'Value type {value_type} is not yet implemented')
    return [
        ', '.join([f'{k}: ${k}' for k in formatted_properties.keys()]),
        formatted_properties,
    ]


def create_properties(row: List, db_props_lookup: Dict, file_header_lookup: Dict, prop_conf: Dict):
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
        # https://github.com/apache/incubator-age/issues/48
        # if conf[0] == 'point':
        #     value = row[file_header_lookup[conf[1]]]
        #     if value[0] in ['']:
        #         continue
        #     properties[db_key] = {
        #         'type': 'point',
        #         'value': value.split(', '),
        #     }
        #     continue
        else:
            raise Exception(f'Type {conf[0]} has not yet been implemented')
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
                'entity_id': properties['id']['value'],
                'entity_type_id': entity_type_id,
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
                'entity_type_id': entity_type_id,
            }
        )
        id = await db.fetch_val(
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
    await db.execute(
        (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$CREATE (\:e_{dtu(entity_type_id)} {{{props[0]}}})$$, :params'
            f') as (a agtype);'
        ),
        {
            'params': json.dumps(props[1]),
        }
    )

    # TODO: revision, property relations


async def create_entities(
    db: Database,
    params: Dict,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    prop_conf: Dict,
    batch: List,
) -> None:
    project_id = await get_project_id(db, params['project_name'])
    entity_type_id = await get_entity_type_id(db, params['project_name'], params['entity_type_name'])

    if 'id' not in prop_conf:
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
    max_id = 0
    # key: placeholder string
    # value: list with corresponding parameters
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
    await db.execute(
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
        await db.execute_many(
            (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$CREATE (\:e_{dtu(entity_type_id)} {{{placeholder}}})$$, :params'
                f') as (a agtype);'
            ),
            [{'params': json.dumps(params)} for params in props_collection[placeholder]]
        )

    # TODO: revision, property relations


# def age_format_properties_set(vertex: str, properties: Dict):
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


# def age_update_entity(project_id: str, entity_type_id: str, id: int, properties: Dict):
#     return (
#         f'SELECT * FROM cypher('
#         f'\'{project_id}\', '
#         f'$$MATCH (n:e_{dtu(entity_type_id)} {{id: {id}}}) {age_format_properties_set("n", properties)}$$'
#         f') as (a agtype);'
#     )


# async def update_entity(
#     db: Database,
#     row: List,
#     params: Dict,
#     db_props_lookup: Dict,
#     file_header_lookup: Dict,
#     prop_conf: Dict
# ) -> None:
#     project_id = await get_project_id(db, params['project_name'])
#     entity_type_id = await get_entity_type_id(db, params['project_name'], params['entity_type_name'])
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
        await db.execute(
            '''
                UPDATE app.relation_count
                SET current_id = current_id + 1
                WHERE id = :relation_type_id;
            ''',
            {
                'relation_type_id': relation_type_id,
            }
        )
        id = await db.fetch_val(
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

    domain_props = age_format_properties(domain_properties)
    range_props = age_format_properties(range_properties)
    props = age_format_properties(properties)

    await db.execute(
        (
            f'SELECT * FROM cypher('
            f'\'{project_id}\', '
            f'$$MATCH'
            f'        (d:e_{dtu(domain_type_id)} {{{domain_props[0]}}}),'
            f'        (r:e_{dtu(range_type_id)} {{{range_props[0]}}})'
            f' '
            f'CREATE'
            f'(d)-[\:r_{dtu(relation_type_id)} {{{props[0]}}}]->(r)$$, :params'
            f') as (a agtype);'
        ),
        {
            'params': json.dumps({**domain_props[1], **range_props[1], **props[1]})
        }
    )


async def create_relations(
    db: Database,
    params: Dict,
    db_domain_props_lookup: Dict,
    db_range_props_lookup: Dict,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    domain_conf: Dict,
    range_conf: Dict,
    prop_conf: Dict,
    batch: List,
) -> None:
    project_id = await get_project_id(db, params['project_name'])
    relation_type_id = await get_relation_type_id(db, params['project_name'], params['relation_type_name'])
    domain_type_id = await get_entity_type_id(db, params['project_name'], params['domain_type_name'])
    range_type_id = await get_entity_type_id(db, params['project_name'], params['range_type_name'])

    if 'id' not in prop_conf:
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
    max_id = 0
    # key: placeholder strings separated by | (domain_placeholder|range_placeholder|placeholder)
    # value: list with corresponding parameters
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

        domain_props = age_format_properties(domain_properties)
        range_props = age_format_properties(range_properties)
        props = age_format_properties(properties)

        key = f'{domain_props[0]}|{range_props[0]}|{props[0]}'
        value = {**domain_props[1], **range_props[1], **props[1]}
        if key in props_collection:
            props_collection[key].append(value)
        else:
            props_collection[key] = [value]

    # GREATEST is needed when id in prop_conf
    await db.execute(
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
        await db.execute_many(
            (
                f'SELECT * FROM cypher('
                f'\'{project_id}\', '
                f'$$MATCH'
                f'        (d:e_{dtu(domain_type_id)} {{{split[0]}}}),'
                f'        (r:e_{dtu(range_type_id)} {{{split[1]}}})'
                f' '
                f'CREATE'
                f'(d)-[\:r_{dtu(relation_type_id)} {{{split[2]}}}]->(r)$$, :params'
                f') as (a agtype);'
            ),
            [{'params': json.dumps(params)} for params in props_collection[placeholder]]
        )

    # TODO: relation entity, revision, property relations
