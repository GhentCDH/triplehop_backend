from typing import Dict, List

from databases import Database
from json import loads as json_loads


def read_config_from_file(project_name: str, type: str, name: str):
    with open(f'./config/{project_name}/{type}/{name}.json') as config_file:
        return config_file.read()


async def get_entity_type_id(db: Database, project_name: str, entity_type_name: str) -> str:
    return await db.fetch_val(
        '''
            SELECT entity.id
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
    config = json_loads(await db.fetch_val(
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


async def set_path(db: Database):
    await db.execute(
        '''
            SET search_path = ag_catalog, "$user", public;
        '''
    )


async def create_entity(
    db: Database,
    params: List,
    db_props_lookup: Dict,
    file_header_lookup: Dict,
    data: List,
    prop_conf: Dict
) -> None:
    properties = []
    if 'id' in prop_conf:
        await db.execute(
            '''
                UPDATE app.entity_count
                SET current_id = GREATEST(current_id, :entity_id)
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': params['entity_type_id']
            }
        )
        properties.append(f"id: {prop_conf['id']}")
    else:
        await db.execute(
            '''
                UPDATE app.entity_count
                SET current_id = current_id + 1
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': params['entity_type_id']
            }
        )
        id = await db.fetch_val(
            '''
                SELECT current_id
                FROM app.entity_count
                WHERE id = :entity_type_id;
            ''',
            {
                'entity_type_id': params['entity_type_id']
            }
        )
        properties.append(f"id: {id}")

    for (key, conf) in prop_conf.items():
        if key == 'id':
            continue
        value = data[file_header_lookup[conf[0]]]
        if len(conf) == 2 and conf[1] == 'int':
            if value not in ['', 'N/A']:
                properties.append(f"p_{params['entity_type_id']}_{db_props_lookup[key]}: {int(value)}")
        else:
            if value != '':
                properties.append(f"p_{params['entity_type_id']}_{db_props_lookup[key]}: {value}")
    # await db.execute(
    #     '''
    #         SELECT * FROM cypher(
                (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text
                , $$
    #             CREATE (v:Part {:properties})
    #         $$
    #         );
    #     ''',
    #     {
    #         :graph_name =
    #     }
    # )

