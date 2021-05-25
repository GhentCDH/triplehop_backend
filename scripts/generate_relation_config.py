import asyncio
import asyncpg
import json
import os

import config
import utils


async def generate_config():
    pool = await asyncpg.create_pool(**config.DATABASE)
    for project_folder in os.listdir('human_readable_config'):
        relations = {}
        records = await utils.fetch(
            pool,
            '''
                SELECT relation.id::text, relation.system_name
                FROM app.relation
                INNER JOIN app.project
                    ON relation.project_id = project.id
                WHERE project.system_name = :project_name;
            ''',
            {
                'project_name': project_folder,
            }
        )
        for record in records:
            relations[record['system_name']] = {
                'id': record['id'],
            }

        for relation in relations:
            id = relations[relation]['id']
            # todo: relations with multiple domains / ranges?
            relations[relation]['domain'] = await utils.fetchval(
                pool,
                '''
                    SELECT entity.system_name
                    FROM app.relation_domain
                    INNER JOIN app.entity
                        ON relation_domain.entity_id = entity.id
                    WHERE relation_domain.relation_id = :relation_id;
                ''',
                {
                    'relation_id': id,
                }
            )
            relations[relation]['range'] = await utils.fetchval(
                pool,
                '''
                    SELECT entity.system_name
                    FROM app.relation_range
                    INNER JOIN app.entity
                        ON relation_range.entity_id = entity.id
                    WHERE relation_range.relation_id = :relation_id;
                ''',
                {
                    'relation_id': id,
                }
            )

        with open(f'human_readable_config/{project_folder}/relations.json', 'w') as f:
            json.dump(relations, f, indent=4)

    await pool.close()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(generate_config())
    loop.close()


if __name__ == '__main__':
    main()
