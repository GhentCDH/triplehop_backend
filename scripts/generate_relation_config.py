import asyncio
import databases
import json
import os

import config


async def generate_config():
    async with databases.Database(config.DATABASE_CONNECTION_STRING) as db:
        for project_folder in os.listdir('human_readable_config'):
            relations = {}
            records = await db.fetch_all(
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
                relations[relation]['domain'] = await db.fetch_val(
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
                relations[relation]['range'] = await db.fetch_val(
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


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(generate_config())
    loop.close()


if __name__ == '__main__':
    main()
