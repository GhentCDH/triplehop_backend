import asyncio
import databases

import config


async def create_user_data():
    async with databases.Database(config.DATABASE_CONNECTION_STRING) as db:
        await db.execute_many(
            '''
                INSERT INTO app.user (username, display_name, hashed_password, disabled)
                VALUES (:username, :display_name, :hashed_password, :disabled)
                ON CONFLICT DO NOTHING;
            ''',
            [
                {
                    'username': 'pieterjan.depotter@ugent.be',
                    'display_name': 'Pieterjan De Potter',
                    'hashed_password': config.USER_PASS_1,
                    'disabled': False,
                },
                {
                    'username': 'info@cinemabelgica.be',
                    'display_name': 'Cinema Belgica',
                    'hashed_password': config.USER_PASS_2,
                    'disabled': False,
                },
            ]
        )

        await db.execute(
            '''
                INSERT INTO app.group (system_name, display_name, description)
                VALUES (:system_name, :display_name, :description);
            ''',
            {
                'system_name': 'global_admin',
                'display_name': 'Global administrator',
                'description': 'Users in this group have all permissions',
            }
        )

        await db.execute(
            '''
                INSERT INTO app.permission (system_name, display_name, description)
                VALUES (:system_name, :display_name, :description);
            ''',
            {
                'system_name': 'es_index',
                'display_name': 'Index data in Elasticsearch',
                'description': 'Users in with this permission can run batch jobs to index in elasticsearch',
            }
        )

        await db.execute(
            '''
                INSERT INTO app.users_groups (user_id, group_id)
                VALUES (
                    (SELECT "user".id FROM app.user WHERE "user".username = :username),
                    (SELECT "group".id FROM app.group WHERE "group".system_name = :group_name)
                );
            ''',
            {
                'username': 'pieterjan.depotter@ugent.be',
                'group_name': 'global_admin',
            }
        )

        await db.execute(
            '''
                INSERT INTO app.groups_permissions (group_id, permission_id, project_id, entity_id, relation_id)
                VALUES (
                    (SELECT "group".id FROM app.group WHERE "group".system_name = :group_name),
                    (SELECT permission.id FROM app.permission WHERE permission.system_name = :permission_name),
                    (SELECT project.id FROM app.project WHERE project.system_name = :project_name),
                    (SELECT entity.id FROM app.entity WHERE entity.system_name = :entity_name),
                    (SELECT relation.id FROM app.relation WHERE relation.system_name = :relation_name)
                );
            ''',
            {
                'group_name': 'global_admin',
                'permission_name': 'es_index',
                'project_name': '__all__',
                'entity_name': '__all__',
                'relation_name': '__all__',
            }
        )


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_user_data())
    loop.close()


if __name__ == '__main__':
    main()
