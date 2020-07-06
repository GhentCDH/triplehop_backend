import psycopg2

from config import DATABASE_CONNECTION_STRING, USER_PASS_1, USER_PASS_2

with psycopg2.connect(DATABASE_CONNECTION_STRING) as conn:
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO app.user (username, display_name, hashed_password, disabled)
        VALUES (
            'pieterjan.depotter@ugent.be',
            'Pieterjan De Potter',
            %(user_pass_1)s,
            'false'
        )
        ON CONFLICT DO NOTHING;
        INSERT INTO app.user (username, display_name, hashed_password, disabled)
        VALUES (
            'info@cinemabelgica.be',
            'Cinema Belgica',
            %(user_pass_2)s,
            'false'
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.group (system_name, display_name, description)
        VALUES (
            'global_admin',
            'Global administrator',
            'Users in this group have all permissions'
        );

        INSERT INTO app.permission (system_name, display_name, description)
        VALUES (
            'es_index',
            'Index data in Elasticsearch',
            'Index data in Elasticsearch'
        );

        INSERT INTO app.users_groups (user_id, group_id)
        VALUES (
            (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be'),
            (SELECT "group".id FROM app.group WHERE "group".system_name = 'global_admin')
        );

        INSERT INTO app.groups_permissions (group_id, permission_id, project_id, entity_id, relation_id)
        VALUES (
            (SELECT "group".id FROM app.group WHERE "group".system_name = 'global_admin'),
            (SELECT permission.id FROM app.permission WHERE permission.system_name = 'es_index'),
            (SELECT project.id FROM app.project WHERE project.system_name = '__all__'),
            (SELECT entity.id FROM app.entity WHERE entity.system_name = '__all__'),
            (SELECT relation.id FROM app.relation WHERE relation.system_name = '__all__')
        );
        ''',
        {
            'user_pass_1': USER_PASS_1,
            'user_pass_2': USER_PASS_2,
        }
        )
