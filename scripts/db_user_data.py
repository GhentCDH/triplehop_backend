import psycopg2

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO app.user (username, display_name, hashed_password, disabled)
        VALUES (
            'pieterjan.depotter@ugent.be',
            'Pieterjan De Potter',
            '$2b$12$crqpzmrPpGLg1bkOlie.leZAopo1GUHALXukbuORu6d1EnrQoAWyC',
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

        INSERT INTO app.groups_permissions (group_id, permission_id)
        VALUES (
            (SELECT "group".id FROM app.group WHERE "group".system_name = 'global_admin'),
            (SELECT permission.id FROM app.permission WHERE permission.system_name = 'es_index')
        );
        ''')
