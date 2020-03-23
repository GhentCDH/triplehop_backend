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
        ''')
