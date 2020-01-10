import psycopg2

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO app.user (email, name)
        VALUES ('pieterjan.depotter@ugent.be', 'Pieterjan De Potter')
        ON CONFLICT DO NOTHING;
        ''')