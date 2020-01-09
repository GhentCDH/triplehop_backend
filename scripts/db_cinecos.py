import psycopg2

conn = psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant')
cur = conn.cursor()

cur.execute('''
INSERT INTO app.project (system_name, display_name, user_id)
VALUES ('cinecos', 'Cinecos', 1)
ON CONFLICT DO NOTHING;

INSERT INTO app.entity_definition (project_id, system_name, display_name, user_id)
VALUES (1, 'film', 'Film', 1)
ON CONFLICT DO NOTHING;

CREATE GRAPH g1;
CREATE VLABEL v1;
''')

conn.commit()

cur.close()
conn.close()