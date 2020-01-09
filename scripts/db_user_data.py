import psycopg2

conn = psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant')
cur = conn.cursor()

cur.execute('''
INSERT INTO app.user (email, name)
VALUES ('pieterjan.depotter@ugent.be', 'Pieterjan De Potter')
ON CONFLICT DO NOTHING;
''')

conn.commit()

cur.close()
conn.close()