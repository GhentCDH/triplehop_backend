import csv
import psycopg2

import utils

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute('''
        SELECT "user".id
        FROM app.user
        WHERE "user".email = %(email)s;
        ''', {
            'email': 'pieterjan.depotter@ugent.be',
        })
        userId = cur.fetchone()[0]

        cur.execute('''
        INSERT INTO app.project (systemName, displayName, userId)
        VALUES ('cinecos', 'Cinecos', %(userId)s)
        ON CONFLICT DO NOTHING;
        ''', {
            'userId': userId,
        })

        cur.execute('''
            SELECT project.id
            FROM app.project
            WHERE project.systemName = %(project)s;
            ''', {
                'project': 'cinecos',
        })
        projectId = cur.fetchone()[0]

        cur.execute('''
        INSERT INTO app.entity (projectId, systemName, displayName, config, userId)
        VALUES (
            %(projectId)s,
            'film',
            'Film',
            '{
                "0": {
                    "systemName": "title",
                    "displayName": "Title"
                },
                "1": {
                    "systemName": "year",
                    "displayName": "Year"
                }
            }',
            %(userId)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.entityCount (id)
        VALUES (1)
        ON CONFLICT DO NOTHING;
        ''', {
            'projectId': projectId,
            'userId': userId,
        })

        cur.execute('''
            SELECT
                entity.id,
                entity.config
            FROM app.entity
            WHERE entity.systemName = %(entity)s;
            ''', {
                'entity': 'film',
        })
        (entityId, entityConf) = list(cur.fetchone())
        confLookup = {entityConf[k]['systemName']:int(k) for k in entityConf.keys()}

        cur.execute('''
        DROP GRAPH IF EXISTS g%(projectId)s CASCADE;
        CREATE GRAPH g%(projectId)s;
        CREATE VLABEL v%(entityId)s;
        ''', {
            'entityId': entityId,
            'projectId': projectId,
        })

        with open('data/cinecos_films.csv') as inputFile:
            csvReader = csv.reader(inputFile)
            header = next(csvReader)
            headerLookup = {h:header.index(h) for h in header}
            propConf = {
                'title': [confLookup['title'], headerLookup['title']],
                'year': [confLookup['year'], headerLookup['film_year']],
            }

            for row in csvReader:
                utils.addEntity(cur, propConf, userId, entityId, row)