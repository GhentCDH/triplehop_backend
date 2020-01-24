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
        user_id = cur.fetchone()[0]

        cur.execute('''
        INSERT INTO app.project (system_name, display_name, user_id)
        VALUES ('cinecos', 'Cinecos', %(user_id)s)
        ON CONFLICT DO NOTHING;
        ''', {
            'user_id': user_id,
        })

        cur.execute('''
            SELECT project.id
            FROM app.project
            WHERE project.system_name = %(project)s;
            ''', {
                'project': 'cinecos',
        })
        project_id = cur.fetchone()[0]

        cur.execute('''
        INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
        VALUES (
            %(project_id)s,
            'film',
            'Film',
            '{
                "0": {
                    "system_name": "title",
                    "display_name": "Title"
                },
                "1": {
                    "system_name": "year",
                    "display_name": "Year"
                }
            }',
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.entity_count (id)
        VALUES (1)
        ON CONFLICT DO NOTHING;
        ''', {
            'project_id': project_id,
            'user_id': user_id,
        })

        cur.execute('''
            SELECT
                entity.id,
                entity.config
            FROM app.entity
            WHERE entity.system_name = %(entity)s;
            ''', {
                'entity': 'film',
        })
        (entity_id, entity_conf) = list(cur.fetchone())
        conf_lookup = {entity_conf[k]['system_name']:int(k) for k in entity_conf.keys()}

        cur.execute('''
        DROP GRAPH IF EXISTS g%(project_id)s CASCADE;
        CREATE GRAPH g%(project_id)s;

        CREATE VLABEL v%(entity_id)s;
        CREATE PROPERTY INDEX ON v%(entity_id)s ( id );
        ''', {
            'entity_id': entity_id,
            'project_id': project_id,
        })

        with open('data/cinecos_films.csv') as input_file:
            csv_reader = csv.reader(input_file)
            header = next(csv_reader)
            header_lookup = {h:header.index(h) for h in header}
            prop_conf = {
                'title': [conf_lookup['title'], header_lookup['title']],
                'year': [conf_lookup['year'], header_lookup['film_year']],
            }

            counter = 0
            batch_query = []
            params = {
                'entity_id': entity_id,
                'user_id': user_id,
            }

            for row in csv_reader:
                counter += 1
                utils.add_entity(batch_query, params, prop_conf, row, counter)

                # execute queries in batches
                if not counter % 500:
                    cur.execute(
                        '\n'.join(batch_query),
                        params
                    )
                    batch_query = []
                    params = {
                        'entity_id': entity_id,
                        'user_id': user_id,
                    }

            # execute remaining queries
            if len(batch_query):
                cur.execute(
                    '\n'.join(batch_query),
                    params
                )
