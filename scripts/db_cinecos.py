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
                    "system_name": "original_id",
                    "display_name": "Original id",
                    "type": "string"
                },
                "1": {
                    "system_name": "title",
                    "display_name": "Title",
                    "type": "string"
                },
                "2": {
                    "system_name": "year",
                    "display_name": "Year",
                    "type": "int"
                }
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'person',
            'Person',
            '{
                "0": {
                    "system_name": "original_id",
                    "display_name": "Original id",
                    "type": "string"
                },
                "1": {
                    "system_name": "name",
                    "display_name": "Name",
                    "type": "string"
                }
            }',
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.entity_count (id)
        VALUES (1), (2)
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
        (film_type_id, film_type_conf) = list(cur.fetchone())
        film_type_conf_lookup = {film_type_conf[k]['system_name']: int(k) for k in film_type_conf.keys()}

        cur.execute('''
            SELECT
                entity.id,
                entity.config
            FROM app.entity
            WHERE entity.system_name = %(entity)s;
            ''', {
                'entity': 'person',
        })
        (person_type_id, person_type_conf) = list(cur.fetchone())
        person_type_conf_lookup = {person_type_conf[k]['system_name']: int(k) for k in person_type_conf.keys()}

        cur.execute('''
        DROP GRAPH IF EXISTS g%(project_id)s CASCADE;
        CREATE GRAPH g%(project_id)s;

        CREATE VLABEL v%(film_type_id)s;
        CREATE VLABEL v%(person_type_id)s;
        CREATE PROPERTY INDEX ON v%(film_type_id)s ( id );
        CREATE PROPERTY INDEX ON v%(person_type_id)s ( id );
        ''', {
            'film_type_id': film_type_id,
            'person_type_id': person_type_id,
            'project_id': project_id,
        })

        with open('data/cinecos_films.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            film_prop_conf = {
                'original_id': [film_type_conf_lookup['original_id'], header_lookup['film_id']],
                'title': [film_type_conf_lookup['title'], header_lookup['title']],
                'year': [film_type_conf_lookup['year'], header_lookup['film_year']],
            }

            film_params = {
                'entity_type_id': film_type_id,
                'user_id': user_id,
            }
            utils.batch_process(cur, csv_reader, film_params, utils.add_entity, film_prop_conf)

            director_data = []
            for row in csv_reader:
                for index, director_name in enumerate(row[header_lookup['film_director']].split('|')):
                    director_data.append(
                        [
                            row[header_lookup['film_id']] + '_' + str(index),
                            director_name
                        ]
                    )

            person_prop_conf = {
                'original_id': [film_type_conf_lookup['original_id'], 0],
                'name': [person_type_conf_lookup['name'], 1],
            }

            person_params = {
                'entity_type_id': person_type_id,
                'user_id': user_id,
            }
            utils.batch_process(cur, director_data, person_params, utils.add_entity, person_prop_conf)
