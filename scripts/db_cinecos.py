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
                "data": {
                    "0": {
                        "system_name": "original_id",
                        "display_name": "Original id",
                        "type": "Int"
                    },
                    "1": {
                        "system_name": "title",
                        "display_name": "Title",
                        "type": "String"
                    },
                    "2": {
                        "system_name": "year",
                        "display_name": "Year",
                        "type": "Int"
                    }
                },
                "display": {
                    "title": "$0 $1",
                    "layout": {
                        "label": "General",
                        "fields": [
                            {
                                "field": "1"
                            },
                            {
                                "label": "Production year",
                                "field": "2"
                            }
                        ]
                    }
                }
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'person',
            'Person',
            '{
                "data": {
                    "0": {
                        "system_name": "original_id",
                        "display_name": "Original id",
                        "type": "Int"
                    },
                    "1": {
                        "system_name": "name",
                        "display_name": "Name",
                        "type": "String"
                    }
                },
                "display": {
                    "title": "$1",
                    "layout": {
                        "fields": [
                            {
                                "field": "1"
                            }
                        ]
                    }
                }
            }',
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.entity_count (id)
        VALUES (1), (2)
        ON CONFLICT DO NOTHING;

        INSERT INTO app.relation (project_id, system_name, display_name, config, user_id)
        VALUES (
            %(project_id)s,
            'director',
            'Director',
            '{
                "data": {}
            }',
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.relation_domain (relation_id, entity_id, user_id)
        VALUES (
            (SELECT id from app.relation where system_name = 'director'),
            (SELECT id from app.entity where system_name = 'film'),
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.relation_range (relation_id, entity_id, user_id)
        VALUES (
            (SELECT id from app.relation where system_name = 'director'),
            (SELECT id from app.entity where system_name = 'person'),
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.relation_count (id)
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
            WHERE entity.system_name = %(entity_type_name)s;
            ''', {
                'entity_type_name': 'film',
        })
        (film_type_id, film_type_conf) = list(cur.fetchone())
        film_type_conf_lookup = {film_type_conf['data'][k]['system_name']: int(k) for k in film_type_conf['data'].keys()}

        cur.execute('''
            SELECT
                entity.id,
                entity.config
            FROM app.entity
            WHERE entity.system_name = %(entity_type_name)s;
            ''', {
                'entity_type_name': 'person',
        })
        (person_type_id, person_type_conf) = list(cur.fetchone())
        person_type_conf_lookup = {person_type_conf['data'][k]['system_name']: int(k) for k in person_type_conf['data'].keys()}

        cur.execute('''
            SELECT
                relation.id,
                relation.config
            FROM app.relation
            WHERE relation.system_name = %(relation_type_name)s;
            ''', {
                'relation_type_name': 'director',
        })
        (director_type_id, director_type_conf) = list(cur.fetchone())
        director_type_conf_lookup = {director_type_conf['data'][k]['system_name']: int(k) for k in director_type_conf['data'].keys()}

        cur.execute('''
        DROP GRAPH IF EXISTS g%(project_id)s CASCADE;
        CREATE GRAPH g%(project_id)s;

        CREATE VLABEL v%(film_type_id)s;
        CREATE VLABEL v%(person_type_id)s;
        CREATE PROPERTY INDEX ON v%(film_type_id)s ( id );
        CREATE PROPERTY INDEX ON v%(film_type_id)s ( p1_0 );
        CREATE PROPERTY INDEX ON v%(person_type_id)s ( id );
        CREATE PROPERTY INDEX ON v%(person_type_id)s ( p2_0 );
        ''', {
            'film_type_id': film_type_id,
            'person_type_id': person_type_id,
            'project_id': project_id,
        })
        # cur.execute('''
        # SET graph_path = g1;
        # ''')

        with open('data/cinecos_films.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'original_id': [film_type_conf_lookup['original_id'], header_lookup['film_id'], 'int'],
                'title': [film_type_conf_lookup['title'], header_lookup['title']],
                'year': [film_type_conf_lookup['year'], header_lookup['film_year'], 'int'],
            }

            params = {
                'entity_type_id': film_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing films')
            utils.batch_process(cur, [r for r in csv_reader], params, utils.add_entity, prop_conf)

        with open('data/cinecos_directors.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'original_id': [person_type_conf_lookup['original_id'], header_lookup['director_id'], 'int'],
                'name': [person_type_conf_lookup['name'], header_lookup['name']],
            }

            params = {
                'entity_type_id': person_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing persons')
            utils.batch_process(cur, [r for r in csv_reader], params, utils.add_entity, prop_conf)

        with open('data/cinecos_films_directors.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            relation_config = [header_lookup['film_id'], header_lookup['director_id']]

            prop_conf = {}

            params = {
                'domain_type_id': film_type_id,
                'domain_prop': f"p{film_type_id}_{film_type_conf_lookup['original_id']}",
                'range_type_id': person_type_id,
                'range_prop': f"p{person_type_id}_{person_type_conf_lookup['original_id']}",
                'relation_type_id': director_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing director relations')
            utils.batch_process(cur, [r for r in csv_reader], params, utils.add_relation, relation_config, prop_conf)
