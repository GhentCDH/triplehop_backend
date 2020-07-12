import csv
import psycopg2

from config import DATABASE_CONNECTION_STRING

from utils import add_entity, add_relation, batch_process, dtu


with psycopg2.connect(DATABASE_CONNECTION_STRING) as conn:
    with conn.cursor() as cur:
        cur.execute(
            '''
            SELECT "user".id
            FROM app.user
            WHERE "user".username = %(username)s;
            ''',
            {
                'username': 'info@cinemabelgica.be',
            }
        )
        user_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.project (system_name, display_name, user_id)
                VALUES (
                    'cinecos',
                    'Cinecos',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;
            '''
        )

        cur.execute(
            '''
                SELECT project.id
                FROM app.project
                WHERE project.system_name = %(project)s;
            ''',
            {
                'project': 'cinecos',
            }
        )
        project_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
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
                                "display_name": "Production year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "imdb_id",
                                "display_name": "IMDb ID",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "wikidata_id",
                                "display_name": "Wikidata ID",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1 ($2)",
                            "layout": [
                                {
                                    "label": "General",
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        },
                                        {
                                            "field": "3"
                                        },
                                        {
                                            "field": "4"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "title",
                                "display_name": "Title",
                                "selector_value": "$title",
                                "type": "text"
                            },
                            "1": {
                                "system_name": "year",
                                "display_name": "Production year",
                                "selector_value": "$year",
                                "type": "integer"
                            },
                            "2": {
                                "system_name": "director",
                                "display_name": "Director(s)",
                                "relation": "r_director",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_director->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_director->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            }
                        },
                        "es_filters": [
                            {
                                "filters": [
                                    {
                                        "filter": "0",
                                        "type": "autocomplete"
                                    },
                                    {
                                        "filter": "1",
                                        "type": "histogram_slider",
                                        "interval": 10
                                    },
                                    {
                                        "filter": "2"
                                    }
                                ]
                            }
                        ],
                        "es_columns": [
                            {
                                "column": "0",
                                "sortable": true
                            },
                            {
                                "column": "1",
                                "sortable": true
                            },
                            {
                                "column": "2",
                                "sortable": true
                            }
                        ]
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
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
                            },
                            "2": {
                                "system_name": "wikidata_id",
                                "display_name": "Wikidata ID",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.entity_count (id)
                VALUES
                    ((select entity.id FROM app.entity WHERE entity.system_name = 'film')),
                    ((select entity.id FROM app.entity WHERE entity.system_name = 'person'))
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation (project_id, system_name, display_name, config, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'director',
                    'Director',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Directed by",
                            "range_title": "Directed",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_domain (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT id FROM app.relation WHERE system_name = 'director'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_range (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT id FROM app.relation WHERE system_name = 'director'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_count (id)
                VALUES
                    ((SELECT relation.id FROM app.relation WHERE system_name = 'director'))
                ON CONFLICT DO NOTHING;
            '''
        )

        cur.execute(
            '''
                SELECT
                    entity.id,
                    entity.config
                FROM app.entity
                WHERE entity.system_name = %(entity_type_name)s;
            ''',
            {
                'entity_type_name': 'film',
            }
        )
        (film_type_id, film_type_conf) = list(cur.fetchone())
        film_type_conf_lookup = {film_type_conf['data'][k]['system_name']: int(k) for k in film_type_conf['data'].keys()}

        cur.execute(
            '''
                SELECT
                    entity.id,
                    entity.config
                FROM app.entity
                WHERE entity.system_name = %(entity_type_name)s;
                ''',
            {
                'entity_type_name': 'person',
            }
        )
        (person_type_id, person_type_conf) = list(cur.fetchone())
        person_type_conf_lookup = {person_type_conf['data'][k]['system_name']: int(k) for k in person_type_conf['data'].keys()}

        cur.execute(
            '''
                SELECT
                    relation.id,
                    relation.config
                FROM app.relation
                WHERE relation.system_name = %(relation_type_name)s;
            ''',
            {
                'relation_type_name': 'director',
            }
        )
        (director_type_id, director_type_conf) = list(cur.fetchone())
        director_type_conf_lookup = {director_type_conf['data'][k]['system_name']: int(k) for k in director_type_conf['data'].keys()}

        cur.execute(
            '''
                DROP GRAPH IF EXISTS g_{project_id} CASCADE;
                CREATE GRAPH g_{project_id};

                CREATE VLABEL v_{film_type_id};
                CREATE VLABEL v_{person_type_id};
                CREATE PROPERTY INDEX ON v_{film_type_id} ( id );
                CREATE PROPERTY INDEX ON v_{film_type_id} ( p_{film_type_id}_0 );
                CREATE PROPERTY INDEX ON v_{person_type_id} ( id );
                CREATE PROPERTY INDEX ON v_{person_type_id} ( p_{person_type_id}_0 );
            '''.format(
                project_id=dtu(project_id),
                film_type_id=dtu(film_type_id),
                person_type_id=dtu(person_type_id),
            )
        )

        with open('data/cinecos_films.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [film_type_conf_lookup['original_id'], header_lookup['film_id'], 'int'],
                'original_id': [film_type_conf_lookup['original_id'], header_lookup['film_id'], 'int'],
                'title': [film_type_conf_lookup['title'], header_lookup['title']],
                'year': [film_type_conf_lookup['year'], header_lookup['film_year'], 'int'],
                'imdb_id': [film_type_conf_lookup['imdb_id'], header_lookup['imdb']],
                'wikidata_id': [film_type_conf_lookup['wikidata_id'], header_lookup['wikidata']],
            }

            params = {
                'entity_type_id': film_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing films')
            batch_process(cur, [r for r in csv_reader], params, add_entity, prop_conf)

        with open('data/cinecos_directors.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [film_type_conf_lookup['original_id'], header_lookup['person_id'], 'int'],
                'original_id': [person_type_conf_lookup['original_id'], header_lookup['person_id'], 'int'],
                'name': [person_type_conf_lookup['name'], header_lookup['name']],
                'wikidata_id': [film_type_conf_lookup['wikidata_id'], header_lookup['wikidata']],
            }

            params = {
                'entity_type_id': person_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing persons')
            batch_process(cur, [r for r in csv_reader], params, add_entity, prop_conf)

        with open('data/cinecos_films_directors.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            relation_config = [header_lookup['film_id'], header_lookup['person_id']]

            prop_conf = {}

            params = {
                'domain_type_id': film_type_id,
                'domain_prop': f'p_{dtu(film_type_id)}_{film_type_conf_lookup["original_id"]}',
                'range_type_id': person_type_id,
                'range_prop': f'p_{dtu(person_type_id)}_{person_type_conf_lookup["original_id"]}',
                'relation_type_id': director_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing director relations')
            batch_process(cur, [r for r in csv_reader], params, add_relation, relation_config, prop_conf)
