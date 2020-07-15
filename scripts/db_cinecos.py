import csv
import psycopg2

from config import DATABASE_CONNECTION_STRING

from utils import add_entity, add_relation, batch_process, dtu, update_entity

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
                                "system_name": "title_variations",
                                "display_name": "Alternative title(s)",
                                "type": "[String]"
                            },
                            "3": {
                                "system_name": "year",
                                "display_name": "Production year",
                                "type": "Int"
                            },
                            "4": {
                                "system_name": "imdb_id",
                                "display_name": "IMDb ID",
                                "type": "String"
                            },
                            "5": {
                                "system_name": "wikidata_id",
                                "display_name": "Wikidata ID",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1 ($3)",
                            "layout": [
                                {
                                    "label": "General",
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2",
                                            "type": "list"
                                        },
                                        {
                                            "field": "3"
                                        },
                                        {
                                            "field": "4",
                                            "type": "online_identifier",
                                            "base_url": "https://www.imdb.com/title/"
                                        },
                                        {
                                            "field": "5",
                                            "type": "online_identifier",
                                            "base_url": "https://www.wikidata.org/wiki/"
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
                                "system_name": "title_variations",
                                "display_name": "Alternative title(s)",
                                "selector_value": "$title_variations",
                                "type": "text_array"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Production year",
                                "selector_value": "$year",
                                "type": "integer"
                            },
                            "3": {
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
                                        "type": "autocomplete"
                                    },
                                    {
                                        "filter": "2",
                                        "type": "histogram_slider",
                                        "interval": 10
                                    },
                                    {
                                        "filter": "3"
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
                            },
                            {
                                "column": "3",
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
                                            "field": "2",
                                            "type": "online_identifier",
                                            "base_url": "https://www.wikidata.org/wiki/"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue',
                    'Venue',
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
                                "system_name": "date_opened_display",
                                "display_name": "Date opened",
                                "type": "String"
                            },
                            "3": {
                                "system_name": "date_opened",
                                "display_name": "Date opened",
                                "type": "Int"
                            },
                            "4": {
                                "system_name": "date_closed_display",
                                "display_name": "Date closed",
                                "type": "String"
                            },
                            "5": {
                                "system_name": "date_closed",
                                "display_name": "Date opened",
                                "type": "Int"
                            },
                            "6": {
                                "system_name": "status",
                                "display_name": "Status",
                                "type": "Int"
                            },
                            "7": {
                                "system_name": "type",
                                "display_name": "Type",
                                "type": "Int"
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
                                        },
                                        {
                                            "field": "4"
                                        },
                                        {
                                            "field": "6"
                                        },
                                        {
                                            "field": "7"
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
                SELECT
                    entity.id,
                    entity.config
                FROM app.entity
                WHERE entity.system_name = %(entity_type_name)s;
                ''',
            {
                'entity_type_name': 'venue',
            }
        )
        (venue_type_id, venue_type_conf) = list(cur.fetchone())
        venue_type_conf_lookup = {venue_type_conf['data'][k]['system_name']: int(k) for k in venue_type_conf['data'].keys()}

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

        with open('data/tblFilm.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, header_lookup['film_id'], 'int'],
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
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                add_entity,
                prop_conf,
            )

        with open('data/tblFilmTitleVariation.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, header_lookup['film_id'], 'int'],
                'title_variations': [film_type_conf_lookup['title_variations'], header_lookup['title'], 'array'],
            }

            params = {
                'entity_type_id': film_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing film title variations')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                update_entity,
                prop_conf
            )

        with open('data/tblPerson.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, header_lookup['person_id'], 'int'],
                'original_id': [person_type_conf_lookup['original_id'], header_lookup['person_id'], 'int'],
                'name': [person_type_conf_lookup['name'], header_lookup['name']],
                'wikidata_id': [person_type_conf_lookup['wikidata_id'], header_lookup['wikidata']],
            }

            params = {
                'entity_type_id': person_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing persons')
            batch_process(
                cur,
                [r for r in csv_reader if r[header_lookup['name']] != ''],
                params,
                add_entity,
                prop_conf
            )

        with open('data/tblJoinFilmPerson.csv') as input_file:
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
            batch_process(
                cur,
                [r for r in csv_reader if r[header_lookup['info']] == 'director'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblVenue.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('date_opened_system')
            header.append('date_closed_system')
            header_lookup = {h: header.index(h) for h in header}

            # Process dates: process question marks, X, asterisks and N/A
            venues = []
            for row in csv_reader:
                # Clean up N/A and X
                for col in ['date_opened', 'date_closed']:
                    val = row[header_lookup[col]]
                    if val in ['', 'N/A', 'NA?']:
                        row[header_lookup[col]] = ''
                    elif len(val) == 4 and val[:3].isnumeric() and val[3:] == '?':
                        row[header_lookup[col]] = f'{val[:3]}X'
                    elif len(val) == 5 and val[:3].isnumeric() and val[3:] == 'X?':
                        row[header_lookup[col]] = f'{val[:3]}X'
                    elif val == '1967-1968?':
                        row[header_lookup[col]] = '[1967,1968]'
                    elif val == '1935/36':
                        row[header_lookup[col]] = '[1935,1936]'
                    elif val == '1962/68':
                        row[header_lookup[col]] = '[1963..1968]'

                for col in ['date_opened', 'date_closed']:
                    val = row[header_lookup[col]]
                    if val == '':
                        row.append('')
                    elif val == '*':
                        row.append('..')
                    elif val.isnumeric():
                        row.append(val)
                    elif len(val) == 4 and val[:3].isnumeric() and val[3:] == 'X':
                        # Make interval as wide as possible
                        if col == 'date_opened':
                            row.append(f'{val[:3]}0')
                        else:
                            row.append(f'{val[:3]}9')
                    elif len(val) == 5 and val[:4].isnumeric() and val[4:] == '?':
                        row.append(val[:4])
                    elif val[0] == '[' and val[-1] == ']':
                        # Make interval as wide as possible
                        if col == 'date_opened':
                            row.append(val[1:-1].replace('..', ',').split(',')[0])
                        else:
                            row.append(val[1:-1].replace('..', ',').split(',')[-1])
                    else:
                        print('incorrect date')
                        print(row)
                        print(col)
                        print(val)

                # create an interval if only opening or closing year is known
                for col in ['date_opened', 'date_closed']:
                    other_col = 'date_opened' if col == 'date_closed' else 'date_opened'
                    if row[header_lookup[col]] == '' and row[header_lookup[other_col]] != '':
                        val = row[header_lookup[other_col]]
                        if val.isnumeric():
                            row[header_lookup[f'{col}_system']] = val
                        elif len(val) == 4 and val[:3].isnumeric() and val[3:] == 'X':
                            if col == 'date_opened':
                                row[header_lookup[f'{col}_system']] = f'{val[:3]}0'
                            else:
                                row[header_lookup[f'{col}_system']] = f'{val[:3]}9'
                        elif len(val) == 5 and val[:4].isnumeric() and val[4:] == '?':
                            row[header_lookup[f'{col}_system']] = val[:4]
                        else:
                            print('incorrect date when creating interval')
                            print(row)
                            print(col)
                            print(val)

                venues.append(row)

            prop_conf = {
                'id': [None, header_lookup['sequential_id'], 'int'],
                'original_id': [venue_type_conf_lookup['original_id'], header_lookup['venue_id']],
                'name': [venue_type_conf_lookup['name'], header_lookup['name']],
                'date_opened_display': [venue_type_conf_lookup['date_opened_display'], header_lookup['date_opened']],
                'date_opened': [venue_type_conf_lookup['date_opened'], header_lookup['date_opened_system']],
                'date_closed_display': [venue_type_conf_lookup['date_closed_display'], header_lookup['date_closed']],
                'date_closed': [venue_type_conf_lookup['date_closed'], header_lookup['date_closed_system']],
                'status': [venue_type_conf_lookup['status'], header_lookup['status']],
                'type': [venue_type_conf_lookup['type'], header_lookup['type']],
            }

            params = {
                'entity_type_id': person_type_id,
                'user_id': user_id,
            }

            print('Cinecos importing venues')
            batch_process(
                cur,
                venues,
                params,
                add_entity,
                prop_conf
            )
