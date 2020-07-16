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
                                        },
                                        {
                                            "field": "5",
                                            "label": "Wikidata images",
                                            "type": "wikidata_images"
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
                                "type": "String"
                            },
                            "7": {
                                "system_name": "type",
                                "display_name": "Type",
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
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'address',
                    'Address',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "street_name",
                                "display_name": "Street name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "location",
                                "display_name": "Location",
                                "type": "Geometry"
                            },
                            "3": {
                                "system_name": "district",
                                "display_name": "District",
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
                                            "type": "geometry"
                                        },
                                        {
                                            "field": "3"
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
                    'city',
                    'City',
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
                                "system_name": "postal_code",
                                "display_name": "Postal code",
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
                SELECT entity.id from app.entity
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
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'address_city',
                    'City',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "City",
                            "range_title": "Address",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue_address',
                    'Address',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Address",
                            "range_title": "Venue",
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
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'address_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'address'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_address'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_range (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT id FROM app.relation WHERE system_name = 'director'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'address_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'city'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_address'),
                    (SELECT id FROM app.entity WHERE system_name = 'address'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_count (id)
                SELECT relation.id FROM app.relation
                ON CONFLICT DO NOTHING;
            '''
        )

        types = {}
        for type_name in [
            'film',
            'person',
            'venue',
            'address',
            'city',
        ]:
            cur.execute(
                '''
                    SELECT
                        entity.id,
                        entity.config
                    FROM app.entity
                    WHERE entity.system_name = %(entity_type_name)s;
                ''',
                {
                    'entity_type_name': type_name,
                }
            )
            (id, conf) = list(cur.fetchone())
            types[type_name] = {
                'id': id,
                'cl': {conf['data'][k]['system_name']: int(k) for k in conf['data'].keys()},
            }

        relations = {}
        for relation_name in [
            'director',
            'address_city',
            'venue_address',
        ]:
            cur.execute(
                '''
                    SELECT
                        relation.id,
                        relation.config
                    FROM app.relation
                    WHERE relation.system_name = %(relation_type_name)s;
                ''',
                {
                    'relation_type_name': relation_name,
                }
            )
            (id, conf) = list(cur.fetchone())
            relations[relation_name] = {
                'id': id,
                'cl': {conf['data'][k]['system_name']: int(k) for k in conf['data'].keys()},
            }

        cur.execute(
            '''
                DROP GRAPH IF EXISTS g_{project_id} CASCADE;
                CREATE GRAPH g_{project_id};
            '''.format(
                project_id=dtu(project_id),
            )
        )

        for id in [v['id'] for v in types.values()]:
            cur.execute(
                '''
                    CREATE VLABEL v_{id};
                    CREATE PROPERTY INDEX ON v_{id} ( id );
                    CREATE PROPERTY INDEX ON v_{id} ( p_{id}_0 );
                '''.format(
                    id=dtu(id),
                )
            )

        with open('data/tblFilm.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['film_id'], 'int'],
                'original_id': [types['film']['cl']['original_id'], file_lookup['film_id'], 'int'],
                'title': [types['film']['cl']['title'], file_lookup['title']],
                'year': [types['film']['cl']['year'], file_lookup['film_year'], 'int'],
                'imdb_id': [types['film']['cl']['imdb_id'], file_lookup['imdb']],
                'wikidata_id': [types['film']['cl']['wikidata_id'], file_lookup['wikidata']],
            }

            params = {
                'entity_type_id': types['film']['id'],
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
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['film_id'], 'int'],
                'title_variations': [types['film']['cl']['title_variations'], file_lookup['title'], 'array'],
            }

            params = {
                'entity_type_id': types['film']['id'],
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
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['person_id'], 'int'],
                'original_id': [types['person']['cl']['original_id'], file_lookup['person_id'], 'int'],
                'name': [types['person']['cl']['name'], file_lookup['name']],
                'wikidata_id': [types['person']['cl']['wikidata_id'], file_lookup['wikidata']],
            }

            params = {
                'entity_type_id': types['person']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing persons')
            batch_process(
                cur,
                [r for r in csv_reader if r[file_lookup['name']] != ''],
                params,
                add_entity,
                prop_conf
            )

        with open('data/tblJoinFilmPerson.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            relation_config = [
                [file_lookup['film_id'], 'int'],
                [file_lookup['person_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['person']['id'],
                'range_prop': f'p_{dtu(types["person"]["id"])}_{types["person"]["cl"]["original_id"]}',
                'relation_type_id': relations['director']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing director relations')
            batch_process(
                cur,
                [r for r in csv_reader if r[file_lookup['info']] == 'director'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblAddress.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('city_id')
            header.append('long')
            header.append('lat')
            file_lookup = {h: header.index(h) for h in header}

            # extract cities from addresses
            city_counter = 1
            city_lookup = {}
            cities = []

            addresses = []
            for row in csv_reader:
                # clean n/a
                for col in ['city_name', 'street_name', 'geodata', 'postal_code', 'info']:
                    if row[file_lookup[col]] in ['N/A', '?']:
                        row[file_lookup[col]] = ''

                # cities
                city_key = f'{row[file_lookup["city_name"]]}_{row[file_lookup["postal_code"]]}'
                if city_key == '_':
                    row.append('')
                else:
                    if city_key not in city_lookup:
                        cities.append([city_counter, row[file_lookup["city_name"]], row[file_lookup["postal_code"]]])
                        city_lookup[city_key] = city_counter
                        city_counter += 1
                    row.append(city_lookup[city_key])

                # long, lat
                if row[file_lookup['geodata']] != '':
                    split = row[file_lookup['geodata']].split(',')
                    if len(split) != 2:
                        print(row)
                    row.append(split[1])
                    row.append(split[0])
                else:
                    row.append('')
                    row.append('')
                addresses.append(row)

            # import cities
            prop_conf = {
                'id': [None, 0, 'int'],
                'original_id': [types['city']['cl']['original_id'], 0, 'int'],
                'name': [types['city']['cl']['name'], 1],
                'postal_code': [types['city']['cl']['postal_code'], 2, 'int'],
            }

            params = {
                'entity_type_id': types['city']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing cities')
            batch_process(
                cur,
                cities,
                params,
                add_entity,
                prop_conf
            )

            # import addresses
            prop_conf = {
                'id': [None, file_lookup['sequential_id'], 'int'],
                'original_id': [types['address']['cl']['original_id'], file_lookup['address_id']],
                'street_name': [types['address']['cl']['street_name'], file_lookup['street_name']],
                'location': [types['address']['cl']['location'], [file_lookup['long'], file_lookup['lat']], 'point'],
                'district': [types['address']['cl']['district'], file_lookup['info']],
            }

            params = {
                'entity_type_id': types['address']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing addresses')
            batch_process(
                cur,
                addresses,
                params,
                add_entity,
                prop_conf
            )

            # import relation between addresses and cities
            relation_config = [
                [file_lookup['address_id']],
                [file_lookup['city_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['address']['id'],
                'domain_prop': f'p_{dtu(types["address"]["id"])}_{types["address"]["cl"]["original_id"]}',
                'range_type_id': types['city']['id'],
                'range_prop': f'p_{dtu(types["city"]["id"])}_{types["city"]["cl"]["original_id"]}',
                'relation_type_id': relations['address_city']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing address city relations')
            batch_process(
                cur,
                [a for a in addresses if a[file_lookup['city_id']] != ''],
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
            file_lookup = {h: header.index(h) for h in header}

            # Process dates: process question marks, X, asterisks and N/A
            venues = []
            for row in csv_reader:
                # Clean up N/A and X
                for col in ['date_opened', 'date_closed']:
                    val = row[file_lookup[col]]
                    if val in ['', 'N/A', 'NA?']:
                        row[file_lookup[col]] = ''
                    elif len(val) == 4 and val[:3].isnumeric() and val[3:] == '?':
                        row[file_lookup[col]] = f'{val[:3]}X'
                    elif len(val) == 5 and val[:3].isnumeric() and val[3:] == 'X?':
                        row[file_lookup[col]] = f'{val[:3]}X'
                    elif val == '1967-1968?':
                        row[file_lookup[col]] = '[1967,1968]'
                    elif val == '1935/36':
                        row[file_lookup[col]] = '[1935,1936]'
                    elif val == '1962/68':
                        row[file_lookup[col]] = '[1963..1968]'

                for col in ['date_opened', 'date_closed']:
                    val = row[file_lookup[col]]
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
                    if row[file_lookup[col]] == '' and row[file_lookup[other_col]] != '':
                        val = row[file_lookup[other_col]]
                        if val.isnumeric():
                            row[file_lookup[f'{col}_system']] = val
                        elif len(val) == 4 and val[:3].isnumeric() and val[3:] == 'X':
                            if col == 'date_opened':
                                row[file_lookup[f'{col}_system']] = f'{val[:3]}0'
                            else:
                                row[file_lookup[f'{col}_system']] = f'{val[:3]}9'
                        elif len(val) == 5 and val[:4].isnumeric() and val[4:] == '?':
                            row[file_lookup[f'{col}_system']] = val[:4]
                        else:
                            print('incorrect date when creating interval')
                            print(row)
                            print(col)
                            print(val)

                venues.append(row)

            # import venues
            prop_conf = {
                'id': [None, file_lookup['sequential_id'], 'int'],
                'original_id': [types['venue']['cl']['original_id'], file_lookup['venue_id']],
                'name': [types['venue']['cl']['name'], file_lookup['name']],
                'date_opened_display': [types['venue']['cl']['date_opened_display'], file_lookup['date_opened']],
                'date_opened': [types['venue']['cl']['date_opened'], file_lookup['date_opened_system']],
                'date_closed_display': [types['venue']['cl']['date_closed_display'], file_lookup['date_closed']],
                'date_closed': [types['venue']['cl']['date_closed'], file_lookup['date_closed_system']],
                'status': [types['venue']['cl']['status'], file_lookup['status']],
                'type': [types['venue']['cl']['type'], file_lookup['type']],
            }

            params = {
                'entity_type_id': types['venue']['id'],
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

            # import relation between venues and addresses
            relation_config = [
                [file_lookup['venue_id']],
                [file_lookup['address_id']],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['venue']['id'],
                'domain_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'range_type_id': types['address']['id'],
                'range_prop': f'p_{dtu(types["address"]["id"])}_{types["address"]["cl"]["original_id"]}',
                'relation_type_id': relations['venue_address']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venue address relations')
            batch_process(
                cur,
                [v for v in venues if v[file_lookup['address_id']] != ''],
                params,
                add_relation,
                relation_config,
                prop_conf
            )
