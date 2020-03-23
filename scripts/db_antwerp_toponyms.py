import csv
import psycopg2

from utils import add_entity, add_relation, batch_process, dtu

# TODO: precalculate overlaps between e.g. streets
# https://shapely.readthedocs.io/en/latest/manual.html
# Example for streets:
# For each linestring: calculate buffer
# For each 2 linestrings (corresponding streets must be from a different year):
# Calculate intersection of buffers compared to buffers of both single linestrings and compared to the union

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute(
            '''
                SELECT "user".id
                FROM app.user
                WHERE "user".username = %(username)s;
            ''',
            {
                'username': 'pieterjan.depotter@ugent.be',
            }
        )
        user_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.project (system_name, display_name, user_id)
                VALUES ('antwerp_toponyms', 'Antwerp toponyms', %(user_id)s)
                ON CONFLICT DO NOTHING;
            ''',
            {
                'user_id': user_id,
            }
        )

        cur.execute(
            '''
                SELECT project.id
                FROM app.project
                WHERE project.system_name = %(project)s;
            ''',
            {
                'project': 'antwerp_toponyms',
            }
        )
        project_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE system_name = 'antwerp_toponyms'),
                    'area',
                    'Area',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "toponym",
                                "display_name": "Toponym",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "gis_base_layers",
                                "display_name": "GIS base layers",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "geometry",
                                "display_name": "Geometry",
                                "type": "Geometry"
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
                                            "field": "4",
                                            "type": "geometry",
                                            "base_layer": "3"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'antwerp_toponyms'),
                    'house',
                    'House',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "toponym",
                                "display_name": "Toponym",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "gis_base_layers",
                                "display_name": "GIS base layers",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "geometry",
                                "display_name": "Geometry",
                                "type": "Geometry"
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
                                            "field": "4",
                                            "type": "geometry",
                                            "base_layer": "3"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'antwerp_toponyms'),
                    'street',
                    'Street',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "toponym",
                                "display_name": "Toponym",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "gis_base_layers",
                                "display_name": "GIS base layers",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "geometry",
                                "display_name": "Geometry",
                                "type": "Geometry"
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
                                            "field": "4",
                                            "type": "geometry",
                                            "base_layer": "3"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'antwerp_toponyms'),
                    'cadastral_number',
                    'Cadastral number',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "toponym",
                                "display_name": "Toponym",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "gis_base_layers",
                                "display_name": "GIS base layers",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "geometry",
                                "display_name": "Geometry",
                                "type": "Geometry"
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
                                            "field": "4",
                                            "type": "geometry",
                                            "base_layer": "3"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'antwerp_toponyms'),
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
                                "system_name": "toponym",
                                "display_name": "Toponym",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Year",
                                "type": "Int"
                            },
                            "3": {
                                "system_name": "gis_base_layers",
                                "display_name": "GIS base layers",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "geometry",
                                "display_name": "Geometry",
                                "type": "Geometry"
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
                                            "field": "4",
                                            "type": "geometry",
                                            "base_layer": "3"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'pieterjan.depotter@ugent.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.entity_count (id)
                VALUES
                    ((SELECT entity.id FROM app.entity WHERE entity.system_name = 'area')),
                    ((SELECT entity.id FROM app.entity WHERE entity.system_name = 'house')),
                    ((SELECT entity.id FROM app.entity WHERE entity.system_name = 'street')),
                    ((SELECT entity.id FROM app.entity WHERE entity.system_name = 'cadastral_number')),
                    ((SELECT entity.id FROM app.entity WHERE entity.system_name = 'address'))
                ON CONFLICT DO NOTHING;
            '''
        )

        cur.execute(
            '''
                DROP GRAPH IF EXISTS g_{project_id} CASCADE;
                CREATE GRAPH g_{project_id};
            '''.format(
                project_id=dtu(project_id),
            )
        )

        type_mapping = {
            'area': 'Area',
            'house': 'House name',
            'street': 'Street name',
            'cadastral_number': 'Cadastral number',
            'address': 'Address',
        }

        with open('data/antwerp_toponyms.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header_lookup = {h: header.index(h) for h in header}

        for row in csv_reader:
            if row[header_lookup['Type']] not in type_mapping.values():
                print(row[header_lookup['Type']])

        for type in type_mapping:
            cur.execute(
                '''
                    SELECT
                        entity.id,
                        entity.config
                    FROM app.entity
                    WHERE entity.system_name = %(entity_type_name)s;
                ''',
                {
                    'entity_type_name': type,
                }
            )
            (type_id, type_conf) = list(cur.fetchone())
            type_conf_lookup = {type_conf['data'][k]['system_name']: int(k) for k in type_conf['data'].keys()}

            cur.execute(
                '''
                    CREATE VLABEL v_{type_id};
                    CREATE PROPERTY INDEX ON v_{type_id} ( id );
                    CREATE PROPERTY INDEX ON v_{type_id} ( p_{type_id}_0 );
                '''.format(
                    type_id=dtu(type_id),
                )
            )

            with open('data/antwerp_toponyms.csv') as input_file:
                lines = input_file.readlines()
                csv_reader = csv.reader(lines)

                header = next(csv_reader)
                header_lookup = {h: header.index(h) for h in header}

                prop_conf = {
                    'original_id': [type_conf_lookup['original_id'], header_lookup['FID'], 'int'],
                    'toponym': [type_conf_lookup['toponym'], header_lookup['Toponym']],
                    'year': [type_conf_lookup['year'], header_lookup['Date'], 'int'],
                    'gis_base_layers': [type_conf_lookup['gis_base_layers'], header_lookup['GIS base layers']],
                    'geometry': [type_conf_lookup['geometry'], [header_lookup['LONG'], header_lookup['LAT']], 'point'],
                }

                params = {
                    'entity_type_id': type_id,
                    'user_id': user_id,
                }

                data = [r for r in csv_reader if r[header_lookup['Type']] == type_mapping[type]]

                print(f'Antwerp toponyms importing {type}')
                batch_process(cur, data, params, add_entity, prop_conf)
