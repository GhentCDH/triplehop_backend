import csv
import psycopg2

import utils

# TODO: precalculate overlaps between e.g. streets
# https://shapely.readthedocs.io/en/latest/manual.html
# Example for streets:
# For each linestring: calculate buffer
# For each 2 linestrings (corresponding streets must be from a different year):
# Calculate intersection of buffers compared to buffers of both single linestrings and compared to the union

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
        VALUES ('antwerp_toponyms', 'Antwerp toponyms', %(user_id)s)
        ON CONFLICT DO NOTHING;
        ''', {
            'user_id': user_id,
        })

        cur.execute('''
            SELECT project.id
            FROM app.project
            WHERE project.system_name = %(project)s;
            ''', {
                'project': 'antwerp_toponyms',
        })
        project_id = cur.fetchone()[0]

        cur.execute('''
        INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
        VALUES (
            %(project_id)s,
            'area',
            'Area',
            '{
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
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'house',
            'House',
            '{
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
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'street',
            'Street',
            '{
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
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'cadastral_number',
            'Cadastral number',
            '{
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
            }',
            %(user_id)s
        ),
        (
            %(project_id)s,
            'address',
            'Address',
            '{
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
            }',
            %(user_id)s
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO app.entity_count (id) SELECT id FROM app.entity WHERE system_name = 'area' ON CONFLICT DO NOTHING;
        INSERT INTO app.entity_count (id) SELECT id FROM app.entity WHERE system_name = 'house' ON CONFLICT DO NOTHING;
        INSERT INTO app.entity_count (id) SELECT id FROM app.entity WHERE system_name = 'street' ON CONFLICT DO NOTHING;
        INSERT INTO app.entity_count (id) SELECT id FROM app.entity WHERE system_name = 'cadastral_number' ON CONFLICT DO NOTHING;
        INSERT INTO app.entity_count (id) SELECT id FROM app.entity WHERE system_name = 'address' ON CONFLICT DO NOTHING;
        ''', {
            'project_id': project_id,
            'user_id': user_id,
        })

        cur.execute('''
        DROP GRAPH IF EXISTS g%(project_id)s CASCADE;
        CREATE GRAPH g%(project_id)s;
        ''', {
            'project_id': project_id,
        })

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
            cur.execute('''
                SELECT
                    entity.id,
                    entity.config
                FROM app.entity
                WHERE entity.system_name = %(entity_type_name)s;
                ''', {
                    'entity_type_name': type,
            })
            (type_id, type_conf) = list(cur.fetchone())
            type_conf_lookup = {type_conf[k]['system_name']: int(k) for k in type_conf.keys()}

            cur.execute('''
            CREATE VLABEL v%(type_id)s;
            CREATE PROPERTY INDEX ON v%(type_id)s ( id );
            CREATE PROPERTY INDEX ON v%(type_id)s ( p%(type_id)s_0 );
            ''', {
                'type_id': type_id,
            })

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
                utils.batch_process(cur, data, params, utils.add_entity, prop_conf)
