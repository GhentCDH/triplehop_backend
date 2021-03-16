import asyncio
import csv
import databases
import tqdm

import config
import utils


async def create_cinecos_structure():
    async with databases.Database(config.DATABASE_CONNECTION_STRING) as db:
        async with db.transaction():
            await utils.init_age(db)

            await db.execute(
                '''
                    INSERT INTO app.project (system_name, display_name, user_id)
                    VALUES (
                        :system_name,
                        :display_name,
                        (SELECT "user".id FROM app.user WHERE "user".username = :username)
                    )
                    ON CONFLICT DO NOTHING;
                ''',
                {
                    'system_name': 'cinecos',
                    'display_name': 'Cinecos',
                    'username': 'info@cinemabelgica.be',
                }
            )

            await db.execute_many(
                '''
                    INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
                    VALUES (
                        (SELECT project.id FROM app.project WHERE system_name = :project_name),
                        :system_name,
                        :display_name,
                        :config,
                        (SELECT "user".id FROM app.user WHERE "user".username = :username)
                    )
                    ON CONFLICT (project_id, system_name) DO UPDATE
                    SET config = EXCLUDED.config;
                ''',
                [
                    {
                        'project_name': 'cinecos',
                        'system_name': 'film',
                        'display_name': 'Film',
                        'config': utils.read_config_from_file('cinecos', 'entity', 'film'),
                        'username': 'info@cinemabelgica.be',
                    },
                ]
            )
            await db.execute(
                '''
                    SELECT drop_graph(
                        (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text,
                        true
                    );
                ''',
                {
                    'project_name': 'cinecos',
                }
            )

            await db.execute(
                '''
                    SELECT create_graph(
                        (SELECT project.id FROM app.project WHERE project.system_name = :project_name)::text
                    );
                ''',
                {
                    'project_name': 'cinecos',
                }
            )


async def create_cinecos_data():
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    async with databases.Database(config.DATABASE_CONNECTION_STRING, statement_cache_size=0) as db:
        async with db.transaction():
            await utils.init_age(db)

            with open('data/tblFilm.csv') as data_file:
                data_csv = csv.reader(data_file)

                params = {
                    'project_name': 'cinecos',
                    'entity_type_name': 'film',
                    'username': 'info@cinemabelgica.be',
                }

                file_header = next(data_csv)
                file_header_lookup = {h: file_header.index(h) for h in file_header}

                db_props_lookup = await utils.get_props_lookup(db, 'cinecos', 'film')

                prop_conf = {
                    'id': ['film_id', 'int'],
                    'original_id': ['film_id', 'int'],
                    'title': ['title'],
                    'year': ['film_year', 'int'],
                    'imdb_id': ['imdb'],
                    'wikidata_id': ['wikidata'],
                }

                print('Cinecos importing films')

                for row in tqdm.tqdm([r for r in data_csv]):
                    await utils.create_entity(
                        db,
                        row,
                        params,
                        db_props_lookup,
                        file_header_lookup,
                        prop_conf
                    )

            with open('data/tblVenue.csv') as data_file:
                data_csv = csv.reader(data_file)

                params = {
                    'project_name': 'cinecos',
                    'entity_type_name': 'film',
                    'username': 'info@cinemabelgica.be',
                }

                file_header = next(data_csv)
                file_header_lookup = {h: file_header.index(h) for h in file_header}

                db_props_lookup = await utils.get_props_lookup(db, 'cinecos', 'film')

                prop_conf = {
                    'id': ['film_id', 'int'],
                    'original_id': ['film_id', 'int'],
                    'title': ['title'],
                    'year': ['film_year', 'int'],
                    'imdb_id': ['imdb'],
                    'wikidata_id': ['wikidata'],
                }

                print('Cinecos importing films')

                for row in tqdm.tqdm([r for r in data_csv]):
                    await utils.create_entity(
                        db,
                        row,
                        params,
                        db_props_lookup,
                        file_header_lookup,
                        prop_conf
                    )


def main():
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(create_cinecos_structure())
    loop.run_until_complete(create_cinecos_data())
    loop.close()


if __name__ == '__main__':
    main()
