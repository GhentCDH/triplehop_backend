from asyncio import get_event_loop
from asyncpg.exceptions import InvalidSchemaNameError
from csv import reader as csv_reader
from databases import Database
# from datetime import datetime, timedelta
from tqdm import tqdm

from config import DATABASE_CONNECTION_STRING
# from utils import add_entity, add_relation, batch_process, dtu, read_config_from_file, update_entity
from utils import create_entity, get_entity_type_id, get_props_lookup, get_user_id, read_config_from_file, set_path

# venue address hack:
# * add postal_code, city_name, street_name, geodata directly to venue
# * add a relation directly from venue to city
#
# programe hack:
# * add venue name directly to programme
# * add relation directly from programme to film
#
# programe item hack:
# * add film title, venue name, programme start and end date directly to programme item
#
# person function hack:
# * add function directly to person
#
# company function hack:
# * add function directly to company
person_functions = set()
company_functions = set()


async def create_cinecos_structure():
    async with Database(DATABASE_CONNECTION_STRING) as db:
        async with db.transaction():
            await set_path(db)

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
                        'config': read_config_from_file('cinecos', 'entity', 'film'),
                        'username': 'info@cinemabelgica.be',
                    },
                ]
            )

            try:
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
            except InvalidSchemaNameError as e:
                print(e)

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
    async with Database(DATABASE_CONNECTION_STRING) as db:
        with open('data/tblFilm.csv') as data_file:
            data_csv = csv_reader(data_file)

            params = {
                'entity_type_id': await get_entity_type_id(db, 'cinecos', 'film'),
                'user_id': await get_user_id(db, 'info@cinemabelgica.be'),
            }

            file_header = next(data_csv)
            file_header_lookup = {h: file_header.index(h) for h in file_header}

            db_props_lookup = await get_props_lookup(db, 'cinecos', 'film')

            prop_conf = {
                'id': ['film_id', 'int'],
                'original_id': ['film_id', 'int'],
                'title': ['title'],
                'year': ['film_year', 'int'],
                'imdb_id': ['imdb'],
                'wikidata_id': ['wikidata'],
            }

            print('Cinecos importing films')
            for row in tqdm(data_csv):
                await create_entity(db, params, db_props_lookup, file_header_lookup, row, prop_conf)


def main():
    loop = get_event_loop()
    loop.run_until_complete(create_cinecos_structure())
    loop.run_until_complete(create_cinecos_data())
    loop.close()

if __name__ == '__main__':
    main()
