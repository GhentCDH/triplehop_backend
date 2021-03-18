from databases import Database

import asyncio
import csv
import databases
import tqdm

import config
import utils


async def create_structure():
    async with databases.Database(config.DATABASE_CONNECTION_STRING) as db:
        async with db.transaction():
            await utils.init_age(db)

            await utils.create_project_config(db, 'cinecos', 'Cinecos', 'info@cinemabelgica.be')

            entities_types = {
                'film': 'Film',
                'mentioned_film_title': 'Mentioned Film Title',
                'venue': 'Venue',
            }
            for (system_name, display_name) in entities_types.items():
                await utils.create_entity_config(
                    db,
                    'cinecos',
                    'info@cinemabelgica.be',
                    system_name,
                    display_name,
                    utils.read_config_from_file('cinecos', 'entity', system_name),
                )

            relation_types = {
                'mentioned_film_title': ['Mentioned Film Title', ['film'], ['mentioned_film_title']]
            }
            for (system_name, (display_name, domains, ranges)) in relation_types.items():
                await utils.create_relation_config(
                    db,
                    'cinecos',
                    'info@cinemabelgica.be',
                    system_name,
                    display_name,
                    utils.read_config_from_file('cinecos', 'relation', system_name),
                    domains,
                    ranges,
                )

            await utils.drop_project_graph(db, 'cinecos')
            await utils.create_project_graph(db, 'cinecos')


async def get_entity_utils(db: Database, filename: str, entity_type_name: str):
    with open(filename) as data_file:
        data_reader = csv.reader(data_file)

        params = {
            'project_name': 'cinecos',
            'entity_type_name': entity_type_name,
            'username': 'info@cinemabelgica.be',
        }

        file_header = next(data_reader)
        file_header_lookup = {h: file_header.index(h) for h in file_header}

        db_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', entity_type_name)

        return [[r for r in data_reader], params, file_header_lookup, db_props_lookup]


async def get_relation_utils(
    db: Database,
    filename: str,
    relation_type_name: str,
    domain_type_name: str,
    range_type_name: str
):
    with open(filename) as data_file:
        data_reader = csv.reader(data_file)

        params = {
            'project_name': 'cinecos',
            'relation_type_name': relation_type_name,
            'domain_type_name': domain_type_name,
            'range_type_name': range_type_name,
            'username': 'info@cinemabelgica.be',
        }

        file_header = next(data_reader)
        file_header_lookup = {h: file_header.index(h) for h in file_header}

        db_props_lookup = await utils.get_relation_props_lookup(db, 'cinecos', relation_type_name)

        return [[r for r in data_reader], params, file_header_lookup, db_props_lookup]


async def create_data():
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    async with databases.Database(config.DATABASE_CONNECTION_STRING, statement_cache_size=0) as db:
        async with db.transaction():
            await utils.init_age(db)

            print('Cinecos importing films')
            (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
                db,
                'data/tblFilm.csv',
                'film'
            )

            prop_conf = {
                'id': ['film_id', 'int'],
                'original_id': ['film_id', 'int'],
                'title': ['title', 'string'],
                'year': ['film_year', 'int'],
                'imdb_id': ['imdb', 'string'],
                'wikidata_id': ['wikidata', 'string'],
            }

            for row in tqdm.tqdm(data[:10]):
                await utils.create_entity(
                    db,
                    row,
                    params,
                    db_props_lookup,
                    file_header_lookup,
                    prop_conf
                )

            print('Cinecos importing film title variations')
            (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
                db,
                'data/tblFilmTitleVariation.csv',
                'mentioned_film_title'
            )

            prop_conf = {
                'id': ['film_id', 'int'],
                'title': ['title', 'string'],
            }

            for row in tqdm.tqdm(data[:10]):
                await utils.create_entity(
                    db,
                    row,
                    params,
                    db_props_lookup,
                    file_header_lookup,
                    prop_conf
                )

            print('Cinecos importing relations between films and film title variations')
            (data, params, file_header_lookup, db_props_lookup) = await get_relation_utils(
                db,
                'data/tblFilmTitleVariation.csv',
                'mentioned_film_title'
            )

            prop_conf = {
                'domain_id': ['film_id', 'int'],
                'range_id': ['film_title_variation_id', 'int'],
            }
            for row in tqdm.tqdm(data[:10]):
                await utils.create_relation(
                    db,
                    row,
                    params,
                    db_props_lookup,
                    file_header_lookup,
                    prop_conf
                )

            # with open('data/tblVenue.csv') as data_file:
            #     data_csv = csv.reader(data_file)

            #     params = {
            #         'project_name': 'cinecos',
            #         'entity_type_name': 'film',
            #         'username': 'info@cinemabelgica.be',
            #     }

            #     file_header = next(data_csv)
            #     file_header_lookup = {h: file_header.index(h) for h in file_header}

            #     db_props_lookup = await utils.get_props_lookup(db, 'cinecos', 'film')

            #     prop_conf = {
            #         'id': ['film_id', 'int'],
            #         'original_id': ['film_id', 'int'],
            #         'title': ['title', 'string'],
            #         'year': ['film_year', 'int'],
            #         'imdb_id': ['imdb', 'string'],
            #         'wikidata_id': ['wikidata', 'string'],
            #     }

            #     print('Cinecos importing films')

            #     for row in tqdm.tqdm([r for r in data_csv]):
            #         await utils.create_entity(
            #             db,
            #             row,
            #             params,
            #             db_props_lookup,
            #             file_header_lookup,
            #             prop_conf
            #         )


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_structure())
    loop.run_until_complete(create_data())
    loop.close()


if __name__ == '__main__':
    main()
