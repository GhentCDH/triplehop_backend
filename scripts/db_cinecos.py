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
                'city': 'City',
                'address': 'Address',
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
                'mentioned_film_title': ['Mentioned Film Title', ['film'], ['mentioned_film_title']],
                'address_city': ['City', ['address'], ['city']],
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

        db_domain_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', domain_type_name)
        db_range_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', range_type_name)
        db_props_lookup = await utils.get_relation_props_lookup(db, 'cinecos', relation_type_name)

        return [
            [r for r in data_reader],
            params,
            file_header_lookup,
            db_domain_props_lookup,
            db_range_props_lookup,
            db_props_lookup,
        ]


async def create_data():
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    async with databases.Database(config.DATABASE_CONNECTION_STRING, statement_cache_size=0) as db:
        async with db.transaction():
            await utils.init_age(db)

            # print('Cinecos importing films')
            # (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
            #     db,
            #     'data/tblFilm.csv',
            #     'film'
            # )

            # prop_conf = {
            #     'id': ['int', 'film_id'],
            #     'original_id': ['int', 'film_id'],
            #     'title': ['string', 'title'],
            #     'year': ['int', 'film_year'],
            #     'imdb_id': ['string', 'imdb'],
            #     'wikidata_id': ['string', 'wikidata'],
            # }

            # for row in tqdm.tqdm(data[:10]):
            #     await utils.create_entity(
            #         db,
            #         row,
            #         params,
            #         db_props_lookup,
            #         file_header_lookup,
            #         prop_conf
            #     )

            # print('Cinecos importing film title variations')
            # (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
            #     db,
            #     'data/tblFilmTitleVariation.csv',
            #     'mentioned_film_title'
            # )

            # prop_conf = {
            #     'id': ['int', 'film_id'],
            #     'title': ['string', 'title'],
            # }

            # for row in tqdm.tqdm(data[:10]):
            #     await utils.create_entity(
            #         db,
            #         row,
            #         params,
            #         db_props_lookup,
            #         file_header_lookup,
            #         prop_conf
            #     )

            # print('Cinecos importing relations between films and film title variations')
            # (data, params, file_header_lookup, db_props_lookup) = await get_relation_utils(
            #     db,
            #     'data/tblFilmTitleVariation.csv',
            #     'mentioned_film_title',
            #     'film',
            #     'mentioned_film_title'
            # )

            # prop_conf = {
            #     'domain_id': ['int', 'film_id'],
            #     'range_id': ['int', 'film_variation_id'],
            # }
            # for row in tqdm.tqdm(data[:10]):
            #     await utils.create_relation(
            #         db,
            #         row,
            #         params,
            #         db_props_lookup,
            #         file_header_lookup,
            #         prop_conf
            #     )

            print('Cinecos importing cities')
            (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
                db,
                'data/tblCity.csv',
                'city'
            )

            prop_conf = {
                'id': ['int', 'id'],
                'original_id': ['int', 'id'],
                'name': ['string', 'name'],
                'postal_code': ['int', 'postal_code'],
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

            print('Cinecos importing addresses')
            (data, params, file_header_lookup, db_props_lookup) = await get_entity_utils(
                db,
                'data/tblAddress.csv',
                'address'
            )

            prop_conf = {
                'id': ['int', 'sequential_id'],
                'original_id': ['string', 'address_id'],
                'street_name': ['string', 'street_name'],
                # https://github.com/apache/incubator-age/issues/48
                # 'location': ['point', 'geodata'],
                'district': ['string', 'info'],
                'architectural_info': ['string', 'architectural_info'],
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

            print('Cinecos importing relations between addresses and cities')
            (
                data,
                params,
                file_header_lookup,
                db_domain_props_lookup,
                db_range_props_lookup,
                db_props_lookup
            ) = await get_relation_utils(
                db,
                'data/tblJoinAddressCity.csv',
                'address_city',
                'address',
                'city'
            )

            domain_conf = {
                'original_id': ['string', 'address_id'],
            }
            range_conf = {
                'id': ['int', 'city_id'],
            }
            prop_conf = {}

            for row in tqdm.tqdm(data[:10]):
                await utils.create_relation(
                    db,
                    row,
                    params,
                    db_domain_props_lookup,
                    db_range_props_lookup,
                    db_props_lookup,
                    file_header_lookup,
                    domain_conf,
                    range_conf,
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
