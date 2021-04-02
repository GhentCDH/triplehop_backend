from databases import Database
from typing import Callable, Dict, Iterable

import asyncio
import csv
import databases
import tqdm

import config
import utils


async def batch(method: Callable, data: Iterable, *args):
    counter = 0
    batch = []
    for row in tqdm.tqdm([r for r in data]):
        counter += 1
        batch.append(row)
        if not counter % 500:
            await method(*args, batch)
            batch = []
    if len(batch):
        await method(*args, batch)


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
                'venue_address': ['Address', ['venue'], ['address']],
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


async def create_entity(db: Database, conf: Dict):
    print(f'Cinecos importing entity {conf["entity_type_name"]}')
    with open(f'data/processed/{conf["filename"]}') as data_file:
        data_reader = csv.reader(data_file)

        params = {
            'project_name': 'cinecos',
            'entity_type_name': conf['entity_type_name'],
            'username': 'info@cinemabelgica.be',
        }

        file_header = next(data_reader)
        file_header_lookup = {h: file_header.index(h) for h in file_header}

        db_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', conf['entity_type_name'])

        await batch(
            utils.create_entities,
            data_reader,
            db,
            params,
            db_props_lookup,
            file_header_lookup,
            conf['props']
        )


async def create_relation(db: Database, conf: Dict):
    print(f'Cinecos importing relation {conf["relation_type_name"]}')
    with open(f'data/processed/{conf["filename"]}') as data_file:
        data_reader = csv.reader(data_file)

        params = {
            'project_name': 'cinecos',
            'relation_type_name': conf['relation_type_name'],
            'domain_type_name': conf['domain_type_name'],
            'range_type_name': conf['range_type_name'],
            'username': 'info@cinemabelgica.be',
        }

        file_header = next(data_reader)
        file_header_lookup = {h: file_header.index(h) for h in file_header}

        db_domain_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', conf['domain_type_name'])
        db_range_props_lookup = await utils.get_entity_props_lookup(db, 'cinecos', conf['range_type_name'])
        db_props_lookup = await utils.get_relation_props_lookup(db, 'cinecos', conf['relation_type_name'])

        await batch(
            utils.create_relations,
            data_reader,
            db,
            params,
            db_domain_props_lookup,
            db_range_props_lookup,
            db_props_lookup,
            file_header_lookup,
            conf['domain'],
            conf['range'],
            conf['props']
        )


async def create_data():
    async with databases.Database(config.DATABASE_CONNECTION_STRING) as db:
        async with db.transaction():
            await utils.init_age(db)

            await create_entity(
                db,
                {
                    'filename': 'tblFilm.csv',
                    'entity_type_name': 'film',
                    'props': {
                        'id': ['int', 'film_id'],
                        'original_id': ['int', 'film_id'],
                        'title': ['string', 'title'],
                        'year': ['int', 'film_year'],
                        'imdb_id': ['string', 'imdb'],
                        'wikidata_id': ['string', 'wikidata'],
                    },
                }
            )

            await create_entity(
                db,
                {
                    'filename': 'tblFilmTitleVariation.csv',
                    'entity_type_name': 'mentioned_film_title',
                    'props': {
                        'id': ['int', 'film_variation_id'],
                        'title': ['string', 'title'],
                    },
                }
            )

            await create_relation(
                db,
                {
                    'filename': 'tblFilmTitleVariation.csv',
                    'relation_type_name': 'mentioned_film_title',
                    'domain_type_name': 'film',
                    'range_type_name': 'mentioned_film_title',
                    'domain': {
                        'id': ['int', 'film_id'],
                    },
                    'range': {
                        'id': ['int', 'film_variation_id'],
                    },
                    'props': {}
                }
            )

            await create_entity(
                db,
                {
                    'filename': 'tblCity.csv',
                    'entity_type_name': 'city',
                    'props': {
                        'id': ['int', 'id'],
                        'original_id': ['int', 'id'],
                        'name': ['string', 'name'],
                        'postal_code': ['int', 'postal_code'],
                    },
                }
            )

            await create_entity(
                db,
                {
                    'filename': 'tblAddress.csv',
                    'entity_type_name': 'address',
                    'props': {
                        'id': ['int', 'sequential_id'],
                        'original_id': ['string', 'address_id'],
                        'street_name': ['string', 'street_name'],
                        # https://github.com/apache/incubator-age/issues/48
                        # 'location': ['point', 'geodata'],
                        'district': ['string', 'info'],
                        'architectural_info': ['string', 'architectural_info'],
                    },
                }
            )

            await create_relation(
                db,
                {
                    'filename': 'tblJoinAddressCity.csv',
                    'relation_type_name': 'address_city',
                    'domain_type_name': 'address',
                    'range_type_name': 'city',
                    'domain': {
                        'original_id': ['string', 'address_id'],
                    },
                    'range': {
                        'id': ['int', 'city_id'],
                    },
                    'props': {}
                }
            )

            await create_entity(
                db,
                {
                    'filename': 'tblVenue.csv',
                    'entity_type_name': 'venue',
                    'props': {
                        'id': ['int', 'sequential_id'],
                        'original_id': ['string', 'venue_id'],
                        'name': ['string', 'name'],
                        'date_opened':  ['string', 'date_opened'],
                        'date_closed':  ['string', 'date_closed'],
                        'status':  ['string', 'status'],
                        'type':  ['string', 'type'],
                        'ideological_characteristic':  ['string', 'ideological_characteristic'],
                        'ideological_remark':  ['string', 'ideological_remark'],
                        'infrastructure_info':  ['string', 'infrastructure_info'],
                        'name_remarks':  ['string', 'name_remarks'],
                    },
                }
            )

            await create_relation(
                db,
                {
                    'filename': 'tblVenue.csv',
                    'relation_type_name': 'venue_address',
                    'domain_type_name': 'venue',
                    'range_type_name': 'address',
                    'domain': {
                        'original_id': ['string', 'venue_id'],
                    },
                    'range': {
                        'original_id': ['string', 'address_id'],
                    },
                    'props': {}
                }
            )


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_structure())
    loop.run_until_complete(create_data())
    loop.close()


if __name__ == '__main__':
    main()
