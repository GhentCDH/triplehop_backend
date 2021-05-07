import asyncio
import asyncpg
import csv
import tqdm
import typing

import config
import utils


async def batch(method: typing.Callable, data: typing.Iterable, *args):
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
    conn = await asyncpg.connect(**config.DATABASE)
    await utils.init_age(conn)

    await utils.create_project_config(conn, 'cinecos', 'Cinecos', 'info@cinemabelgica.be')

    entities_types = {
        'film': 'Film',
        'mentioned_film_title': 'Mentioned Film Title',
        'city': 'City',
        'address': 'Address',
        'venue': 'Venue',
        'person': 'Person'
    }
    for (system_name, display_name) in entities_types.items():
        await utils.create_entity_config(
            conn,
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
            conn,
            'cinecos',
            'info@cinemabelgica.be',
            system_name,
            display_name,
            utils.read_config_from_file('cinecos', 'relation', system_name),
            domains,
            ranges,
        )

    await utils.drop_project_graph(conn, 'cinecos')
    await utils.create_project_graph(conn, 'cinecos')

    await conn.close()


async def create_entity(conn: asyncpg.connection.Connection, conf: typing.Dict):
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

        db_props_lookup = await utils.get_entity_props_lookup(conn, 'cinecos', conf['entity_type_name'])

        await batch(
            utils.create_entities,
            data_reader,
            conn,
            params,
            db_props_lookup,
            file_header_lookup,
            conf['props']
        )


async def create_relation(conn: asyncpg.connection.Connection, conf: typing.Dict):
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

        db_domain_props_lookup = await utils.get_entity_props_lookup(conn, 'cinecos', conf['domain_type_name'])
        db_range_props_lookup = await utils.get_entity_props_lookup(conn, 'cinecos', conf['range_type_name'])
        db_props_lookup = await utils.get_relation_props_lookup(conn, 'cinecos', conf['relation_type_name'])

        await batch(
            utils.create_relations,
            data_reader,
            conn,
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
    conn = await asyncpg.connect(**config.DATABASE)
    await utils.init_age(conn)

    await create_entity(
        conn,
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
        conn,
        {
            'filename': 'tblFilmTitleVariation.csv',
            'entity_type_name': 'mentioned_film_title',
            'props': {
                'id': ['int', 'film_variation_id'],
                'title': ['string', 'title'],
            },
        }
    )

    await create_entity(
        conn,
        {
            'filename': 'tblPersonWithFirstNames.csv',
            'entity_type_name': 'person',
            'props': {
                'id': ['int', 'person_id'],
                'original_id': ['int', 'person_id'],
                'first_names': ['array[string]', 'first_names', '|'],
                'last_name': ['string', 'last_name'],
                'suffix': ['string', 'suffix'],
                'name': ['string', 'name'],
                'info': ['string', 'info'],
                'imdb_id': ['string', 'imdb'],
                'wikidata_id': ['string', 'wikidata'],
            },
        }
    )

    await create_relation(
        conn,
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
        conn,
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
        conn,
        {
            'filename': 'tblAddressWithGeoJson.csv',
            'entity_type_name': 'address',
            'props': {
                'id': ['int', 'sequential_id'],
                'original_id': ['string', 'address_id'],
                'street_name': ['string', 'street_name'],
                'location': ['geometry', 'geodata'],
                'district': ['string', 'info'],
                'architectural_info': ['string', 'architectural_info'],
            },
        }
    )

    await create_relation(
        conn,
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
        conn,
        {
            'filename': 'tblVenue.csv',
            'entity_type_name': 'venue',
            'props': {
                'id': ['int', 'sequential_id'],
                'original_id': ['string', 'venue_id'],
                'name': ['string', 'name'],
                'date_opened':  ['edtf', 'date_opened'],
                'date_closed':  ['edtf', 'date_closed'],
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
        conn,
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

    await conn.close()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_structure())
    loop.run_until_complete(create_data())
    loop.close()


if __name__ == '__main__':
    main()
