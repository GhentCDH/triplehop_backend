import asyncio
import asyncpg
import csv
import time
import tqdm
import typer
import typing

import config
import utils


async def batch(method: typing.Callable, data: typing.Iterable, limit: int = None, *args):
    counter = 0
    batch = []
    for row in tqdm.tqdm([r for r in data][:limit]):
        counter += 1
        batch.append(row)
        if not counter % 500:
            await method(*args, batch)
            batch = []
    if len(batch):
        await method(*args, batch)


async def create_structure(structure_actions: typing.List[str]):
    pool = await asyncpg.create_pool(**config.DATABASE)

    if structure_actions is None or 'project_config' in structure_actions:
        await utils.create_project_config(pool, 'cinecos', 'Cinecos', 'info@cinemabelgica.be')

    if structure_actions is None or 'entity_configs' in structure_actions:
        entities_types = {
            'film': 'Film',
            'mentioned_film_title': 'Mentioned Film Title',
            'continent': 'Continent',
            'country': 'Country',
            'city': 'City',
            'address': 'Address',
            'venue': 'Venue',
            'person': 'Person',
            'company': 'Company',
            'company_name': 'Company name',
        }
        for (system_name, display_name) in entities_types.items():
            await utils.create_entity_config(
                pool,
                'cinecos',
                'info@cinemabelgica.be',
                system_name,
                display_name,
                utils.read_config_from_file('cinecos', 'entity', system_name),
            )

    if structure_actions is None or 'relation_configs' in structure_actions:
        relation_types = {
            'mentioned_film_title': ['Mentioned Film Title', ['film'], ['mentioned_film_title']],
            'country_continent': ['Continent', ['country'], ['continent']],
            'film_country': ['Film Country', ['film'], ['country']],
            'address_city': ['City', ['address'], ['city']],
            'venue_address': ['Address', ['venue'], ['address']],
            'film_person': ['Film Person', ['film'], ['person']],
            'venue_person': ['Venue Person', ['venue'], ['person']],
            'company_name': ['Company Name', ['company'], ['company_name']],
            'company_company': ['Subsidiary', ['company'], ['company']],
            'company_person': ['Company Person', ['company'], ['person']],
            'film_company': ['Film Company', ['film'], ['company']],
        }
        for (system_name, (display_name, domains, ranges)) in relation_types.items():
            await utils.create_relation_config(
                pool,
                'cinecos',
                'info@cinemabelgica.be',
                system_name,
                display_name,
                utils.read_config_from_file('cinecos', 'relation', system_name),
                domains,
                ranges,
            )

    if structure_actions is None or 'recreate_graph' in structure_actions:
        await utils.drop_project_graph(pool, 'cinecos')
        await utils.create_project_graph(pool, 'cinecos')

    await pool.close()


async def create_entity(pool: asyncpg.pool.Pool, conf: typing.Dict, limit: int = None):
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

        db_props_lookup = await utils.get_entity_props_lookup(pool, 'cinecos', conf['entity_type_name'])

        await batch(
            utils.create_entities,
            data_reader,
            limit,
            pool,
            params,
            db_props_lookup,
            file_header_lookup,
            conf['props']
        )


async def create_relation(pool: asyncpg.pool.Pool, conf: typing.Dict, limit: int = None):
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

        db_domain_props_lookup = await utils.get_entity_props_lookup(pool, 'cinecos', conf['domain_type_name'])
        db_range_props_lookup = await utils.get_entity_props_lookup(pool, 'cinecos', conf['range_type_name'])
        db_props_lookup = await utils.get_relation_props_lookup(pool, 'cinecos', conf['relation_type_name'])

        await batch(
            utils.create_relations,
            data_reader,
            limit,
            pool,
            params,
            db_domain_props_lookup,
            db_range_props_lookup,
            db_props_lookup,
            file_header_lookup,
            conf['domain'],
            conf['range'],
            conf['props']
        )


async def create_data(data_actions: typing.List[str]):
    pool = await asyncpg.create_pool(**config.DATABASE)

    if data_actions is None or 'entity__film' in data_actions:
        await create_entity(
            pool,
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
            },
        )

    if data_actions is None or 'entity__mentioned_film_title' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblFilmTitleVariation.csv',
                'entity_type_name': 'mentioned_film_title',
                'props': {
                    'id': ['int', 'film_variation_id'],
                    'title': ['string', 'title'],
                },
            },
            1000,
        )

    if data_actions is None or 'entity__person' in data_actions:
        await create_entity(
            pool,
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
            },
        )

    if data_actions is None or 'entity__continent' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblContinent.csv',
                'entity_type_name': 'continent',
                'props': {
                    'id': ['int', 'continent_id'],
                    'original_id': ['string', 'code'],
                    'name': ['string', 'name'],
                },
            },
        )

    if data_actions is None or 'entity__country' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblCountry.csv',
                'entity_type_name': 'country',
                'props': {
                    'id': ['int', 'country_id'],
                    'original_id': ['string', 'code'],
                    'name': ['string', 'name'],
                },
            },
        )

    if data_actions is None or 'entity__city' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblCity.csv',
                'entity_type_name': 'city',
                'props': {
                    'id': ['int', 'id'],
                    'original_id': ['int', 'id'],
                    'name': ['string', 'name'],
                    'postal_code': ['int', 'postal_code'],
                },
            },
        )

    if data_actions is None or 'entity__address' in data_actions:
        await create_entity(
            pool,
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
            },
        )

    if data_actions is None or 'entity__venue' in data_actions:
        await create_entity(
            pool,
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
            },
        )

    if data_actions is None or 'entity__company' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblCompany.csv',
                'entity_type_name': 'company',
                'props': {
                    'id': ['int', 'company_id'],
                    'original_id': ['int', 'company_id'],
                    'name': ['string', 'name'],
                    'date_start':  ['edtf', 'date_extablished'],
                    'date_end':  ['edtf', 'date_disbanded'],
                    'info':  ['string', 'info'],
                    'nature':  ['string', 'nature'],
                },
            },
        )

    if data_actions is None or 'entity__company_name' in data_actions:
        await create_entity(
            pool,
            {
                'filename': 'tblCompanyNamesSplitDates.csv',
                'entity_type_name': 'company_name',
                'props': {
                    'id': ['int', 'sequential_id'],
                    'original_id': ['int', 'sequential_id'],
                    'name': ['string', 'name'],
                },
            },
        )

    if data_actions is None or 'relation__mentioned_film_title' in data_actions:
        await create_relation(
            pool,
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
            },
            1000,
        )

    if data_actions is None or 'relation__country_continent' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblCountry.csv',
                'relation_type_name': 'country_continent',
                'domain_type_name': 'country',
                'range_type_name': 'continent',
                'domain': {
                    'id': ['int', 'country_id'],
                },
                'range': {
                    'original_id': ['string', 'continent_code'],
                },
                'props': {}
            },
            1000,
        )

    if data_actions is None or 'relation__film_country' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblFilm.csv',
                'relation_type_name': 'film_country',
                'domain_type_name': 'film',
                'range_type_name': 'country',
                'domain': {
                    'id': ['int', 'film_id'],
                },
                'range': {
                    'original_id': ['string', 'country'],
                },
                'props': {}
            },
            1000,
        )

    if data_actions is None or 'relation__address_city' in data_actions:
        await create_relation(
            pool,
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
            },
            1000,
        )

    if data_actions is None or 'relation__film_person' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblJoinFilmPerson.csv',
                'relation_type_name': 'film_person',
                'domain_type_name': 'film',
                'range_type_name': 'person',
                'domain': {
                    'id': ['int', 'film_id'],
                },
                'range': {
                    'id': ['int', 'person_id'],
                },
                'props': {
                    'original_id': ['int', 'film_person_id'],
                    'type': ['string', 'info'],
                }
            },
            5000,
        )

    if data_actions is None or 'relation__venue_address' in data_actions:
        await create_relation(
            pool,
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
            },
            1000,
        )

    if data_actions is None or 'relation__venue_person' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblJoinVenuePerson.csv',
                'relation_type_name': 'venue_person',
                'domain_type_name': 'venue',
                'range_type_name': 'person',
                'domain': {
                    'original_id': ['string', 'venue_id'],
                },
                'range': {
                    'id': ['int', 'person_id'],
                },
                'props': {
                    'type': ['string', 'job_type'],
                    'date_start': ['edtf', 'start_date'],
                    'date_end': ['edtf', 'end_date'],
                    'years': ['string', 'years'],
                }
            },
        )

    if data_actions is None or 'relation__mcompany_name' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblCompanyNamesSplitDates.csv',
                'relation_type_name': 'company_name',
                'domain_type_name': 'company',
                'range_type_name': 'company_name',
                'domain': {
                    'id': ['int', 'company_id'],
                },
                'range': {
                    'id': ['int', 'sequential_id'],
                },
                'props': {
                    'date_start': ['edtf', 'date_start'],
                    'date_end': ['edtf', 'date_end'],
                }
            },
        )

    if data_actions is None or 'relation__company_company' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblJoinCompanyCompany.csv',
                'relation_type_name': 'company_company',
                'domain_type_name': 'company',
                'range_type_name': 'company',
                'domain': {
                    'id': ['int', 'company_id'],
                },
                'range': {
                    'id': ['int', 'subsidiary_id'],
                },
                'props': {
                    'subsidiary_type': ['string', 'subsidiary_type'],
                    'date_start': ['edtf', 'start_date'],
                    'date_end': ['edtf', 'end_date'],
                }
            },
        )

    if data_actions is None or 'relation__company_person' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblJoinCompanyPerson.csv',
                'relation_type_name': 'company_person',
                'domain_type_name': 'company',
                'range_type_name': 'person',
                'domain': {
                    'id': ['int', 'company_id'],
                },
                'range': {
                    'id': ['int', 'person_id'],
                },
                'props': {
                    'type': ['string', 'job_type'],
                    'date_start': ['edtf', 'start_date'],
                    'date_end': ['edtf', 'end_date'],
                    'years': ['string', 'years'],
                }
            },
        )

    if data_actions is None or 'relation__film_company' in data_actions:
        await create_relation(
            pool,
            {
                'filename': 'tblJoinFilmCompany.csv',
                'relation_type_name': 'film_company',
                'domain_type_name': 'film',
                'range_type_name': 'company',
                'domain': {
                    'id': ['int', 'film_id'],
                },
                'range': {
                    'id': ['int', 'company_id'],
                },
                'props': {
                    'type': ['string', 'info'],
                }
            },
            1000,
        )

    await pool.close()


def main(
    actions: typing.List[str] = typer.Option(None, help="create_structure or create_data"),
    sub_actions: typing.List[str] = typer.Option(None)
):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    if actions is None or 'create_structure' in actions:
        loop.run_until_complete(create_structure(sub_actions))
    if actions is None or 'create_data' in actions:
        loop.run_until_complete(create_data(sub_actions))
    loop.close()
    print(f'Total time: {time.time() - start_time}')


if __name__ == '__main__':
    typer.run(main)
