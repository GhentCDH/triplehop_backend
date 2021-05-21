import csv
import json
import os
import re
import typing


def fix_cell(value: str, short_config: typing.Dict):
    if value in ['', 'N/A']:
        return ''
    if short_config is None:
        return value

    if 'nullable' in short_config:
        if value in short_config['nullable']:
            return ''
    if 'venue_date' in short_config:
        m = re.match(r'^([0-9]{3})(?:[?]|X[?])$', value)
        if m:
            return f'{m.group(1)}X'
        if value == '*':
            return '..'
        if value == '1967-1968?':
            return '[1967,1968]'
        if value == '1935/36':
            return '[1935,1936]'
        if value == '1962/68':
            return '[1963..1968]'
    return value


def fix_data(filename: str, config: typing.Dict):
    with open(f'data/original/{filename}') as input_file,\
         open(f'data/processed/{filename}', 'w') as output_file:
        input_reader = csv.reader(input_file)
        output_writer = csv.writer(output_file, lineterminator='\n')

        header = next(input_reader)
        output_writer.writerow(header)

        short_config = []
        for h in header:
            if h in config:
                short_config.append(config[h])
            else:
                short_config.append(None)

        for row in input_reader:
            output_writer.writerow([fix_cell(c, short_config[i]) for (i, c) in enumerate(row)])


configs = {
    'tblAddress.csv': {
        'city_name': {
            'nullable': ['?'],
        },
        'postal_code': {
            'nullable': ['?', 'NA'],
        },
    },
    'tblVenue.csv': {
        'date_opened': {
            'nullable': ['NA?'],
            'venue_date': None,
        },
        'date_closed': {
            'nullable': ['NA?'],
            'venue_date': None,
        },
    }
}


for filename in os.listdir('data/original'):
    if filename in configs:
        fix_data(filename, configs[filename])
    else:
        fix_data(filename, {})


# tblAddress.csv -> tblCity.csv and tblJoinAddressCity.csv
# tblAddress -> save geodata as geojson
city_counter = 1
city_lookup = {}

with open('data/processed/tblAddress.csv') as address_file,\
     open('data/processed/tblCity.csv', 'w') as city_file,\
     open('data/processed/tblJoinAddressCity.csv', 'w') as join_file,\
     open('data/processed/tblAddressWithGeoJson.csv', 'w') as address_out_file:
    address_reader = csv.reader(address_file)
    city_writer = csv.writer(city_file, lineterminator='\n')
    join_writer = csv.writer(join_file, lineterminator='\n')
    address_out_writer = csv.writer(address_out_file, lineterminator='\n')

    address_header = next(address_reader)
    address_header_lookup = {h: address_header.index(h) for h in address_header}
    address_out_writer.writerow(address_header)

    city_writer.writerow(['id', 'name', 'postal_code'])
    join_writer.writerow(['address_id', 'city_id'])

    for row in address_reader:
        if (
            row[address_header_lookup['city_name']] != ''
            or row[address_header_lookup['postal_code']] != ''
        ):
            key = f'{row[address_header_lookup["city_name"]]}_{row[address_header_lookup["postal_code"]]}'

            if key not in city_lookup:
                city_lookup[key] = city_counter
                city_writer.writerow([
                    city_counter,
                    row[address_header_lookup['city_name']],
                    row[address_header_lookup['postal_code']]
                ])
                city_counter += 1

            join_writer.writerow([row[address_header_lookup['address_id']], city_lookup[key]])

        if (row[address_header_lookup['geodata']] != ''):
            coordinates = row[address_header_lookup['geodata']].split(', ')
            row[address_header_lookup['geodata']] = json.dumps({
                'type': 'Point',
                'coordinates': [coordinates[1], coordinates[0]]
            })
        address_out_writer.writerow(row)


# join tlbPerson and tblPersonFirstNames so an update is not needed
with open('data/processed/tblPerson.csv') as p_file,\
     open('data/processed/tblPersonFirstNames.csv') as fn_file,\
     open('data/processed/tblPersonWithFirstNames.csv', 'w') as join_file:
    p_reader = csv.reader(p_file)
    fn_reader = csv.reader(fn_file)
    join_writer = csv.writer(join_file, lineterminator='\n')

    p_header = next(p_reader)
    p_header.append('first_names')
    p_header.append('imdb')
    p_header_lookup = {h: p_header.index(h) for h in p_header}

    fn_header = next(fn_reader)
    fn_header_lookup = {h: fn_header.index(h) for h in fn_header}

    join_writer.writerow(p_header)

    fn_lookup = {}
    for row in fn_reader:
        person_id = row[fn_header_lookup['person_id']]
        if person_id not in fn_lookup:
            fn_lookup[person_id] = []
        fn_lookup[person_id].append(row[fn_header_lookup['first_name']])

    for row in p_reader:
        person_id = row[p_header_lookup['person_id']]
        if person_id in fn_lookup:
            row.append('|'.join(fn_lookup[person_id]))
        else:
            row.append('')
        if row[p_header_lookup['name']] == '':
            row[p_header_lookup['name']] = (
                f'{" / ".join(row[p_header_lookup["first_names"]].split("|"))} '
                f'{row[p_header_lookup["last_name"]]} '
                f'{row[p_header_lookup["suffix"]]}'
            ).replace('  ', ' ').strip()
        # imdb
        row.append('')
        join_writer.writerow(row)


# split dates in tblCompanyNames
with open('data/processed/tblCompanyNames.csv') as input_file,\
     open('data/processed/tblCompanyNamesSplitDates.csv', 'w') as output_file:
    i_reader = csv.reader(input_file)
    o_writer = csv.writer(output_file, lineterminator='\n')

    i_header = next(i_reader)
    i_header_lookup = {h: i_header.index(h) for h in i_header}

    o_header = ['company_id', 'name', 'date_start', 'date_end', 'sequential_id']
    o_writer.writerow(o_header)

    for row in i_reader:
        date = row[i_header_lookup['date']]
        if '/' in date:
            (date_start, date_end) = date.split('/')
        else:
            date_start = date
            date_end = date
        row[i_header_lookup['date']:i_header_lookup['date'] + 1] = date_start, date_end
        o_writer.writerow(row)
