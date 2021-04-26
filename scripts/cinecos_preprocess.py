from typing import Dict

import csv
import os
import re


def fix_cell(value: str, short_config: Dict):
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


def fix_data(filename: str, config: Dict):
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
city_counter = 1
city_lookup = {}

with open('data/processed/tblAddress.csv') as input_file,\
     open('data/processed/tblCity.csv', 'w') as city_file,\
     open('data/processed/tblJoinAddressCity.csv', 'w') as join_file:
    input_reader = csv.reader(input_file)
    city_writer = csv.writer(city_file, lineterminator='\n')
    join_writer = csv.writer(join_file, lineterminator='\n')

    header = next(input_reader)
    header_lookup = {h: header.index(h) for h in header}

    city_writer.writerow(['id', 'name', 'postal_code'])
    join_writer.writerow(['address_id', 'city_id'])

    for row in input_reader:
        if (
            row[header_lookup["city_name"]] == ''
            and row[header_lookup["postal_code"]] == ''
        ):
            continue

        key = f'{row[header_lookup["city_name"]]}_{row[header_lookup["postal_code"]]}'

        if key not in city_lookup:
            city_lookup[key] = city_counter
            city_writer.writerow([city_counter, row[header_lookup['city_name']], row[header_lookup['postal_code']]])
            city_counter += 1

        join_writer.writerow([row[header_lookup['address_id']], city_lookup[key]])
