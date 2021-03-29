import csv

# tblAddress.csv -> tblCity.csv and tblJoinAddressCity.csv
city_counter = 1
city_lookup = {}

with open('data/tblAddress.csv') as input_file,\
     open('data/tblCity.csv', 'w') as city_file,\
     open('data/tblJoinAddressCity.csv', 'w') as join_file:
    input_reader = csv.reader(input_file)
    city_writer = csv.writer(city_file, lineterminator='\n')
    join_writer = csv.writer(join_file, lineterminator='\n')

    header = next(input_reader)
    header_lookup = {h: header.index(h) for h in header}

    city_writer.writerow(['id', 'name', 'postal_code'])
    join_writer.writerow(['address_id', 'city_id'])

    for row in input_reader:
        if (
            row[header_lookup["city_name"]] in ['', '?', 'N/A']
            and row[header_lookup["postal_code"]] in ['', '?', 'N/A']
        ):
            continue

        key = f'{row[header_lookup["city_name"]]}_{row[header_lookup["postal_code"]]}'

        if key not in city_lookup:
            city_lookup[key] = city_counter
            city_writer.writerow([city_counter, row[header_lookup['city_name']], row[header_lookup['postal_code']]])
            city_counter += 1

        join_writer.writerow([row[header_lookup['address_id']], city_lookup[key]])
