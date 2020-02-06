import csv

directors = []
films_directors = []

with open('cinecos_films.csv') as input_file:
    lines = input_file.readlines()
    csv_reader = csv.reader(lines)

    header = next(csv_reader)
    header_lookup = {h: header.index(h) for h in header}

    for row in csv_reader:
        for director_name in row[header_lookup['film_director']].split('|'):
            if director_name == '':
                continue

            try:
                index = directors.index(director_name)
            except ValueError:
                index = len(directors)
                directors.append(director_name)
            films_directors.append([row[header_lookup['film_id']], index + 1])

with open('cinecos_directors.csv', 'w') as directors_file:
    directors_writer = csv.writer(directors_file, lineterminator='\n')
    directors_writer.writerow(['director_id', 'name'])

    for index, director_name in enumerate(directors):
        directors_writer.writerow([index + 1, director_name])

with open('cinecos_films_directors.csv', 'w') as films_directors_file:
    films_directors_writer = csv.writer(films_directors_file, lineterminator='\n')
    films_directors_writer.writerow(['film_id', 'director_id'])

    for film_director in films_directors:
        films_directors_writer.writerow(film_director)
