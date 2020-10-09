import csv
import psycopg2
from datetime import datetime, timedelta

from config import DATABASE_CONNECTION_STRING

from utils import add_entity, add_relation, batch_process, dtu, update_entity

# venue address hack:
# * add postal_code, city_name, street_name, geodata directly to venue
# * add a relation directly from venue to city

with psycopg2.connect(DATABASE_CONNECTION_STRING) as conn:
    with conn.cursor() as cur:
        cur.execute(
            '''
            SELECT "user".id
            FROM app.user
            WHERE "user".username = %(username)s;
            ''',
            {
                'username': 'info@cinemabelgica.be',
            }
        )
        user_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.project (system_name, display_name, user_id)
                VALUES (
                    'cinecos',
                    'Cinecos',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;
            '''
        )

        cur.execute(
            '''
                SELECT project.id
                FROM app.project
                WHERE project.system_name = %(project)s;
            ''',
            {
                'project': 'cinecos',
            }
        )
        project_id = cur.fetchone()[0]

        cur.execute(
            '''
                INSERT INTO app.entity (project_id, system_name, display_name, config, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'film',
                    'Film',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "title",
                                "display_name": "Title",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "mentioned_titles",
                                "display_name": "Mentioned title(s)",
                                "type": "[String]"
                            },
                            "3": {
                                "system_name": "year",
                                "display_name": "Production year",
                                "type": "Int"
                            },
                            "4": {
                                "system_name": "imdb_id",
                                "display_name": "IMDb ID",
                                "type": "String"
                            },
                            "5": {
                                "system_name": "wikidata_id",
                                "display_name": "Wikidata ID",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1 ($3)",
                            "layout": [
                                {
                                    "label": "General",
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2",
                                            "type": "list"
                                        },
                                        {
                                            "field": "3"
                                        },
                                        {
                                            "field": "4",
                                            "type": "online_identifier",
                                            "base_url": "https://www.imdb.com/title/"
                                        },
                                        {
                                            "field": "5",
                                            "type": "online_identifier",
                                            "base_url": "https://www.wikidata.org/wiki/"
                                        },
                                        {
                                            "field": "5",
                                            "label": "Wikidata images",
                                            "type": "wikidata_images"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "title",
                                "display_name": "Title",
                                "selector_value": "$title",
                                "type": "text"
                            },
                            "1": {
                                "system_name": "mentioned_titles",
                                "display_name": "Mentioned title(s)",
                                "selector_value": "$mentioned_titles",
                                "type": "text"
                            },
                            "2": {
                                "system_name": "year",
                                "display_name": "Production year",
                                "selector_value": "$year",
                                "type": "integer"
                            },
                            "3": {
                                "system_name": "director",
                                "display_name": "Director(s)",
                                "relation": "r_director",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_director->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_director->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            },
                            "4": {
                                "system_name": "distributor",
                                "display_name": "Distributor(s)",
                                "relation": "r_distributor",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_distributor->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_distributor->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            },
                            "5": {
                                "system_name": "production_company",
                                "display_name": "Production company(-ies)",
                                "relation": "r_production_company",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_production_company->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_production_company->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            },
                            "6": {
                                "system_name": "country",
                                "display_name": "Country(-ies)",
                                "relation": "r_film_country",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_film_country->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_film_country->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            }
                        },
                        "es_display": {
                            "title": "Search films",
                            "filters": [
                                {
                                    "filters": [
                                        {
                                            "filter": "0",
                                            "type": "autocomplete"
                                        },
                                        {
                                            "filter": "1",
                                            "type": "autocomplete"
                                        },
                                        {
                                            "filter": "2",
                                            "type": "histogram_slider",
                                            "interval": 10
                                        },
                                        {
                                            "filter": "3"
                                        },
                                        {
                                            "filter": "4"
                                        },
                                        {
                                            "filter": "5"
                                        },
                                        {
                                            "filter": "6"
                                        }
                                    ]
                                }
                            ],
                            "columns": [
                                {
                                    "column": "0",
                                    "sortable": true
                                },
                                {
                                    "column": "1",
                                    "sortable": true
                                },
                                {
                                    "column": "2",
                                    "sortable": true
                                },
                                {
                                    "column": "3",
                                    "sortable": true
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'person',
                    'Person',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "wikidata_id",
                                "display_name": "Wikidata ID",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2",
                                            "type": "online_identifier",
                                            "base_url": "https://www.wikidata.org/wiki/"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "name",
                                "display_name": "Name",
                                "selector_value": "$name",
                                "type": "text"
                            }
                        },
                        "es_display": {
                            "title": "Search persons",
                            "filters": [
                                {
                                    "filters": [
                                        {
                                            "filter": "0",
                                            "type": "autocomplete"
                                        }
                                    ]
                                }
                            ],
                            "columns": [
                                {
                                    "column": "0",
                                    "sortable": true
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'company',
                    'Company',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "String"
                            },
                            "3": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "info",
                                "display_name": "Info",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        },
                                        {
                                            "field": "3"
                                        },
                                        {
                                            "field": "4"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "name",
                                "display_name": "Name",
                                "selector_value": "$name",
                                "type": "text"
                            },
                            "1": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "selector_value": "$date_start",
                                "type": "text"
                            },
                            "2": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "selector_value": "$date_end",
                                "type": "text"
                            }
                        },
                        "es_display": {
                            "title": "Search companies",
                            "filters": [
                                {
                                    "filters": [
                                        {
                                            "filter": "0",
                                            "type": "autocomplete"
                                        }
                                    ]
                                }
                            ],
                            "columns": [
                                {
                                    "column": "0",
                                    "sortable": true
                                },
                                {
                                    "column": "1",
                                    "sortable": true
                                },
                                {
                                    "column": "2",
                                    "sortable": true
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'company_branch',
                    'Company Branch',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'country',
                    'Country',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'continent',
                    'Continent',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue',
                    'Venue',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "date_opened_display",
                                "display_name": "Date opened",
                                "type": "String"
                            },
                            "3": {
                                "system_name": "date_opened",
                                "display_name": "Date opened",
                                "type": "Int"
                            },
                            "4": {
                                "system_name": "date_closed_display",
                                "display_name": "Date closed",
                                "type": "String"
                            },
                            "5": {
                                "system_name": "date_closed",
                                "display_name": "Date opened",
                                "type": "Int"
                            },
                            "6": {
                                "system_name": "status",
                                "display_name": "Status",
                                "type": "String"
                            },
                            "7": {
                                "system_name": "type",
                                "display_name": "Type",
                                "type": "String"
                            },
                            "8": {
                                "system_name": "ideological_characteristic",
                                "display_name": "Ideological characteristic",
                                "type": "String"
                            },
                            "9": {
                                "system_name": "ideological_remark",
                                "display_name": "Ideological remark",
                                "type": "String"
                            },
                            "10": {
                                "system_name": "infrastructure_info",
                                "display_name": "Infrastructure info",
                                "type": "String"
                            },
                            "11": {
                                "system_name": "name_remarks",
                                "display_name": "Remarks about the name",
                                "type": "String"
                            },
                            "12": {
                                "system_name": "screens",
                                "display_name": "Number of screens",
                                "type": "[String]"
                            },
                            "13": {
                                "system_name": "seats",
                                "display_name": "Number of seats",
                                "type": "[String]"
                            },
                            "14": {
                                "system_name": "postal_code",
                                "display_name": "Postal code",
                                "type": "Int"
                            },
                            "15": {
                                "system_name": "city_name",
                                "display_name": "City",
                                "type": "String"
                            },
                            "16": {
                                "system_name": "street_name",
                                "display_name": "Street",
                                "type": "String"
                            },
                            "17": {
                                "system_name": "location",
                                "display_name": "Location",
                                "type": "Geometry"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        },
                                        {
                                            "field": "4"
                                        },
                                        {
                                            "field": "6"
                                        },
                                        {
                                            "field": "7"
                                        },
                                        {
                                            "field": "8"
                                        },
                                        {
                                            "field": "9"
                                        },
                                        {
                                            "field": "10"
                                        },
                                        {
                                            "field": "11"
                                        },
                                        {
                                            "field": "12"
                                        },
                                        {
                                            "field": "13"
                                        },
                                        {
                                            "field": "14"
                                        },
                                        {
                                            "field": "15"
                                        },
                                        {
                                            "field": "16"
                                        },
                                        {
                                            "field": "17",
                                            "type": "geometry"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "name",
                                "display_name": "Name",
                                "selector_value": "$name",
                                "type": "text"
                            },
                            "1": {
                                "system_name": "city",
                                "display_name": "City",
                                "relation": "r_venue_city",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_venue_city->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_venue_city->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            },
                            "2": {
                                "system_name": "street_name",
                                "display_name": "Street name",
                                "selector_value": "$street_name",
                                "type": "text"
                            },
                            "3": {
                                "system_name": "date_opened",
                                "display_name": "Date opened",
                                "selector_value": "$date_opened_display",
                                "type": "text"
                            },
                            "4": {
                                "system_name": "date_closed",
                                "display_name": "Date closed",
                                "selector_value": "$date_closed_display",
                                "type": "text"
                            },
                            "5": {
                                "system_name": "city_name",
                                "display_name": "City",
                                "selector_value": "$city_name",
                                "type": "text"
                            }
                        },
                        "es_display": {
                            "title": "Search venues",
                            "filters": [
                                {
                                    "filters": [
                                        {
                                            "filter": "0",
                                            "type": "autocomplete"
                                        },
                                        {
                                            "filter": "1"
                                        },
                                        {
                                            "filter": "2",
                                            "type": "autocomplete"
                                        }
                                    ]
                                }
                            ],
                            "columns": [
                                {
                                    "column": "0",
                                    "sortable": true
                                },
                                {
                                    "column": "5",
                                    "sortable": true
                                },
                                {
                                    "column": "2",
                                    "sortable": true
                                },
                                {
                                    "column": "3",
                                    "sortable": true
                                },
                                {
                                    "column": "4",
                                    "sortable": true
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'address',
                    'Address',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "street_name",
                                "display_name": "Street name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "location",
                                "display_name": "Location",
                                "type": "Geometry"
                            },
                            "3": {
                                "system_name": "district",
                                "display_name": "District",
                                "type": "String"
                            },
                            "4": {
                                "system_name": "architectural_info",
                                "display_name": "Architectural information",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2",
                                            "type": "geometry"
                                        },
                                        {
                                            "field": "3"
                                        },
                                        {
                                            "field": "4"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'city',
                    'City',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "name",
                                "display_name": "Name",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "postal_code",
                                "display_name": "Postal code",
                                "type": "Int"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'programme',
                    'Programme',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "String"
                            },
                            "3": {
                                "system_name": "dates_mentioned",
                                "display_name": "Date(s) mentioned",
                                "type": "[String]"
                            },
                            "4": {
                                "system_name": "vooruit_image",
                                "display_name": "Announcement in \\"Vooruit\\"",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "Programme ($1 - $2)",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        },
                                        {
                                            "field": "3",
                                            "type": "list"
                                        },
                                        {
                                            "field": "4",
                                            "type": "vooruit_image"
                                        }
                                    ]
                                }
                            ]
                        },
                        "es_data": {
                            "0": {
                                "system_name": "id",
                                "display_name": "Id",
                                "selector_value": "$original_id",
                                "type": "integer"
                            },
                            "1": {
                                "system_name": "venue",
                                "display_name": "Venue",
                                "selector_value": "$venue",
                                "relation": "r_programme_venue",
                                "parts": {
                                    "id": {
                                        "selector_value": "$r_programme_venue->$id",
                                        "type": "integer"
                                    },
                                    "name": {
                                        "selector_value": "$r_programme_venue->$name",
                                        "type": "text"
                                    }
                                },
                                "type": "nested"
                            },
                            "2": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "selector_value": "$date_start",
                                "type": "text"
                            },
                            "3": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "selector_value": "$date_end",
                                "type": "text"
                            }
                        },
                        "es_display": {
                            "title": "Search programmes",
                            "filters": [
                                {
                                    "filters": [
                                        {
                                            "filter": "1"
                                        }
                                    ]
                                }
                            ],
                            "columns": [
                                {
                                    "column": "0",
                                    "sortable": true
                                },
                                {
                                    "column": "1",
                                    "sortable": true
                                },
                                {
                                    "column": "2",
                                    "sortable": true
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'programme_item',
                    'Programme item',
                    '{
                        "data": {
                            "0": {
                                "system_name": "original_id",
                                "display_name": "Original id",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "mentioned_film",
                                "display_name": "Mentioned film title",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "mentioned_venue",
                                "display_name": "Mentioned venue name",
                                "type": "String"
                            }
                        },
                        "display": {
                            "title": "$1",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT (project_id, system_name) DO UPDATE
                SET config = EXCLUDED.config;

                INSERT INTO app.entity_count (id)
                SELECT entity.id from app.entity
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation (project_id, system_name, display_name, config, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'director',
                    'Director',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Directed by",
                            "range_title": "Directed",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'actor',
                    'Actor',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Cast",
                            "range_title": "Filmography",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'distributor',
                    'Distributor',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Distributor",
                            "range_title": "Distributor of",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'production_company',
                    'Production company',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Production company",
                            "range_title": "Production company of",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'company_person',
                    'Company person',
                    '{
                        "data": {
                            "0": {
                                "system_name": "job_type",
                                "display_name": "Job type",
                                "type": "String"
                            },
                            "1": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "Int"
                            },
                            "2": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "Int"
                            }
                        },
                        "display": {
                            "domain_title": "People",
                            "range_title": "Companies",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "0"
                                        },
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue_company',
                    'Venue company',
                    '{
                        "data": {
                            "0": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "Int"
                            }
                        },
                        "display": {
                            "domain_title": "Companies",
                            "range_title": "Venues",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "0"
                                        },
                                        {
                                            "field": "1"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue_person',
                    'Venue person',
                    '{
                        "data": {
                            "0": {
                                "system_name": "job_type",
                                "display_name": "Job type",
                                "type": "String"
                            },
                            "1": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "Int"
                            },
                            "2": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "Int"
                            }
                        },
                        "display": {
                            "domain_title": "People",
                            "range_title": "Venues",
                            "layout": [
                                {
                                    "fields": [
                                        {
                                            "field": "0"
                                        },
                                        {
                                            "field": "1"
                                        },
                                        {
                                            "field": "2"
                                        }
                                    ]
                                }
                            ]
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'subsidiary',
                    'Subsidiary',
                    '{
                        "data": {
                            "0": {
                                "system_name": "subsidiary_type",
                                "display_name": "Subsidiary type",
                                "type": "Int"
                            },
                            "1": {
                                "system_name": "date_start",
                                "display_name": "Start date",
                                "type": "String"
                            },
                            "2": {
                                "system_name": "date_end",
                                "display_name": "End date",
                                "type": "String"
                            }
                        },
                        "display": {
                            "domain_title": "Subsidiary",
                            "range_title": "Subsidiary of",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'branch',
                    'Branch',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Branch",
                            "range_title": "Company",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'country_continent',
                    'Continent',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Continent",
                            "range_title": "Country",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'film_country',
                    'Country',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Country",
                            "range_title": "Film",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'address_city',
                    'City',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "City",
                            "range_title": "Address",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue_address',
                    'Address',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Address",
                            "range_title": "Venue",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'venue_city',
                    'City',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "",
                            "range_title": "",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'programme_venue',
                    'Venue',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Venue",
                            "range_title": "Programme",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'programme_item',
                    'Programme item',
                    '{
                        "data": {
                            "0": {
                                "system_name": "order",
                                "display_name": "Order",
                                "type": "Int"
                            }
                        },
                        "display": {
                            "domain_title": "Programme item",
                            "range_title": "Programme",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT project.id FROM app.project WHERE system_name = 'cinecos'),
                    'programme_item_film',
                    'Film',
                    '{
                        "data": {},
                        "display": {
                            "domain_title": "Film",
                            "range_title": "Programme item",
                            "layout": []
                        }
                    }',
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT (project_id, system_name) DO UPDATE
                SET config = EXCLUDED.config;

                INSERT INTO app.relation_domain (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT id FROM app.relation WHERE system_name = 'director'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'actor'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'distributor'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'production_company'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'company_person'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_company'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_person'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'subsidiary'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'branch'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'country_continent'),
                    (SELECT id FROM app.entity WHERE system_name = 'country'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'film_country'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'address_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'address'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_address'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_venue'),
                    (SELECT id FROM app.entity WHERE system_name = 'programme'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_item'),
                    (SELECT id FROM app.entity WHERE system_name = 'programme'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_item_film'),
                    (SELECT id FROM app.entity WHERE system_name = 'programme_item'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_range (relation_id, entity_id, user_id)
                VALUES (
                    (SELECT id FROM app.relation WHERE system_name = 'director'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'actor'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'distributor'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'production_company'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'company_person'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_company'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_person'),
                    (SELECT id FROM app.entity WHERE system_name = 'person'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'subsidiary'),
                    (SELECT id FROM app.entity WHERE system_name = 'company'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'branch'),
                    (SELECT id FROM app.entity WHERE system_name = 'company_branch'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'country_continent'),
                    (SELECT id FROM app.entity WHERE system_name = 'continent'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'film_country'),
                    (SELECT id FROM app.entity WHERE system_name = 'country'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'address_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'city'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_address'),
                    (SELECT id FROM app.entity WHERE system_name = 'address'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'venue_city'),
                    (SELECT id FROM app.entity WHERE system_name = 'city'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_venue'),
                    (SELECT id FROM app.entity WHERE system_name = 'venue'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_item'),
                    (SELECT id FROM app.entity WHERE system_name = 'programme_item'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                ),
                (
                    (SELECT id FROM app.relation WHERE system_name = 'programme_item_film'),
                    (SELECT id FROM app.entity WHERE system_name = 'film'),
                    (SELECT "user".id FROM app.user WHERE "user".username = 'info@cinemabelgica.be')
                )
                ON CONFLICT DO NOTHING;

                INSERT INTO app.relation_count (id)
                SELECT relation.id FROM app.relation
                ON CONFLICT DO NOTHING;
            '''
        )

        types = {}
        for type_name in [
            'film',
            'person',
            'company',
            'company_branch',
            'country',
            'continent',
            'venue',
            'address',
            'city',
            'programme',
            'programme_item',
        ]:
            cur.execute(
                '''
                    SELECT
                        entity.id,
                        entity.config
                    FROM app.entity
                    WHERE entity.system_name = %(entity_type_name)s;
                ''',
                {
                    'entity_type_name': type_name,
                }
            )
            (id, conf) = list(cur.fetchone())
            types[type_name] = {
                'id': id,
                'cl': {conf['data'][k]['system_name']: int(k) for k in conf['data'].keys()},
            }

        relations = {}
        for relation_name in [
            'director',
            'actor',
            'distributor',
            'production_company',
            'company_person',
            'venue_company',
            'venue_person',
            'subsidiary',
            'country_continent',
            'film_country',
            'branch',
            'address_city',
            'venue_address',
            'venue_city',
            'programme_venue',
            'programme_item',
            'programme_item_film',
        ]:
            cur.execute(
                '''
                    SELECT
                        relation.id,
                        relation.config
                    FROM app.relation
                    WHERE relation.system_name = %(relation_type_name)s;
                ''',
                {
                    'relation_type_name': relation_name,
                }
            )
            (id, conf) = list(cur.fetchone())
            relations[relation_name] = {
                'id': id,
                'cl': {conf['data'][k]['system_name']: int(k) for k in conf['data'].keys()},
            }

        cur.execute(
            '''
                DROP GRAPH IF EXISTS g_{project_id} CASCADE;
                CREATE GRAPH g_{project_id};
            '''.format(
                project_id=dtu(project_id),
            )
        )

        cur.execute(
            '''
                SET graph_path = g_{project_id};
            '''.format(
                project_id=dtu(project_id),
            )
        )

        for id in [v['id'] for v in types.values()]:
            cur.execute(
                '''
                    CREATE VLABEL v_{id};
                    CREATE PROPERTY INDEX ON v_{id} ( id );
                    CREATE PROPERTY INDEX ON v_{id} ( p_{id}_0 );
                '''.format(
                    id=dtu(id),
                )
            )

        for id in [r['id'] for r in relations.values()]:
            cur.execute(
                '''
                    CREATE ELABEL e_{id};
                '''.format(
                    id=dtu(id),
                )
            )

        with open('data/tblFilm.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['film_id'], 'int'],
                'original_id': [types['film']['cl']['original_id'], file_lookup['film_id'], 'int'],
                'title': [types['film']['cl']['title'], file_lookup['title']],
                'year': [types['film']['cl']['year'], file_lookup['film_year'], 'int'],
                'imdb_id': [types['film']['cl']['imdb_id'], file_lookup['imdb']],
                'wikidata_id': [types['film']['cl']['wikidata_id'], file_lookup['wikidata']],
            }

            params = {
                'entity_type_id': types['film']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing films')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                add_entity,
                prop_conf,
            )

        with open('data/tblFilmTitleVariation.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['film_id'], 'int'],
                'mentioned_titles': [types['film']['cl']['mentioned_titles'], file_lookup['title'], 'array'],
            }

            params = {
                'entity_type_id': types['film']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing film title variations')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                update_entity,
                prop_conf
            )

        with open('data/tblPerson.csv') as input_file, \
             open('data/tblPersonFirstNames.csv') as fn_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            fn_lines = fn_file.readlines()
            fn_reader = csv.reader(fn_lines)

            fn_header = next(fn_reader)
            fn_lookup = {h: fn_header.index(h) for h in fn_header}

            fn_index = {}
            for row in fn_reader:
                person_id = row[fn_lookup['person_id']]
                if person_id not in fn_index:
                    fn_index[person_id] = []
                fn_index[person_id].append(row[fn_lookup['first_name']])

            persons = []
            for row in csv_reader:
                if row[file_lookup['name']] == '':
                    if row[file_lookup['person_id']] in fn_index:
                        first_name = ' / '.join(fn_index[row[file_lookup['person_id']]])
                    else:
                        first_name = ''
                    last_name = row[file_lookup['last_name']]
                    suffix = row[file_lookup['suffix']]
                    row[file_lookup['name']] = f'{first_name} {last_name} {suffix}'.replace('  ', ' ').strip()
                persons.append(row)

            prop_conf = {
                'id': [None, file_lookup['person_id'], 'int'],
                'original_id': [types['person']['cl']['original_id'], file_lookup['person_id'], 'int'],
                'name': [types['person']['cl']['name'], file_lookup['name']],
                'wikidata_id': [types['person']['cl']['wikidata_id'], file_lookup['wikidata']],
            }

            params = {
                'entity_type_id': types['person']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing persons')
            batch_process(
                cur,
                [r for r in persons],
                params,
                add_entity,
                prop_conf
            )

        with open('data/tblJoinFilmPerson.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            film_persons = [r for r in csv_reader]

            relation_config = [
                [file_lookup['film_id'], 'int'],
                [file_lookup['person_id'], 'int'],
            ]

            prop_conf = {}

            # import director relations
            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['person']['id'],
                'range_prop': f'p_{dtu(types["person"]["id"])}_{types["person"]["cl"]["original_id"]}',
                'relation_type_id': relations['director']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing director relations')
            batch_process(
                cur,
                [r for r in film_persons if r[file_lookup['info']] == 'director'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

            # import actor relations
            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['person']['id'],
                'range_prop': f'p_{dtu(types["person"]["id"])}_{types["person"]["cl"]["original_id"]}',
                'relation_type_id': relations['actor']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing actor relations')
            batch_process(
                cur,
                [r for r in film_persons if r[file_lookup['info']] == 'actor'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblCompany.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['company_id'], 'int'],
                'original_id': [types['company']['cl']['original_id'], file_lookup['company_id'], 'int'],
                'name': [types['company']['cl']['name'], file_lookup['name']],
                'date_start': [types['company']['cl']['date_start'], file_lookup['date_extablished']],
                'date_end': [types['company']['cl']['date_end'], file_lookup['date_disbanded']],
                'info': [types['company']['cl']['info'], file_lookup['info']],
            }

            params = {
                'entity_type_id': types['company']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing companies')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                add_entity,
                prop_conf
            )

        with open('data/tblJoinFilmCompany.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            film_companies = [r for r in csv_reader]

            relation_config = [
                [file_lookup['film_id'], 'int'],
                [file_lookup['company_id'], 'int'],
            ]

            prop_conf = {}

            # import distributor relations
            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['company']['id'],
                'range_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'relation_type_id': relations['distributor']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing distributor relations')
            batch_process(
                cur,
                [r for r in film_companies if r[file_lookup['info']] == 'distributor'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

            # import production company relations
            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['company']['id'],
                'range_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'relation_type_id': relations['production_company']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing production company relations')
            batch_process(
                cur,
                [r for r in film_companies if r[file_lookup['info']] == 'production_company'],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblJoinCompanyPerson.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            company_persons = [r for r in csv_reader]

            relation_config = [
                [file_lookup['company_id'], 'int'],
                [file_lookup['person_id'], 'int'],
            ]

            prop_conf = {
                'job_type': [relations['company_person']['cl']['job_type'], file_lookup['job_type']],
                'date_start': [relations['company_person']['cl']['date_start'], file_lookup['start_date']],
                'date_end': [relations['company_person']['cl']['date_end'], file_lookup['end_date']],
            }

            # import company_person relations
            params = {
                'domain_type_id': types['company']['id'],
                'domain_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'range_type_id': types['person']['id'],
                'range_prop': f'p_{dtu(types["person"]["id"])}_{types["person"]["cl"]["original_id"]}',
                'relation_type_id': relations['company_person']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing company person relations')
            batch_process(
                cur,
                company_persons,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblCompanyBranch.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('branch_id')
            file_lookup = {h: header.index(h) for h in header}

            branches_header = ['branch_id', 'branch_name']
            branches_lookup = {h: branches_header.index(h) for h in branches_header}
            branches = [
                ['1', 'Film distribution'],
            ]

            company_branches = []
            for row in csv_reader:
                if row[file_lookup['branch_name']] == 'film distribution':
                    row.append('1')
                else:
                    print('unknown branch type')
                company_branches.append(row)

            # Import company branches
            prop_conf = {
                'id': [None, branches_lookup['branch_id'], 'int'],
                'original_id': [types['company_branch']['cl']['original_id'], branches_lookup['branch_id'], 'int'],
                'name': [types['company_branch']['cl']['name'], branches_lookup['branch_name']],
            }

            params = {
                'entity_type_id': types['company_branch']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing company branches')
            batch_process(
                cur,
                branches,
                params,
                add_entity,
                prop_conf
            )

            # import company branch relations
            relation_config = [
                [file_lookup['company_id'], 'int'],
                [file_lookup['branch_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['company']['id'],
                'domain_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'range_type_id': types['company_branch']['id'],
                'range_prop': f'p_{dtu(types["company_branch"]["id"])}_{types["company_branch"]["cl"]["original_id"]}',
                'relation_type_id': relations['branch']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing company branch relations')
            batch_process(
                cur,
                company_branches,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblJoinCompanyCompany.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            relation_config = [
                [file_lookup['company_id'], 'int'],
                [file_lookup['subsidiary_id'], 'int'],
            ]

            prop_conf = {
                'subsidiary_type': [relations['subsidiary']['cl']['subsidiary_type'], file_lookup['subsidiary_type']],
                'date_start': [relations['subsidiary']['cl']['date_start'], file_lookup['start_date']],
                'date_end': [relations['subsidiary']['cl']['date_end'], file_lookup['end_date']],
            }

            params = {
                'domain_type_id': types['company']['id'],
                'domain_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'range_type_id': types['company']['id'],
                'range_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'relation_type_id': relations['subsidiary']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing subsidiary relations')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblContinent.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            prop_conf = {
                'id': [None, file_lookup['continent_id'], 'int'],
                'original_id': [types['continent']['cl']['original_id'], file_lookup['code']],
                'name': [types['continent']['cl']['name'], file_lookup['name']],
            }

            params = {
                'entity_type_id': types['continent']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing continents')
            batch_process(
                cur,
                [r for r in csv_reader],
                params,
                add_entity,
                prop_conf,
            )

        with open('data/tblCountry.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            countries = [r for r in csv_reader]

            prop_conf = {
                'id': [None, file_lookup['country_id'], 'int'],
                'original_id': [types['country']['cl']['original_id'], file_lookup['code']],
                'name': [types['country']['cl']['name'], file_lookup['name']],
            }

            params = {
                'entity_type_id': types['country']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing countries')
            batch_process(
                cur,
                countries,
                params,
                add_entity,
                prop_conf,
            )

            # import relation between countries and continents
            relation_config = [
                [file_lookup['code']],
                [file_lookup['continent_code']],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['country']['id'],
                'domain_prop': f'p_{dtu(types["country"]["id"])}_{types["country"]["cl"]["original_id"]}',
                'range_type_id': types['continent']['id'],
                'range_prop': f'p_{dtu(types["continent"]["id"])}_{types["continent"]["cl"]["original_id"]}',
                'relation_type_id': relations['country_continent']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing country continent relations')
            batch_process(
                cur,
                [c for c in countries if c[file_lookup['continent_code']] != ''],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblFilm.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            fc_header = ['film_id', 'country_code']
            fc_lookup = {h: fc_header.index(h) for h in fc_header}
            film_countries = []
            for row in csv_reader:
                for country_code in row[file_lookup['country']].split('|'):
                    film_countries.append([row[file_lookup['film_id']], country_code])

            relation_config = [
                [fc_lookup['film_id'], 'int'],
                [fc_lookup['country_code']],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['film']['id'],
                'domain_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'range_type_id': types['country']['id'],
                'range_prop': f'p_{dtu(types["country"]["id"])}_{types["country"]["cl"]["original_id"]}',
                'relation_type_id': relations['film_country']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing film country relations')
            batch_process(
                cur,
                film_countries,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblAddress.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('city_id')
            header.append('long')
            header.append('lat')
            file_lookup = {h: header.index(h) for h in header}

            # extract cities from addresses
            city_counter = 1
            city_lookup = {}
            cities = []

            addresses = []
            # TODO: remove venue address hack
            address_lookup = {}
            address_header_lookup = {h: header.index(h) for h in header}
            # /hack
            for row in csv_reader:
                # clean n/a
                for col in ['city_name', 'street_name', 'geodata', 'postal_code', 'info']:
                    if row[file_lookup[col]] in ['N/A', '?']:
                        row[file_lookup[col]] = ''

                # cities
                city_key = f'{row[file_lookup["city_name"]]}_{row[file_lookup["postal_code"]]}'
                if city_key == '_':
                    row.append('')
                else:
                    if city_key not in city_lookup:
                        cities.append([city_counter, row[file_lookup["city_name"]], row[file_lookup["postal_code"]]])
                        city_lookup[city_key] = city_counter
                        city_counter += 1
                    row.append(city_lookup[city_key])

                # long, lat
                if row[file_lookup['geodata']] != '':
                    split = row[file_lookup['geodata']].split(',')
                    if len(split) != 2:
                        print(row)
                    row.append(split[1])
                    row.append(split[0])
                else:
                    row.append('')
                    row.append('')
                addresses.append(row)
                # TODO: remove venue address hack
                address_lookup[row[0]] = row
                # /hack

            # import cities
            prop_conf = {
                'id': [None, 0, 'int'],
                'original_id': [types['city']['cl']['original_id'], 0, 'int'],
                'name': [types['city']['cl']['name'], 1],
                'postal_code': [types['city']['cl']['postal_code'], 2, 'int'],
            }

            params = {
                'entity_type_id': types['city']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing cities')
            batch_process(
                cur,
                cities,
                params,
                add_entity,
                prop_conf
            )

            # import addresses
            prop_conf = {
                'id': [None, file_lookup['sequential_id'], 'int'],
                'original_id': [types['address']['cl']['original_id'], file_lookup['address_id']],
                'street_name': [types['address']['cl']['street_name'], file_lookup['street_name']],
                'location': [types['address']['cl']['location'], [file_lookup['long'], file_lookup['lat']], 'point'],
                'district': [types['address']['cl']['district'], file_lookup['info']],
                'architectural_info': [types['address']['cl']['architectural_info'], file_lookup['architectural_info']],
            }

            params = {
                'entity_type_id': types['address']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing addresses')
            batch_process(
                cur,
                addresses,
                params,
                add_entity,
                prop_conf
            )

            # import relation between addresses and cities
            relation_config = [
                [file_lookup['address_id']],
                [file_lookup['city_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['address']['id'],
                'domain_prop': f'p_{dtu(types["address"]["id"])}_{types["address"]["cl"]["original_id"]}',
                'range_type_id': types['city']['id'],
                'range_prop': f'p_{dtu(types["city"]["id"])}_{types["city"]["cl"]["original_id"]}',
                'relation_type_id': relations['address_city']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing address city relations')
            batch_process(
                cur,
                [a for a in addresses if a[file_lookup['city_id']] != ''],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblVenue.csv') as input_file, \
             open('data/tblVenueScreen.csv') as screen_file, \
             open('data/tblVenueSeats.csv') as seat_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('date_opened_system')
            header.append('date_closed_system')
            header.append('screens')
            header.append('seats')
            # TODO: remove venue address hack
            header.append('postal_code')
            header.append('city_name')
            header.append('street_name')
            header.append('long')
            header.append('lat')
            header.append('city_id')
            # /hack
            file_lookup = {h: header.index(h) for h in header}

            screen_reader = csv.reader(screen_file)
            screen_lookup = {h: i for i, h in enumerate(next(screen_reader))}

            screens = {}
            for row in screen_reader:
                venue_id = row[screen_lookup['venue_id']]
                if venue_id not in screens:
                    screens[venue_id] = []
                screens[venue_id].append(f'{row[screen_lookup["number_of_screens"]]} ({row[screen_lookup["years"]]})')

            seat_reader = csv.reader(seat_file)
            seat_lookup = {h: i for i, h in enumerate(next(seat_reader))}

            seats = {}
            for row in seat_reader:
                venue_id = row[seat_lookup['venue_id']]
                if venue_id not in seats:
                    seats[venue_id] = []
                seats[venue_id].append(f'{row[seat_lookup["number_of_seats"]]} ({row[seat_lookup["years"]]})')

            # Process dates: process question marks, X, asterisks and N/A
            venues = []
            for row in csv_reader:
                # Clean up N/A and X
                for col in ['date_opened', 'date_closed']:
                    val = row[file_lookup[col]]
                    if val in ['', 'N/A', 'NA?']:
                        row[file_lookup[col]] = ''
                    elif len(val) == 4 and val[:3].isnumeric() and val[3:] == '?':
                        row[file_lookup[col]] = f'{val[:3]}X'
                    elif len(val) == 5 and val[:3].isnumeric() and val[3:] == 'X?':
                        row[file_lookup[col]] = f'{val[:3]}X'
                    elif val == '1967-1968?':
                        row[file_lookup[col]] = '[1967,1968]'
                    elif val == '1935/36':
                        row[file_lookup[col]] = '[1935,1936]'
                    elif val == '1962/68':
                        row[file_lookup[col]] = '[1963..1968]'

                for col in ['date_opened', 'date_closed']:
                    val = row[file_lookup[col]]
                    if val == '':
                        row.append('')
                    elif val == '*':
                        row.append('..')
                    elif val.isnumeric():
                        row.append(val)
                    elif len(val) == 4 and val[:3].isnumeric() and val[3:] == 'X':
                        # Make interval as wide as possible
                        if col == 'date_opened':
                            row.append(f'{val[:3]}0')
                        else:
                            row.append(f'{val[:3]}9')
                    elif len(val) == 5 and val[:4].isnumeric() and val[4:] == '?':
                        row.append(val[:4])
                    elif val[0] == '[' and val[-1] == ']':
                        # Make interval as wide as possible
                        if col == 'date_opened':
                            row.append(val[1:-1].replace('..', ',').split(',')[0])
                        else:
                            row.append(val[1:-1].replace('..', ',').split(',')[-1])
                    else:
                        print('incorrect date')
                        print(row)
                        print(col)
                        print(val)

                # create an interval if only opening or closing year is known
                for col in ['date_opened', 'date_closed']:
                    other_col = 'date_opened' if col == 'date_closed' else 'date_opened'
                    if row[file_lookup[col]] == '' and row[file_lookup[other_col]] != '':
                        val = row[file_lookup[other_col]]
                        if val.isnumeric():
                            row[file_lookup[f'{col}_system']] = val
                        elif len(val) == 4 and val[:3].isnumeric() and val[3:] == 'X':
                            if col == 'date_opened':
                                row[file_lookup[f'{col}_system']] = f'{val[:3]}0'
                            else:
                                row[file_lookup[f'{col}_system']] = f'{val[:3]}9'
                        elif len(val) == 5 and val[:4].isnumeric() and val[4:] == '?':
                            row[file_lookup[f'{col}_system']] = val[:4]
                        else:
                            print('incorrect date when creating interval')
                            print(row)
                            print(col)
                            print(val)

                venue_id = row[file_lookup['venue_id']]
                if venue_id in screens:
                    row.append(screens[venue_id])
                else:
                    row.append([])
                if venue_id in seats:
                    row.append(seats[venue_id])
                else:
                    row.append([])

                # TODO: remove venue address hack
                address = address_lookup[row[file_lookup['address_id']]]
                row.append(address[address_header_lookup['postal_code']])
                row.append(address[address_header_lookup['city_name']])
                row.append(address[address_header_lookup['street_name']])
                row.append(address[address_header_lookup['long']])
                row.append(address[address_header_lookup['lat']])
                row.append(address[address_header_lookup['city_id']])
                # /hack

                venues.append(row)

            # import venues
            prop_conf = {
                'id': [None, file_lookup['sequential_id'], 'int'],
                'original_id': [types['venue']['cl']['original_id'], file_lookup['venue_id']],
                'name': [types['venue']['cl']['name'], file_lookup['name']],
                'date_opened_display': [types['venue']['cl']['date_opened_display'], file_lookup['date_opened']],
                'date_opened': [types['venue']['cl']['date_opened'], file_lookup['date_opened_system']],
                'date_closed_display': [types['venue']['cl']['date_closed_display'], file_lookup['date_closed']],
                'date_closed': [types['venue']['cl']['date_closed'], file_lookup['date_closed_system']],
                'status': [types['venue']['cl']['status'], file_lookup['status']],
                'type': [types['venue']['cl']['type'], file_lookup['type']],
                'ideological_characteristic': [types['venue']['cl']['ideological_characteristic'], file_lookup['ideological_characteristic']],
                'ideological_remark': [types['venue']['cl']['ideological_remark'], file_lookup['ideological_remark']],
                'infrastructure_info': [types['venue']['cl']['infrastructure_info'], file_lookup['infrastructure_info']],
                'name_remarks': [types['venue']['cl']['name_remarks'], file_lookup['name_remarks']],
                # TODO: remove venue address hack
                'postal_code': [types['venue']['cl']['postal_code'], file_lookup['postal_code']],
                'city_name': [types['venue']['cl']['city_name'], file_lookup['city_name']],
                'street_name': [types['venue']['cl']['street_name'], file_lookup['street_name']],
                'location': [types['venue']['cl']['location'], [file_lookup['long'], file_lookup['lat']], 'point'],
                # /hack
            }

            params = {
                'entity_type_id': types['venue']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venues')
            batch_process(
                cur,
                venues,
                params,
                add_entity,
                prop_conf
            )

            venue_screens_and_seats = []
            for venue in venues:
                if venue[file_lookup['screens']]:
                    for screen in venue[file_lookup['screens']]:
                        venue_screens_and_seats.append([venue[file_lookup['sequential_id']], screen, ''])
                    for seat in venue[file_lookup['seats']]:
                        venue_screens_and_seats.append([venue[file_lookup['sequential_id']], '', seat])

            # import venue screens and seats (arrays) using update
            prop_conf = {
                'id': [None, 0, 'int'],
                'screens': [types['venue']['cl']['screens'], 1, 'array'],
                'seats': [types['venue']['cl']['seats'], 2, 'array'],
            }

            params = {
                'entity_type_id': types['venue']['id'],
                'user_id': user_id,
            }

            print('Cinecos venue screens and seats')
            batch_process(
                cur,
                venue_screens_and_seats,
                params,
                update_entity,
                prop_conf,
            )

            # import relation between venues and addresses
            relation_config = [
                [file_lookup['venue_id']],
                [file_lookup['address_id']],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['venue']['id'],
                'domain_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'range_type_id': types['address']['id'],
                'range_prop': f'p_{dtu(types["address"]["id"])}_{types["address"]["cl"]["original_id"]}',
                'relation_type_id': relations['venue_address']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venue address relations')
            batch_process(
                cur,
                [v for v in venues if v[file_lookup['address_id']] != ''],
                params,
                add_relation,
                relation_config,
                prop_conf
            )

            # TODO: remove venue address hack
            # import relation between venues and cities
            relation_config = [
                [file_lookup['venue_id']],
                [file_lookup['city_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['venue']['id'],
                'domain_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'range_type_id': types['city']['id'],
                'range_prop': f'p_{dtu(types["city"]["id"])}_{types["city"]["cl"]["original_id"]}',
                'relation_type_id': relations['venue_city']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venue cities relations')
            batch_process(
                cur,
                [v for v in venues if address_lookup[v[file_lookup['address_id']]][address_header_lookup['city_id']] != ''],
                params,
                add_relation,
                relation_config,
                prop_conf
            )
            # /hack

        with open('data/tblJoinVenueCompany.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            venue_companies = [r for r in csv_reader]

            relation_config = [
                [file_lookup['venue_id']],
                [file_lookup['company_id'], 'int'],
            ]

            prop_conf = {
                'date_start': [relations['venue_company']['cl']['date_start'], file_lookup['start_date']],
                'date_end': [relations['venue_company']['cl']['date_end'], file_lookup['end_date']],
            }

            # import company_person relations
            params = {
                'domain_type_id': types['venue']['id'],
                'domain_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'range_type_id': types['company']['id'],
                'range_prop': f'p_{dtu(types["company"]["id"])}_{types["company"]["cl"]["original_id"]}',
                'relation_type_id': relations['venue_company']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venue company relations')
            batch_process(
                cur,
                venue_companies,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblJoinVenuePerson.csv') as input_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            file_lookup = {h: header.index(h) for h in header}

            venue_persons = [r for r in csv_reader]

            relation_config = [
                [file_lookup['venue_id']],
                [file_lookup['person_id'], 'int'],
            ]

            prop_conf = {
                'job_type': [relations['venue_person']['cl']['job_type'], file_lookup['job_type']],
                'date_start': [relations['venue_person']['cl']['date_start'], file_lookup['start_date']],
                'date_end': [relations['venue_person']['cl']['date_end'], file_lookup['end_date']],
            }

            # import company_person relations
            params = {
                'domain_type_id': types['venue']['id'],
                'domain_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'range_type_id': types['person']['id'],
                'range_prop': f'p_{dtu(types["person"]["id"])}_{types["person"]["cl"]["original_id"]}',
                'relation_type_id': relations['venue_person']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing venue person relations')
            batch_process(
                cur,
                venue_persons,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblProgramme.csv') as input_file, \
             open('data/tblProgrammeDate.csv') as date_file, \
             open('data/programmes_image_urls.csv') as image_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('date_start')
            header.append('date_end')
            header.append('dates_mentioned')
            file_lookup = {h: header.index(h) for h in header}

            date_lines = date_file.readlines()
            date_reader = csv.reader(date_lines)

            date_header = next(date_reader)
            date_file_lookup = {h: date_header.index(h) for h in date_header}

            dates_index = {r[0]: r for r in date_reader}

            image_lines = image_file.readlines()
            image_reader = csv.reader(image_lines)

            image_header = next(image_reader)
            image_file_lookup = {h: image_header.index(h) for h in image_header}

            programmes = []
            for row in csv_reader:
                if 'Vertoningsdag' in row[file_lookup['programme_info']]:
                    start_date = dates_index[row[0]][date_file_lookup['programme_date']]
                    row.append(start_date)
                    row.append(start_date)
                    row.append([start_date])
                else:
                    start_date = dates_index[row[0]][date_file_lookup['programme_date']]
                    # TODO: EDTF
                    end_date = datetime.strftime(
                        datetime.strptime(start_date.replace('193X', '1935'), '%Y-%m-%d') + timedelta(days=7),
                        '%Y-%m-%d'
                    )
                    if ('193X' in start_date):
                        end_date = end_date.replace('1935', '193X')
                    # create list with only dates (between parentheses)
                    mentioned_dates = [d.split(')')[0] for d in row[file_lookup['programme_info']].split('(')[1:]]
                    row.append(start_date)
                    row.append(end_date)
                    row.append(mentioned_dates)
                programmes.append(row)

            # Import program items (without mentioned dates)
            prop_conf = {
                'id': [None, file_lookup['programme_id'], 'int'],
                'original_id': [types['programme']['cl']['original_id'], file_lookup['programme_id'], 'int'],
                'date_start': [types['programme']['cl']['date_start'], file_lookup['date_start']],
                'date_end': [types['programme']['cl']['date_end'], file_lookup['date_end']],
            }

            params = {
                'entity_type_id': types['programme']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programmes (without mentioned dates)')
            batch_process(
                cur,
                programmes,
                params,
                add_entity,
                prop_conf,
            )

            programmes_mentioned = []
            for programme in programmes:
                for date_mentioned in programme[file_lookup['dates_mentioned']]:
                    programmes_mentioned.append([programme[0], date_mentioned])

            # Import program items (mentioned dates)
            prop_conf = {
                'id': [None, 0, 'int'],
                'dates_mentioned': [types['programme']['cl']['dates_mentioned'], 1, 'array'],
            }

            params = {
                'entity_type_id': types['programme']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programmes (mentioned dates)')
            batch_process(
                cur,
                programmes_mentioned,
                params,
                update_entity,
                prop_conf,
            )

            programmes_image_urls = [r for r in image_reader]
            # Import Vooruit image urls
            prop_conf = {
                'id': [None, 0, 'int'],
                'vooruit_image': [types['programme']['cl']['vooruit_image'], 1],
            }

            params = {
                'entity_type_id': types['programme']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programmes (Vooruit image urls)')
            batch_process(
                cur,
                programmes_image_urls,
                params,
                update_entity,
                prop_conf,
            )

            # import relation between programmes and venues
            relation_config = [
                [file_lookup['programme_id'], 'int'],
                [file_lookup['venue_id']],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['programme']['id'],
                'domain_prop': f'p_{dtu(types["programme"]["id"])}_{types["programme"]["cl"]["original_id"]}',
                'range_type_id': types['venue']['id'],
                'range_prop': f'p_{dtu(types["venue"]["id"])}_{types["venue"]["cl"]["original_id"]}',
                'relation_type_id': relations['programme_venue']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programme venue relations')
            batch_process(
                cur,
                programmes,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

        with open('data/tblProgrammeItem.csv') as input_file, \
             open('data/tblFilmTitleVariation.csv') as tv_file:
            lines = input_file.readlines()
            csv_reader = csv.reader(lines)

            header = next(csv_reader)
            header.append('mentioned_title')
            file_lookup = {h: header.index(h) for h in header}

            tv_lines = tv_file.readlines()
            tv_reader = csv.reader(tv_lines)

            tv_header = next(tv_reader)
            tv_lookup = {h: tv_header.index(h) for h in tv_header}

            tv_index = {}
            for row in tv_reader:
                tv_index[row[tv_lookup['film_variation_id']]] = row[tv_lookup['title']]

            programme_items = []
            for row in csv_reader:
                # fix s_order: subtract 1
                row[file_lookup['s_order']] = str(int(row[file_lookup['s_order']]) - 1)
                film_variation_id = row[file_lookup['film_variation_id']]
                if film_variation_id != '':
                    row.append(tv_index[film_variation_id])
                else:
                    row.append('')
                programme_items.append(row)

            # import programme items
            prop_conf = {
                'id': [None, file_lookup['programme_item_id'], 'int'],
                'original_id': [types['programme_item']['cl']['original_id'], file_lookup['programme_item_id'], 'int'],
                'mentioned_film': [types['programme_item']['cl']['mentioned_film'], file_lookup['mentioned_title']],
                'mentioned_venue': [types['programme_item']['cl']['mentioned_venue'], file_lookup['info']],
            }

            params = {
                'entity_type_id': types['programme_item']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programme items')
            batch_process(
                cur,
                programme_items,
                params,
                add_entity,
                prop_conf,
            )

            # import relation between programme item and film
            relation_config = [
                [file_lookup['programme_item_id'], 'int'],
                [file_lookup['film_id'], 'int'],
            ]

            prop_conf = {}

            params = {
                'domain_type_id': types['programme_item']['id'],
                'domain_prop': f'p_{dtu(types["programme_item"]["id"])}_{types["programme_item"]["cl"]["original_id"]}',
                'range_type_id': types['film']['id'],
                'range_prop': f'p_{dtu(types["film"]["id"])}_{types["film"]["cl"]["original_id"]}',
                'relation_type_id': relations['programme_item_film']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programme item film relations')
            batch_process(
                cur,
                programme_items,
                params,
                add_relation,
                relation_config,
                prop_conf
            )

            # import relation between programme and programme_item
            relation_config = [
                [file_lookup['programme_id'], 'int'],
                [file_lookup['programme_item_id'], 'int'],
            ]

            prop_conf = {
                'order': [relations['programme_item']['cl']['order'], file_lookup['s_order'], 'int'],
            }

            params = {
                'domain_type_id': types['programme']['id'],
                'domain_prop': f'p_{dtu(types["programme"]["id"])}_{types["programme"]["cl"]["original_id"]}',
                'range_type_id': types['programme_item']['id'],
                'range_prop': f'p_{dtu(types["programme_item"]["id"])}_{types["programme_item"]["cl"]["original_id"]}',
                'relation_type_id': relations['programme_item']['id'],
                'user_id': user_id,
            }

            print('Cinecos importing programme programme item relations')
            batch_process(
                cur,
                programme_items,
                params,
                add_relation,
                relation_config,
                prop_conf
            )
