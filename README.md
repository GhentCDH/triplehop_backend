# Back-end for CRDB

## Prerequisites

* An agensgraph running at localhost

    ```
    # https://github.com/bitnine-oss/agensgraph
    git clone https://github.com/bitnine-oss/agensgraph.git
    sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison

    cd agensgraph
    ./configure --prefix=$(pwd)
    make install
    . ag-env.sh

    # https://bitnine.net/quick-start-guide-html/
    mkdir ~/db_cluster
    export AGDATA=/home/vagrant/db_cluster
    initdb
    ag_ctl start
    createdb crdb
    ```

* PostGIS

    ```
    # https://github.com/bitnine-oss/agensgraph/issues/430#issuecomment-433169791
    cd ~/agensgraph/src
    wget -c https://download.osgeo.org/postgis/source/postgis-3.0.0.tar.gz
    sudo apt-get install libxml2-dev libgeos-dev libgdal-dev libproj-dev
    tar zxvf postgis-3.0.0.tar.gz
    cd postgis-3.0.0/
    ./configure
    make
    make install
    ```

    Tijdens een verbinding met een databank (`agens -d crdb`):
    ```
    -- https://postgis.net/install/
    CREATE EXTENSION postgis;
    -- CREATE EXTENSION postgis_raster;
    -- CREATE EXTENSION postgis_topology;
    ```

* A virtualenv running with requirements installed

    ```
    virtualenv -p python3 venv_crdb_backend
    source venv_crdb_backend/bin/activate
    pip install -r requirements.txt
    ```

## Usage

* Run database

    ```
    cd agensgraph
    . ag-env.sh
    export AGDATA=/home/vagrant/db_cluster
    ag_ctl start
    ```

* Populate database

    ```
    scripts/init.sh
    ```

* Run backend in develop mode

    ```
    uvicorn app.main:app --reload --host 0.0.0.0
    ```

## Restrictions resulting from the GraphQL conventions that are used

All system names must follow the form `/[_A-Za-z][_0-9A-Za-z]*/` (see https://spec.graphql.org/June2018/#sec-Names)

### Forbidden entity type (system) names
* "query", "geometry", "entity_config", "entity_field_config"
* names ending with `_s`
* names starting with `r_` or `ri_`

### Forbidden relation (system) names
* "query"
* names ending with `_s`

### Forbidden property names
* "entity"
* "limit"
* "offset"

## Example requests
```
{
  Film(id: 2) {
    id
    title
    year
    r_director_s {
      id
      entity {
        ... on Person {
          id
          name
          ri_director_s {
            id
            entity {
              ... on Film {
                id
                title
              }
            }
          }
        }
      }
    }
  }
}
```

```
{
  Person(id: 2) {
    id
    name
    ri_director_s {
      id
      entity {
        ... on Film {
          id
          title
          year
          r_director_s {
            id
          }
        }
      }
    }
  }
}
```

## Acknowledgements

Inspired by:
* https://testdriven.io/blog/fastapi-crud/
* https://github.com/nsidnev/fastapi-realworld-example-app
