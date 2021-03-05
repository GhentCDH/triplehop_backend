# Back-end for CRDB

## Prerequisites

* A PostgreSQL server with the AGE extension running at localhost

    ```
    # https://age.apache.org/docs/installation.html - https://github.com/apache/incubator-age/issues/33
    wget https://github.com/bitnine-oss/AgensGraph-Extension/archive/master.zip
    unzip master.unzip
    cd AgensGraph-Extension-master
    sudo apt-get install bison build-essential flex postgresql-server-dev-11
    make clean
    make
    sudo make install

    sudo -u postgres createuser --interactive --pwprompt
        Enter name of role to add: crdb
        Enter password for new role:
        Enter it again:
        Shall the new role be a superuser? (y/n) n
        Shall the new role be allowed to create databases? (y/n) n
        Shall the new role be allowed to create more new roles? (y/n) n

    sudo -u postgres createdb -O crdb crdb
    sudo -u postgres psql -d crdb -c "CREATE EXTENSION age;"
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
    CREATE EXTENSION IF NOT EXISTS postgis;
    -- CREATE EXTENSION IF NOT EXISTS postgis_raster;
    -- CREATE EXTENSION IF NOT EXISTS postgis_topology;
    ```

* pgcrypto (gen_random_uuid)

    ```
    sudo -u postgres psql -d crdb -c "CREATE EXTENSION pgcrypto;"
    ```

* A virtualenv running with requirements installed

    ```
    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

* An elasticsearch server with the ICU Analysis Plugin

    ```
    sudo /home/vagrant/install/elasticsearch7.sh
    sudo cp /etc/elasticsearch/elasticsearch.yml-orig /etc/elasticsearch/elasticsearch.yml
    sudo vim /etc/elasticsearch/elasticsearch.yml

        network.host: 0.0.0.0
        discovery.type: single-node

    sudo systemctl restart elasticsearch.service
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

### Forbidden project (system) names
* \_\_all\_\_

### Forbidden entity type (system) names
* "query", "geometry", "entity_config", "entity_field_config"
* names ending with `_s`
* names starting with `r_` or `ri_`
* \_\_all\_\_

### Forbidden relation (system) names
* "query"
* names ending with `_s`
* \_\_all\_\_

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

## Remarks
https://www.encode.io/databases cannot be used, since it is impossible to add Agensgraph type codecs (see https://github.com/MagicStack/asyncpg/issues/413)

## Acknowledgements

Inspired by:
* https://testdriven.io/blog/fastapi-crud/
* https://github.com/nsidnev/fastapi-realworld-example-app
