# Back-end for CRDB

## Prerequisites

* A PostgreSQL server with the AGE extension running at localhost

    ```
    # https://age.apache.org/docs/installation.html - https://github.com/apache/incubator-age/issues/33
    sudo apt-get install bison build-essential flex postgresql-server-dev-11 postgresql-client-11 postgresql-11 unzip
    wget https://github.com/apache/incubator-age/archive/refs/heads/master.zip
    unzip master.zip
    cd incubator-age-master
    make clean
    make
    sudo make install
    sudo mkdir /usr/lib/postgresql/11/lib/plugins
    sudo ln -s /usr/lib/postgresql/11/lib/age.so /usr/lib/postgresql/11/lib/plugins/age.so

    sudo -u postgres createuser --interactive --pwprompt
        Enter name of role to add: crdb
        Enter password for new role:
        Enter it again:
        Shall the new role be a superuser? (y/n) n
        Shall the new role be allowed to create databases? (y/n) n
        Shall the new role be allowed to create more new roles? (y/n) n

    sudo -u postgres createdb -O crdb crdb
    sudo -u postgres psql -d crdb -c "CREATE EXTENSION age;"
    sudo -u postgres psql -d crdb -c "GRANT USAGE ON SCHEMA ag_catalog TO crdb;"
    sudo -u postgres psql -d crdb -c "GRANT SELECT ON TABLE ag_catalog.ag_label TO crdb;"
    ```

* PostGIS

    ```
    sudo apt-get install postgis
    sudo -u postgres psql -d crdb -c "CREATE EXTENSION postgis;"
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
https://www.encode.io/databases cannot be used, see https://github.com/encode/databases/issues/134