# TripleHop Contributing Guide

We're really excited that you are interested in contributing to TripleHop. Please take a moment to read through our [Code of Conduct](CODE_OF_CONDUCT.md) first. All contributions (participation in discussions, issues, pull requests, ...) are welcome. Unfortunately, we cannot make commitments that issues will be resolved or pull requests will be merged swiftly, especially for new features.

Documentation is currently severely lacking. Please contact <https://github.ugent.be/pdpotter> to get started.

## Development set-up (based on a Debian Virtual Machine)

* A PostgreSQL server with the AGE extension running at localhost

    ```sh
    # https://age.apache.org/age-manual/master/intro/setup.html
    sudo apt-get install bison build-essential flex postgresql-server-dev-11 postgresql-client-11 postgresql-11 unzip
    wget https://github.com/apache/age/archive/refs/tags/v1.1.0-rc0.zip
    unzip v1.1.0-rc0.zip
    cd age-1.1.0-rc0/
    make clean
    make
    sudo make install

    # enable non-root users to use age extension
    sudo mkdir /usr/lib/postgresql/11/lib/plugins
    sudo ln -s /usr/lib/postgresql/11/lib/age.so /usr/lib/postgresql/11/lib/plugins/age.so

    # create user
    sudo -u postgres createuser --interactive --pwprompt
        Enter name of role to add: triplehop
        Enter password for new role:
        Enter it again:
        Shall the new role be a superuser? (y/n) n
        Shall the new role be allowed to create databases? (y/n) n
        Shall the new role be allowed to create more new roles? (y/n) n

    # create extension and grant required permissions
    sudo -u postgres createdb -O triplehop triplehop
    sudo -u postgres psql -d triplehop -c "CREATE EXTENSION age;"
    sudo -u postgres psql -d triplehop -c "GRANT USAGE ON SCHEMA ag_catalog TO triplehop;"
    sudo -u postgres psql -d triplehop -c "GRANT SELECT ON TABLE ag_catalog.ag_label TO triplehop;"
    ```

* pgcrypto (gen_random_uuid)

    ```sh
    sudo -u postgres psql -d triplehop -c "CREATE EXTENSION pgcrypto;"
    ```

* An elasticsearch server with the ICU Analysis Plugin

    ```sh
    # install elasticsearch
    sudo apt-get -y install apt-transport-https gnupg
    wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
    echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-7.x.list
    sudo apt-get update
    sudo apt-get install default-jdk
    sudo apt-get install elasticsearch

    # configure for development
    sudo vim /etc/elasticsearch/elasticsearch.yml

        network.host: 0.0.0.0
        discovery.type: single-node

    # install plugins
    /usr/share/elasticsearch/bin/elasticsearch-plugin install analysis-icu

    # start elasticsearch on system boot
    sudo systemctl daemon-reload
    sudo systemctl enable elasticsearch.service

    # start elasticsearch now
    systemctl start elasticsearch.service
    ```

* Poetry

    ```sh
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
    ```

* Download code

    ```sh
    git clone git@github.com:GhentCDH/triplehop_backend.git
    ```

* Install Python dependencies (in code folder)

    ```sh
    poetry install
    ```

## Usage

* Run backend in develop mode

    ```sh
    poetry run uvicorn app.main:app --reload --host 0.0.0.0
    ```

## Restrictions resulting from the GraphQL conventions that are used

All system names must follow the form `/[_A-Za-z][_0-9A-Za-z]*/` (see <https://spec.graphql.org/June2018/#sec-Names>)

### Forbidden project (system) names

* `__all__`

### Forbidden entity type (system) names

* `query`, `geometry`, `entity_config`, `entity_field_config`
* names ending with `_s`
* names starting with `r_` or `ri_`
* `__all__`

### Forbidden relation (system) names

* `query`
* names ending with `_s`
* `__all__`

### Forbidden property names

* `entity`
* `limit`
* `offset`
