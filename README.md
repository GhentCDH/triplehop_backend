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

## Acknowledgements

Inspired by:
* https://testdriven.io/blog/fastapi-crud/
* https://github.com/nsidnev/fastapi-realworld-example-app
