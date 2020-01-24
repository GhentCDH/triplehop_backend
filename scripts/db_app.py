import psycopg2

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute('''
        DROP SCHEMA IF EXISTS app CASCADE;
        CREATE SCHEMA app;

        CREATE TABLE app.user (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (email)
        );
        -- TODO: user revision?

        CREATE TABLE app.role (
            id SERIAL PRIMARY KEY,
            system_name VARCHAR(255) NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (system_name)
        );
        -- TODO: role revision?

        CREATE TABLE app.user_role (
            id SERIAL PRIMARY KEY,
            user_id INTEGER
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            role_id INTEGER
                REFERENCES app.role (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (user_id, role_id)
        );
        -- TODO: user_role revision?

        CREATE TABLE app.project (
            id SERIAL PRIMARY KEY,
            system_name VARCHAR(255) NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            user_id INTEGER
                REFERENCES app.user
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (system_name)
        );

        -- CREATE TABLE app.revision_project (
        --   id SERIAL PRIMARY KEY,
        --   project_id INTEGER
        --     REFERENCES app.project
        --     ON UPDATE NO ACTION ON DELETE  NO ACTION,
        --   system_name VARCHAR(255) NOT NULL,
        --   display_name VARCHAR(255) NOT NULL,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );

        CREATE TABLE app.entity (
            id SERIAL PRIMARY KEY,
            project_id INTEGER
                REFERENCES app.project
                ON UPDATE CASCADE ON DELETE CASCADE,
            system_name VARCHAR(255) NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            --   classifier BOOLEAN NOT NULL,
            config JSON,
            user_id INTEGER
                REFERENCES app.user
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (project_id, system_name)
        );
        CREATE INDEX ON app.entity (project_id);

        CREATE TABLE app.entity_count (
            id INTEGER
                REFERENCES app.entity (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            current_id INTEGER NOT NULL DEFAULT 0,
            UNIQUE (id)
        );

        -- CREATE TABLE app.revision_entity (
        --   id SERIAL PRIMARY KEY,
        --   entity_id INTEGER
        --     REFERENCES app.entity
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   project_id INTEGER
        --     REFERENCES app.project (id)
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   system_name VARCHAR(255) NOT NULL,
        --   display_name VARCHAR(255) NOT NULL,
        --   classifier BOOLEAN NOT NULL,
        --   config JSON,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );
        --
        -- CREATE TABLE app.relation (
        --   id SERIAL PRIMARY KEY,
        --   project_id INTEGER
        --     REFERENCES app.project
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   system_name VARCHAR(255) NOT NULL,
        --   display_name VARCHAR(255) NOT NULL,
        --   inverse_id INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   domain JSONB,
        --   range JSONB,
        --   config JSONB,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   UNIQUE (project_id, system_name)
        -- );
        -- CREATE INDEX ON app.relation (project_id);

        -- CREATE TABLE app.revision_relation (
        --   id SERIAL PRIMARY KEY,
        --   relation_id INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   project_id INTEGER
        --     REFERENCES app.project (id)
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   system_name VARCHAR(255) NOT NULL,
        --   display_name VARCHAR(255) NOT NULL,
        --   inverse_id INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   domain JSON,
        --   range JSON,
        --   config JSON,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );
        ''')
