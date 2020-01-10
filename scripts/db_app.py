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
            systemName VARCHAR(255) NOT NULL,
            displayName VARCHAR(255) NOT NULL,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (systemName)
        );
        -- TODO: role revision?

        CREATE TABLE app.userRole (
            id SERIAL PRIMARY KEY,
            userId INTEGER
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            roleId INTEGER
                REFERENCES app.role (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (userId, roleId)
        );
        -- TODO: userRole revision?

        CREATE TABLE app.project (
            id SERIAL PRIMARY KEY,
            systemName VARCHAR(255) NOT NULL,
            displayName VARCHAR(255) NOT NULL,
            userId INTEGER
                REFERENCES app.user
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (systemName)
        );

        -- CREATE TABLE app.revisionProject (
        --   id SERIAL PRIMARY KEY,
        --   projectId INTEGER
        --     REFERENCES app.project
        --     ON UPDATE NO ACTION ON DELETE  NO ACTION,
        --   systemName VARCHAR(255) NOT NULL,
        --   displayName VARCHAR(255) NOT NULL,
        --   userId INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );

        CREATE TABLE app.entity (
            id SERIAL PRIMARY KEY,
            projectId INTEGER
                REFERENCES app.project
                ON UPDATE CASCADE ON DELETE CASCADE,
            systemName VARCHAR(255) NOT NULL,
            displayName VARCHAR(255) NOT NULL,
            --   classifier BOOLEAN NOT NULL,
            config JSON,
            userId INTEGER
                REFERENCES app.user
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (projectId, systemName)
        );
        CREATE INDEX ON app.entity (projectId);

        CREATE TABLE app.entityCount (
            id INTEGER
                REFERENCES app.entity (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            currentId INTEGER NOT NULL DEFAULT 0,
            UNIQUE (id)
        );

        -- CREATE TABLE app.revisionEntity (
        --   id SERIAL PRIMARY KEY,
        --   entityId INTEGER
        --     REFERENCES app.entity
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   projectId INTEGER
        --     REFERENCES app.project (id)
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   systemName VARCHAR(255) NOT NULL,
        --   displayName VARCHAR(255) NOT NULL,
        --   classifier BOOLEAN NOT NULL,
        --   config JSON,
        --   userId INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );
        -- 
        -- CREATE TABLE app.relation (
        --   id SERIAL PRIMARY KEY,
        --   projectId INTEGER
        --     REFERENCES app.project
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   systemName VARCHAR(255) NOT NULL,
        --   displayName VARCHAR(255) NOT NULL,
        --   inverseId INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   domain JSONB,
        --   range JSONB,
        --   config JSONB,
        --   userId INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   UNIQUE (projectId, systemName)
        -- );
        -- CREATE INDEX ON app.relation (projectId);

        -- CREATE TABLE app.revisionRelation (
        --   id SERIAL PRIMARY KEY,
        --   relationId INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   projectId INTEGER
        --     REFERENCES app.project (id)
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   systemName VARCHAR(255) NOT NULL,
        --   displayName VARCHAR(255) NOT NULL,
        --   inverseId INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE CASCADE ON DELETE CASCADE,
        --   domain JSON,
        --   range JSON,
        --   config JSON,
        --   userId INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );
        ''')