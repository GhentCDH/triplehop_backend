import psycopg2

with psycopg2.connect('dbname=crdb host=127.0.0.1 user=vagrant') as conn:
    with conn.cursor() as cur:
        cur.execute('''
        DROP SCHEMA IF EXISTS app CASCADE;
        CREATE SCHEMA app;

        CREATE TABLE app.user (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            username VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            hashed_password VARCHAR NOT NULL,
            disabled BOOLEAN NOT NULL,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (username)
        );
        -- TODO: user revision?

        CREATE TABLE app.group (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            system_name VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            description TEXT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (system_name)
        );
        -- TODO: group revision?

        CREATE TABLE app.permission (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            system_name VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            description TEXT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (system_name)
        );
        -- TODO: permission revision?

        CREATE TABLE app.users_groups (
            user_id UUID NOT NULL
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            group_id UUID NOT NULL
                REFERENCES app.group (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (user_id, group_id)
        );
        -- TODO: users_groups revision?

        CREATE TABLE app.project (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            system_name VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            user_id UUID NOT NULL
                REFERENCES app.user (id)
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
        --   system_name VARCHAR NOT NULL,
        --   display_name VARCHAR NOT NULL,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );

        -- TODO: make sure entity id (used to construct the VLABEL) is not dependent on other projects
        -- this guarantees a project can be relocated to another database
        CREATE TABLE app.entity (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            project_id UUID NOT NULL
                REFERENCES app.project
                ON UPDATE CASCADE ON DELETE CASCADE,
            system_name VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            --   classifier BOOLEAN NOT NULL,
            config JSON,
            user_id UUID NOT NULL
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (project_id, system_name)
        );
        CREATE INDEX ON app.entity (project_id);

        CREATE TABLE app.entity_count (
            id UUID NOT NULL
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
        --   system_name VARCHAR NOT NULL,
        --   display_name VARCHAR NOT NULL,
        --   classifier BOOLEAN NOT NULL,
        --   config JSON,
        --   user_id INTEGER
        --     REFERENCES app.user
        --     ON UPDATE RESTRICT ON DELETE RESTRICT,
        --   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        --   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        -- );

        -- TODO: cardinality
        -- TODO: bidirectional relations
        CREATE TABLE app.relation (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            project_id UUID NOT NULL
                REFERENCES app.project
                ON UPDATE CASCADE ON DELETE CASCADE,
            system_name VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            --   domain JSONB,
            --   range JSONB,
            config JSON,
            user_id UUID NOT NULL
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (project_id, system_name)
        );
        CREATE INDEX ON app.relation (project_id);

        CREATE TABLE app.relation_count (
            id UUID NOT NULL
                REFERENCES app.relation (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            current_id INTEGER NOT NULL DEFAULT 0,
            UNIQUE (id)
        );

        CREATE TABLE app.relation_domain (
            relation_id UUID NOT NULL
                REFERENCES app.relation (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            entity_id UUID NOT NULL
                REFERENCES app.entity (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            user_id UUID NOT NULL
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (relation_id, entity_id)
        );

        CREATE TABLE app.relation_range (
            relation_id UUID NOT NULL
                REFERENCES app.relation (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            entity_id UUID NOT NULL
                REFERENCES app.entity (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            user_id UUID NOT NULL
                REFERENCES app.user (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (relation_id, entity_id)
        );

        -- CREATE TABLE app.revision_relation (
        --   id SERIAL PRIMARY KEY,
        --   relation_id INTEGER
        --     REFERENCES app.relation
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   project_id INTEGER
        --     REFERENCES app.project (id)
        --     ON UPDATE NO ACTION ON DELETE NO ACTION,
        --   system_name VARCHAR NOT NULL,
        --   display_name VARCHAR NOT NULL,
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

        CREATE TABLE app.groups_permissions (
            group_id UUID NOT NULL
                REFERENCES app.group (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            permission_id UUID NOT NULL
                REFERENCES app.permission (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            project_id UUID
                REFERENCES app.project (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            entity_id UUID
                REFERENCES app.entity (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            relation_id UUID
                REFERENCES app.relation (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (group_id, permission_id, project_id, entity_id)
        );
        -- TODO: groups_permissions revision?

        CREATE TABLE app.job (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            project_id UUID
                REFERENCES app.project (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            entity_id UUID
                REFERENCES app.entity (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            relation_id UUID
                REFERENCES app.relation (id)
                ON UPDATE RESTRICT ON DELETE RESTRICT,
            type VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            done INTEGER,
            total INTEGER,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            started TIMESTAMP WITH TIME ZONE,
            ended TIMESTAMP WITH TIME ZONE
        );
        ''')
