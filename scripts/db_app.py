from asyncio import get_event_loop
from databases import Database

from config import DATABASE_CONNECTION_STRING


async def create_app_structure():
    async with Database(DATABASE_CONNECTION_STRING) as db:

        await db.execute(
            '''
                DROP SCHEMA IF EXISTS app CASCADE;
            '''
        )

        await db.execute(
            '''
                CREATE SCHEMA app;
            '''
        )

        await db.execute(
            '''
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
            '''
        )
        # TODO: user revision?

        await db.execute(
            '''
                INSERT INTO app.user (username, display_name, hashed_password, disabled)
                VALUES (:username, :display_name, :hashed_password, :disabled);
            ''',
            {
                'username': 'system',
                'display_name': 'System user',
                'hashed_password': '',
                'disabled': True,
            }
        )

        await db.execute(
            '''
                CREATE TABLE app.group (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    system_name VARCHAR NOT NULL,
                    display_name VARCHAR NOT NULL,
                    description TEXT,
                    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    UNIQUE (system_name)
                );
            '''
        )
        # TODO: group revision?

        await db.execute(
            '''
                CREATE TABLE app.permission (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    system_name VARCHAR NOT NULL,
                    display_name VARCHAR NOT NULL,
                    description TEXT,
                    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    UNIQUE (system_name)
                );
            '''
        )
        # TODO: permission revision?

        await db.execute(
            '''
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
            '''
        )
        # TODO: users_groups revision?

        await db.execute(
            '''
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
            '''
        )

        await db.execute(
            '''
                INSERT INTO app.project (system_name, display_name, user_id)
                VALUES (
                    :system_name,
                    :display_name,
                    (SELECT "user".id FROM app.user WHERE "user".username = :username)
                );
            ''',
            {
                'system_name': '__all__',
                'display_name': 'All projects',
                'username': 'system',
            }
        )

        # TODO: project revision?
        # CREATE TABLE app.revision_project (
        #   id SERIAL PRIMARY KEY,
        #   project_id INTEGER
        #     REFERENCES app.project
        #     ON UPDATE NO ACTION ON DELETE  NO ACTION,
        #   system_name VARCHAR NOT NULL,
        #   display_name VARCHAR NOT NULL,
        #   user_id INTEGER
        #     REFERENCES app.user
        #     ON UPDATE RESTRICT ON DELETE RESTRICT,
        #   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        #   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        # );

        await db.execute(
            '''
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
            '''
        )
        await db.execute(
            '''
                CREATE INDEX ON app.entity (project_id);
            '''
        )

        await db.execute(
            '''
                INSERT INTO app.entity (project_id, system_name, display_name, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE project.system_name = :project_name),
                    :system_name,
                    :display_name,
                    (SELECT "user".id FROM app.user WHERE "user".username = :username)
                );
            ''',
            {
                'project_name': '__all__',
                'system_name': '__all__',
                'display_name': 'All entities',
                'username': 'system',
            }
        )

        await db.execute(
            '''
                CREATE TABLE app.entity_count (
                    id UUID NOT NULL
                        REFERENCES app.entity (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    current_id INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (id)
                );
            '''
        )

        # TODO: entity revision?
        # CREATE TABLE app.revision_entity (
        #   id SERIAL PRIMARY KEY,
        #   entity_id INTEGER
        #     REFERENCES app.entity
        #     ON UPDATE NO ACTION ON DELETE NO ACTION,
        #   project_id INTEGER
        #     REFERENCES app.project (id)
        #     ON UPDATE NO ACTION ON DELETE NO ACTION,
        #   system_name VARCHAR NOT NULL,
        #   display_name VARCHAR NOT NULL,
        #   classifier BOOLEAN NOT NULL,
        #   config JSON,
        #   user_id INTEGER
        #     REFERENCES app.user
        #     ON UPDATE RESTRICT ON DELETE RESTRICT,
        #   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        #   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        # );

        # TODO: cardinality
        # TODO: bidirectional relations

        await db.execute(
            '''
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
            '''
        )
        await db.execute(
            '''
                CREATE INDEX ON app.relation (project_id);
            '''
        )

        await db.execute(
            '''
                CREATE TABLE app.relation_count (
                    id UUID NOT NULL
                        REFERENCES app.relation (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    current_id INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (id)
                );
            '''
        )

        await db.execute(
            '''
                INSERT INTO app.relation (project_id, system_name, display_name, user_id)
                VALUES (
                    (SELECT project.id FROM app.project WHERE project.system_name = :project_name),
                    :system_name,
                    :display_name,
                    (SELECT "user".id FROM app.user WHERE "user".username = :username)
                );
            ''',
            {
                'project_name': '__all__',
                'system_name': '__all__',
                'display_name': 'All relations',
                'username': 'system',
            }
        )

        await db.execute(
            '''
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
            '''
        )

        await db.execute(
            '''
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
            '''
        )

        # TODO: relation revision?
        # CREATE TABLE app.revision_relation (
        #   id SERIAL PRIMARY KEY,
        #   relation_id INTEGER
        #     REFERENCES app.relation
        #     ON UPDATE NO ACTION ON DELETE NO ACTION,
        #   project_id INTEGER
        #     REFERENCES app.project (id)
        #     ON UPDATE NO ACTION ON DELETE NO ACTION,
        #   system_name VARCHAR NOT NULL,
        #   display_name VARCHAR NOT NULL,
        #   inverse_id INTEGER
        #     REFERENCES app.relation
        #     ON UPDATE CASCADE ON DELETE CASCADE,
        #   domain JSON,
        #   range JSON,
        #   config JSON,
        #   user_id INTEGER
        #     REFERENCES app.user
        #     ON UPDATE RESTRICT ON DELETE RESTRICT,
        #   created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        #   modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        # );

        await db.execute(
            '''
                CREATE TABLE app.groups_permissions (
                    group_id UUID NOT NULL
                        REFERENCES app.group (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    permission_id UUID NOT NULL
                        REFERENCES app.permission (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    project_id UUID NOT NULL
                        REFERENCES app.project (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    entity_id UUID NOT NULL
                        REFERENCES app.entity (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    relation_id UUID NOT NULL
                        REFERENCES app.relation (id)
                        ON UPDATE RESTRICT ON DELETE RESTRICT,
                    properties VARCHAR[],
                    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    UNIQUE (group_id, permission_id, project_id, entity_id)
                );
            '''
        )
        # TODO: groups_permissions revision?

        await db.execute(
            '''
                CREATE TABLE app.job (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL
                        REFERENCES app.user (id)
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
                    type VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    counter INTEGER,
                    total INTEGER,
                    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                    started TIMESTAMP WITH TIME ZONE,
                    ended TIMESTAMP WITH TIME ZONE
                );
            '''
        )


def main():
    loop = get_event_loop()
    loop.run_until_complete(create_app_structure())
    loop.close()


if __name__ == '__main__':
    main()
