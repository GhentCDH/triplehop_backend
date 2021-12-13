from typing import Dict

from app.db.base import BaseRepository
from app.models.auth import User, UserInDB


class AuthRepository(BaseRepository):
    async def get_user(self, username: str) -> UserInDB:
        record = await self.fetchrow(
            '''
                SELECT
                    "user".id,
                    "user".username,
                    "user".display_name,
                    "user".hashed_password,
                    "user".disabled
                FROM app.user
                WHERE "user".username = :username;
            ''',
            {
                'username': username,
            }
        )
        if record:
            return UserInDB(**dict(record))
        return None

    async def get_permissions(self, user: User) -> Dict:
        records = await self.fetch(
            '''
                SELECT
                    permission.system_name,
                    project.system_name as project_name,
                    entity.system_name as entity_name,
                    relation.system_name as relation_name,
                    groups_permissions.properties
                FROM app.user
                INNER JOIN app.users_groups ON "user".id = users_groups.user_id
                INNER JOIN app.groups_permissions ON users_groups.group_id = groups_permissions.group_id
                INNER JOIN app.permission ON groups_permissions.permission_id = permission.id
                LEFT JOIN app.project ON groups_permissions.project_id = project.id
                LEFT JOIN app.entity ON groups_permissions.entity_id = entity.id
                LEFT JOIN app.relation ON groups_permissions.relation_id = relation.id
                WHERE "user".id = :user_id;
            ''',
            {
                'user_id': str(user.id),
            }
        )

        permissions = {}
        for record in records:
            if record['system_name'] not in permissions:
                permissions[record['system_name']] = {}

            if record['project_name'] not in permissions[record['system_name']]:
                permissions[record['system_name']][record['project_name']] = {
                    'entities': {},
                    'relations': {},
                }

            # TODO: convert entity property ids to entity property names
            if record['entity_name'] is not None:
                permissions[record['system_name']][record['project_name']]['entities'][record['entity_name']] = \
                    '__all__' if record['properties'] is None else record['properties']

            # TODO: convert relation property ids to relation property names
            if record['relation_name'] is not None:
                permissions[record['system_name']][record['project_name']]['relations'][record['relation_name']] = \
                    '__all__' if record['properties'] is None else record['properties']

        return permissions

    async def denylist_add_token(self, token, expiration_time) -> None:
        await self.execute(
            '''
                INSERT INTO app.token_denylist (token, expires)
                VALUES (:token, NOW() + INTERVAL '1 SECOND' * :expiration_time );
            ''',
            {
                'token': token,
                'expiration_time': expiration_time,
            }
        )

    async def denylist_purge_expired_tokens(self) -> None:
        await self.execute(
            '''
                DELETE
                FROM app.token_denylist
                WHERE token_denylist.expires > now();
            '''
        )

    async def denylist_check_token(self, token: str) -> bool:
        return not (await self.fetchrow(
            '''
                SELECT EXISTS (
                    SELECT 1
                    FROM app.token_denylist
                    WHERE token_denylist.token = :token
                    LIMIT 1
                );
            ''',
            {
                'token': token,
            }
        ))[0]
