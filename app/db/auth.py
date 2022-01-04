from typing import Dict

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.models.auth import User, UserInDB


class AuthRepository(BaseRepository):
    def __init__(
        self,
        config_repo: ConfigRepository,
    ):
        self._config_repo = config_repo

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

    async def get_groups(self, user: User) -> Dict:
        return self.fetch(
            '''
                SELECT
                    group_id
                FROM app.users_groups
                WHERE "user".id = :user_id;
            ''',
            {
                'user_id': str(user.id),
            }
        )

    # TODO: cache?
    async def get_permissions(self, user: User) -> Dict:
        user_groups = self.get_groups(user)
        projects = self._config_repo.get_projects_config()

        permissions = {}
        for project_name in projects:
            if project_name == '__all__':
                continue

            entity_types_config = self._config_repo.get_entity_types_config(project_name)
            for etn, et in entity_types_config.items():
                # data
                if 'data' in et['config'] and 'permissions' in et['config']['data']:
                    for permission, groups in et['config']['data']['permissions'].items():
                        permissions[etn]['data']['permissions'] = {}
                        for group in groups:
                            if group in user_groups:
                                if etn not in permissions:
                                    permissions[etn] = {}
                                if 'data' not in permissions[etn]:
                                    permissions[etn]['data'] = {}
                                permissions[etn]['data'][permission] = []
                                if 'fields' in et['config']['data']:
                                    for field in et['config']['data']['fields'].values():
                                        if (
                                            'permissions' in field
                                            and permission in field['permissions']
                                            and group in field['permissions'][permission]
                                        ) :
                                            permissions[etn]['data'][permission].append(field['system_name'])
        # TODO: check and use


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
