import asyncpg
import typing

from pydantic.types import UUID4

from app.db.base import BaseRepository
from app.models.auth import User


class AuthRepository(BaseRepository):
    async def get_user(self, username: str) -> typing.Optional[User]:
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

        if record is None:
            return None

        return User(**record)

    async def get_groups(self, user: User) -> typing.List[str]:
        records = await self.fetch(
            '''
                SELECT
                    group_id
                FROM app.users_groups
                WHERE users_groups.user_id = :user_id;
            ''',
            {
                'user_id': str(user.id),
            }
        )

        return [str(record['group_id']) for record in records]

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
