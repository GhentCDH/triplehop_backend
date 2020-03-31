from typing import Dict

from app.auth.models import UserInDB
from app.db.base import BaseRepository


class UserRepository(BaseRepository):
    async def get_user(self, username: str) -> Dict:
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
