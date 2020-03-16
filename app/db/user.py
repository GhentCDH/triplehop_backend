from typing import Dict

from app.db.base import BaseRepository


class UserRepository(BaseRepository):
    async def get_user(self, email: str) -> Dict:
        return await self.fetchrow(
            '''
                SELECT
                    user.id,
                    user.email,
                    user.full_name,
                    user.hashed_password,
                    user.disabled
                FROM app.user
                WHERE user.email = :email;
            ''',
            {
                'email': email,
            }
        )
