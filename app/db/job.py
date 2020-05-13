from asyncpg.connection import Connection
from uuid import UUID

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.models.auth import User


class JobRepository(BaseRepository):
    def __init__(self, conn: Connection) -> None:
        super().__init__(conn)
        self._conf_repo = ConfigRepository(conn)

    async def create_job(self, user: User, type: str, project_name: str = None, entity_type_name: str = None) -> UUID:
        async with self.connection.transaction():
            project_id = None
            entity_type_id = None
            if project_name is not None:
                project_id = await self._conf_repo.get_project_id_by_name(project_name)
            if entity_type_name is not None:
                entity_type_id = await self._conf_repo.get_entity_type_id_by_name(project_name, entity_type_name)

            job_id = await self.fetchval(
                '''
                    INSERT INTO app.job(user_id, project_id, entity_id, type, status)
                    VALUES (:user_id, :project_id, :entity_id, :type, :status)
                    RETURNING id
                ''', {
                    'user_id': user.id,
                    'project_id': project_id,
                    'entity_id': entity_type_id,
                    'type': type,
                    'status': 'created',
                }
            )

            return job_id

    async def start_job(self, id: UUID, total: int = None) -> None:
        await self.execute(
            '''
                UPDATE app.job
                SET status = :status,
                    counter = 0,
                    total = :total,
                    started = NOW()
                WHERE id = :job_id
            ''', {
                'status': 'started',
                'total': total,
                'job_id': id,
            }
        )

    async def update_counter(self, id: UUID, counter: int = None) -> None:
        await self.execute(
            '''
                UPDATE app.job
                SET counter = :counter
                WHERE id = :job_id
            ''', {
                'counter': counter,
                'job_id': id,
            }
        )

    async def end_job_with_success(self, id: UUID) -> None:
        await self.execute(
            '''
                UPDATE app.job
                SET status = :status,
                    counter = total,
                    ended = NOW()
                WHERE id = :job_id
            ''', {
                'status': 'success',
                'job_id': id,
            }
        )

    async def end_job_with_error(self, id: UUID) -> None:
        await self.execute(
            '''
                UPDATE app.job
                SET status = :status,
                    ended = NOW()
                WHERE id = :job_id
            ''', {
                'status': 'error',
                'job_id': id,
            }
        )
