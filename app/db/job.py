from asyncpg.connection import Connection
from uuid import UUID

from app.db.base import BaseRepository
from app.db.config import ConfigRepository
from app.models.auth import User
from app.models.job import JobToDisplay


class JobRepository(BaseRepository):
    def __init__(self, conn: Connection) -> None:
        super().__init__(conn)
        self._conf_repo = ConfigRepository(conn)

    async def get_by_project(self, id: UUID, project_name: str) -> JobToDisplay:
        async with self.connection.transaction():
            record = await self.fetchrow(
                '''
                    SELECT
                        job.id,
                        "user".display_name as user_name,
                        project.system_name as project_system_name,
                        project.display_name as project_display_name,
                        entity.system_name as entity_type_system_name,
                        entity.display_name as entity_type_display_name,
                        relation.system_name as relation_type_system_name,
                        relation.display_name as relation_type_display_name,
                        job.type,
                        job.status,
                        job.counter,
                        job.total,
                        job.created,
                        job.started,
                        job.ended
                    FROM app.job
                    LEFT JOIN app.user ON job.user_id = "user".id
                    LEFT JOIN app.project ON job.project_id = project.id
                    LEFT JOIN app.entity ON job.entity_id = entity.id
                    LEFT JOIN app.relation ON job.relation_id = relation.id
                    WHERE job.id = :id
                    AND project.system_name = :project_name
                ''', {
                    'id': str(id),
                    'project_name': project_name,
                }
            )
            if record:
                return JobToDisplay(**dict(record))
            return None

    async def create(self, user: User, type: str, project_name: str = None, entity_type_name: str = None) -> UUID:
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

    async def start(self, id: UUID, total: int = None) -> None:
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

    async def end_with_success(self, id: UUID) -> None:
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

    async def end_with_error(self, id: UUID) -> None:
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
