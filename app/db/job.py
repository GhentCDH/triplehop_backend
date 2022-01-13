import asyncpg
import uuid

from app.db.base import BaseRepository


class JobRepository(BaseRepository):
    async def get_by_project(self, id: str, project_name: str) -> asyncpg.Record:
        return await self.fetchrow(
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

    async def create(self, type: str, user_id: str, project_id: str = None, entity_type_id: str = None) -> uuid.UUID:
        return await self.fetchval(
            '''
                INSERT INTO app.job(user_id, project_id, entity_id, type, status)
                VALUES (:user_id, :project_id, :entity_id, :type, :status)
                RETURNING id
            ''', {
                'user_id': user_id,
                'project_id': project_id,
                'entity_id': entity_type_id,
                'type': type,
                'status': 'created',
            }
        )

    async def start(self, id: uuid.UUID, total: int = None) -> str:
        return await self.execute(
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

    async def update_counter(self, id: uuid.UUID, counter: int = None) -> str:
        return await self.execute(
            '''
                UPDATE app.job
                SET counter = :counter
                WHERE id = :job_id
            ''', {
                'counter': counter,
                'job_id': id,
            }
        )

    async def end_with_success(self, id: uuid.UUID) -> str:
        return await self.execute(
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

    async def end_with_error(self, id: uuid.UUID) -> str:
        return await self.execute(
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
