import asyncpg
import fastapi
import starlette
import typing

from app.config import DATABASE
from app.db.base import BaseRepository


async def db_connect(app: fastapi.FastAPI) -> None:
    app.state.pool = await asyncpg.create_pool(**DATABASE)


async def db_disconnect(app: fastapi.FastAPI) -> None:
    await app.state.pool.close()


def get_repository_from_request(
    request: starlette.requests.Request,
    repo_type: typing.Type[BaseRepository],
    *_
) -> BaseRepository:
    return repo_type(request.app.state.pool, *_)
