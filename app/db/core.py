from typing import AsyncGenerator, Callable, List, Type

from asyncpg import create_pool, introspection
from asyncpg.connection import Connection
from asyncpg.pool import Pool
from fastapi import Depends, FastAPI
from fastapi.dependencies.utils import solve_generator
from starlette.requests import Request

from app.config import DATABASE
from app.db.base import BaseRepository


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def utd(string: str) -> str:
    '''Replace all underscores in a string with dashes.'''
    return string.replace('_', '-')


# https://github.com/MagicStack/asyncpg/issues/413
async def _set_type_codec(conn: Connection, typenames: List) -> None:
    schema = 'pg_catalog'
    format = 'text'
    conn._check_open()
    for typename in typenames:
        typeinfo = await conn.fetchrow(
            introspection.TYPE_BY_NAME, typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        oid = typeinfo['oid']
        conn._protocol.get_settings().add_python_codec(
            oid, typename, schema, 'scalar',
            lambda a: a, lambda a: a, format)

    # Statement cache is no longer valid due to codec changes.
    conn._drop_local_statement_cache()


async def db_connect(app: FastAPI) -> None:
    app.state.pool = await create_pool(**DATABASE)


async def db_disconnect(app: FastAPI) -> None:
    await app.state.pool.close()


def _get_db_pool(request: Request) -> Pool:
    return request.app.state.pool


def get_repository(repo_type: Type[BaseRepository]) -> Callable:
    async def _get_repo(
        pool: Pool = Depends(_get_db_pool),
    ) -> AsyncGenerator[BaseRepository, None]:
        async with pool.acquire() as conn:
            await _set_type_codec(
                conn,
                [
                    'graphid',
                    'vertex',
                    'edge',
                    'graphpath'
                ]
            )
            yield repo_type(conn)

    return _get_repo


async def get_repository_from_request(request: Request, repo_type: Type[BaseRepository]) -> BaseRepository:
    return await solve_generator(
        call=get_repository(repo_type),
        stack=request.scope.get("fastapi_astack"),
        sub_values={'pool': request.app.state.pool},
    )
