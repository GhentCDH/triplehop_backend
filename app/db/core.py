from typing import AsyncGenerator, Callable, List, Type

from asyncpg import create_pool, introspection
from asyncpg.pool import Pool
from fastapi import Depends, FastAPI
from starlette.requests import Request

from app.db.base import BaseRepository


# https://github.com/MagicStack/asyncpg/issues/413
async def _set_type_codec(pool: Pool, typenames: List) -> None:
    async with pool.acquire() as conn:
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
    app.state.pool = await create_pool(
        host='127.0.0.1',
        database='crdb',
        user='vagrant'
    )
    await _set_type_codec(
        app.state.pool,
        [
            'graphid',
            'vertex',
            'edge',
            'graphpath'
        ]
    )


async def db_disconnect(app: FastAPI) -> None:
    await app.state.pool.close()


def _get_db_pool(request: Request) -> Pool:
    return request.app.state.pool


def get_repository(repo_type: Type[BaseRepository]) -> Callable:
    async def _get_repo(
        pool: Pool = Depends(_get_db_pool),
    ) -> AsyncGenerator[BaseRepository, None]:
        async with pool.acquire() as conn:
            yield repo_type(conn)

    return _get_repo
