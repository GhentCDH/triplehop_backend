import asyncpg
import buildpg
import typing
from contextlib import asynccontextmanager

RENDERER = buildpg.main.Renderer(regex=r'(?<![a-z\\:]):([a-z][a-z0-9_]*)', sep='__')


class BaseRepository:
    def __init__(self, pool: asyncpg.pool.Pool) -> None:
        self._pool = pool

    @staticmethod
    def _render(query_template: str, params: typing.Dict[str, typing.Any] = None):
        if params is None:
            query, args = RENDERER(query_template)
        else:
            query, args = RENDERER(query_template, **params)
        query = query.replace('\\:', ':')
        return [query, args]

    @asynccontextmanager
    async def connection(self) -> None:
        try:
            connection = await self._pool.acquire()
            yield connection
        finally:
            await self._pool.release(connection)

    # Make sure apache Age queries can be executed
    @staticmethod
    async def _init_age(conn: asyncpg.connection.Connection):
        await conn.execute(
            '''
                SET search_path = ag_catalog, "$user", public;
            '''
        )
        await conn.execute(
            '''
                LOAD '$libdir/plugins/age';
            '''
        )

    async def execute(self, *args, **kwargs):
        return await self._db_call('execute', *args, **kwargs)

    async def fetch(self, *args, **kwargs):
        return await self._db_call('fetch', *args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        return await self._db_call('fetchrow', *args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        return await self._db_call('fetchval', *args, **kwargs)

    async def _db_call(
        self,
        method: str,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
        connection: asyncpg.connection.Connection = None,
    ):
        query, args = self.__class__._render(query_template, params)
        if connection is None:
            async with self._pool.acquire() as connection:
                if age:
                    async with connection.transaction():
                        await self.__class__._init_age(connection)
                        return await getattr(connection, method)(query, *args)
                else:
                    return await getattr(connection, method)(query, *args)
        else:
            if age:
                async with connection.transaction():
                    await self.__class__._init_age(connection)
                    return await getattr(connection, method)(query, *args)
            else:
                return await getattr(connection, method)(query, *args)
