import asyncpg
import buildpg
import typing

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

    async def execute(
        self,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
    ):
        async with self._pool.acquire() as conn:
            query, args = self.__class__._render(query_template, params)
            if age:
                async with conn.transaction():
                    await self.__class__._init_age(conn)
                    return await conn.execute(query, *args)
            return await conn.execute(query, *args)

    async def fetch(
        self,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
    ):
        async with self._pool.acquire() as conn:
            query, args = self.__class__._render(query_template, params)
            if age:
                async with conn.transaction():
                    await self.__class__._init_age(conn)
                    return await conn.fetch(query, *args)
            return await conn.fetch(query, *args)

    async def fetchrow(
        self,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
    ):
        async with self._pool.acquire() as conn:
            query, args = self.__class__._render(query_template, params)
            if age:
                async with conn.transaction():
                    await self.__class__._init_age(conn)
                    return await conn.fetchrow(query, *args)
            return await conn.fetchrow(query, *args)

    async def fetchval(
        self,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
    ):
        async with self._pool.acquire() as conn:
            query, args = self.__class__._render(query_template, params)
            if age:
                async with conn.transaction():
                    await self.__class__._init_age(conn)
                    return await conn.fetchval(query, *args)
            return await conn.fetchval(query, *args)
