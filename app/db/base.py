import asyncpg
import buildpg
import typing
from contextlib import asynccontextmanager

RENDERER = buildpg.main.Renderer(regex=r'(?<![a-z\\:]):([a-z][a-z0-9_]*)', sep='__')


class BaseRepository:
    def __init__(self, pool: asyncpg.pool.Pool) -> None:
        self._pool = pool
        self._connection = None
        self._connection_acquired = False
        self._age_initialized = False

    @staticmethod
    def _render(query_template: str, params: typing.Dict[str, typing.Any] = None):
        if params is None:
            query, args = RENDERER(query_template)
        else:
            query, args = RENDERER(query_template, **params)
        query = query.replace('\\:', ':')
        return [query, args]

    @asynccontextmanager
    async def transaction(self) -> None:
        try:
            if not self._connection_acquired:
                self._connection = await self._pool.acquire()
                self._connection_acquired = True
            transaction = self._connection.transaction()
            await transaction.start()
            yield transaction
        except:
            await transaction.rollback()
            raise
        else:
            await transaction.commit()
        finally:
            if not self._connection.is_in_transaction():
                await self._pool.release(self._connection)
                self._connection_acquired = False
                self._age_initialized = False

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
                    result = await conn.fetch(query, *args)
                    return result
                    # return await conn.fetch(query, *args)
            result = await conn.fetch(query, *args)
            return result
            # return await conn.fetch(query, *args)

    async def fetchrow(
        self,
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
        age: bool = False,
    ):
        query, args = self.__class__._render(query_template, params)
        if self._connection is not None:
            if age:
                async with self._connection.transaction():
                    await self.__class__._init_age(self._connection)
                    return await self._connection.fetchrow(query, *args)
            return await self._connection.fetchrow(query, *args)

        async with self._pool.acquire() as conn:
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
