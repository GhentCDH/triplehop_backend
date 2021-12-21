import asyncpg
import buildpg
import typing
from contextlib import asynccontextmanager

RENDERER = buildpg.main.Renderer(regex=r'(?<![a-z\\:]):([a-z][a-z0-9_]*)', sep='__')


class BaseRepository:
    def __init__(self, pool: asyncpg.pool.Pool) -> None:
        self._pool = pool
        self._connection = None
        self._num_connection_required = 0

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
            print('connection context try')
            if self._num_connection_required == 0:
                self._connection = await self._pool.acquire()
            self._num_connection_required += 1
            print(self._num_connection_required)
            yield self._connection
        finally:
            print('connection context finally')
            self._num_connection_required -= 1
            print(self._num_connection_required)
            if self._num_connection_required == 0:
                print('connection context connection released')
                await self._pool.release(self._connection)

    @asynccontextmanager
    async def transaction(self) -> None:
        try:
            print('transaction context try')
            if self._num_connection_required == 0:
                self._connection = await self._pool.acquire()
            self._num_connection_required += 1
            print(self._num_connection_required)
            transaction = self._connection.transaction()
            print('transaction initialized')
            await transaction.start()
            print('transaction started')
            yield transaction
        except Exception as e:
            print(e)
            await transaction.rollback()
            raise
        else:
            await transaction.commit()
        finally:
            print('transaction context finally')
            self._num_connection_required -= 1
            print(self._num_connection_required)
            if self._num_connection_required == 0:
                print('transaction context connection released')
                await self._pool.release(self._connection)

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
    ):
        print(method)
        print(query_template)
        print(params)
        query, args = self.__class__._render(query_template, params)
        if age:
            async with self.transaction():
                # TODO: try to use ag_catalog explicitly where needed
                await self._connection.execute(
                    '''
                        SET search_path = ag_catalog, "$user", public;
                    '''
                )
                await self._connection.execute(
                    '''
                        LOAD '$libdir/plugins/age';
                    '''
                )
                return await getattr(self._connection, method)(query, *args)
        else:
            async with self.connection():
                return await getattr(self._connection, method)(query, *args)
