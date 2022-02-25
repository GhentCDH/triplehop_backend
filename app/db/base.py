import asyncpg
import buildpg
import typing
import uuid
from contextlib import asynccontextmanager

from app.exceptions import InvalidUUIdException

# Specify regex with negative lookbehind
# Prevent conversion of Apache Age vertices or edges with label
RENDERER = buildpg.Renderer(regex=r'(?<![a-z\\:]):([a-z][a-z0-9_]*)')


class BaseRepository:
    def __init__(self, pool: asyncpg.pool.Pool) -> None:
        self._pool = pool

    @staticmethod
    def _render(
        query_template: str,
        params: typing.Dict[str, typing.Any] = None,
    ) -> typing.List[typing.Union[str, typing.List]]:
        """Convert named placeholders to native PostgreSQL syntax for query arguments (used by asyncpg).

        Args:
            query_template (str): Query with named placeholders.
            params (typing.Dict[str, typing.Any]): Query parameters.

        Returns:
            typing.List[str, typing.List]: The query in native PostgreSQL syntax and the corresponding query arguments.
        """
        if params is None:
            query, args = RENDERER(query_template)
        else:
            query, args = RENDERER(query_template, **params)

        query = query.replace('\\:', ':')

        return [query, args]

    @staticmethod
    async def _init_age(connection: asyncpg.connection.Connection) -> None:
        """Set ag_catalog as search_path and load the Apache AGE library file.

        Args:
            connection: The connection (on which a transaction block is active).
        """
        await connection.execute(
            """
                SET search_path = ag_catalog;
            """
        )
        await connection.execute(
            """
                LOAD '$libdir/plugins/age';
            """
        )

    @staticmethod
    def _check_valid_uuid(uuid_to_test, version=4) -> None:
        try:
            uuid.UUID(uuid_to_test, version=version)
        except ValueError:
            raise InvalidUUIdException

    @asynccontextmanager
    async def connection(self) -> None:
        """Create a with statement context manager that yields a connection that can be used for transactions."""
        try:
            connection = await self._pool.acquire()
            yield connection
        finally:
            await self._pool.release(connection)

    async def execute(self, *args, **kwargs):
        return await self._db_call('execute', *args, **kwargs)

    async def executemany(self, *args, **kwargs):
        return await self._db_call('executemany', *args, **kwargs)

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
        params: typing.Union[typing.Dict[str, typing.Any], typing.List[typing.Dict[str, typing.Any]]] = None,
        age: bool = False,
        connection: asyncpg.connection.Connection = None,
    ):
        """Helper method for db calls.

        Args:
            method (str): Asyncpg method to be called on a connection.
            query_template (str): Query with named placeholders.
            params (typing.Union[typing.Dict[str, typing.Any], typing.List[typing.Dict[str, typing.Any]]]):
                Query parameters.
                A list of dicts for executemany, dict for other methods.
            age (bool, optional): Indicates whether Apache AGE is used in the query.
            connection (asyncpg.connection.Connection, optional):
        """
        if method == 'executemany':
            query, _ = self.__class__._render(query_template, params[0])
            # Additional list to allow unpack operation when calling the corresponding asyncpg method
            args = [
                [self.__class__._render(query_template, p)[1] for p in params]
            ]
        else:
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
