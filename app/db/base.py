from asyncpg.connection import Connection
from buildpg.main import Renderer


class BaseRepository:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._render = Renderer(regex=r'(?<![a-z:]):([a-z][a-z\d_]*)', sep='__')

    async def close(self):
        await self._conn.close()

    @property
    def connection(self) -> Connection:
        return self._conn

    # TODO: prepared statements with LRU cache?
    async def fetch(self, query_template: str, **kwargs):
        query, args = self._render(query_template, **kwargs)
        return await self._conn.fetch(query, *args)

    # TODO: prepared statements with LRU cache?
    async def fetchrow(self, query_template: str, **kwargs):
        query, args = self._render(query_template, **kwargs)
        return await self._conn.fetchrow(query, *args)
