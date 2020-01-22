from asyncpg.connection import Connection
from buildpg.main import Renderer

class BaseRepository:
    def __init__( self, conn: Connection) -> None:
        self._conn = conn
        self._render = Renderer(regex=r'(?<![a-z:]):([a-z][a-z\d_]*)', sep='__')

    async def fetchrow(self, query_template: str, **kwargs):
        query, args = self._render(query_template, **kwargs)
        return await self._conn.fetchrow(query, *args)
