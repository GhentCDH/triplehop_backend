from typing import Dict

from asyncpg.connection import Connection
from buildpg.main import Renderer
from re import compile as re_compile

EDGE_LABEL_DOES_NOT_EXIST_REGEX = re_compile(r'^edge label "e_[a-f0-9_]{36}" does not exist$')
RENDERER = Renderer(regex=r'(?<![a-z:]):([a-z][a-z\d_]*)', sep='__')


class BaseRepository:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    async def close(self):
        await self._conn.close()

    @property
    def connection(self) -> Connection:
        return self._conn

    @staticmethod
    def _render(query_template: str, params: Dict = None):
        if params is None:
            return RENDERER(query_template)
        else:
            return RENDERER(query_template, **params)

    # TODO: prepared statements with LRU cache?
    async def execute(self, query_template: str, params: Dict = None):
        query, args = self.__class__._render(query_template, params)
        return await self._conn.execute(query, *args)

    # TODO: prepared statements with LRU cache?
    async def fetch(self, query_template: str, params: Dict = None):
        query, args = self.__class__._render(query_template, params)
        return await self._conn.fetch(query, *args)

    # TODO: prepared statements with LRU cache?
    async def fetchrow(self, query_template: str, params: Dict = None):
        query, args = self.__class__._render(query_template, params)
        return await self._conn.fetchrow(query, *args)

    # TODO: prepared statements with LRU cache?
    async def fetchval(self, query_template: str, params: Dict = None):
        query, args = self.__class__._render(query_template, params)
        return await self._conn.fetchval(query, *args)
