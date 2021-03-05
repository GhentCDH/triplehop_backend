from typing import Dict

from asyncpg.connection import Connection
from buildpg.main import Renderer
from databases import Database
from re import compile as re_compile

EDGE_LABEL_DOES_NOT_EXIST_REGEX = re_compile(r'^edge label "e_[a-f0-9_]{36}" does not exist$')
RENDERER = Renderer(regex=r'(?<![a-z:]):([a-z][a-z\d_]*)', sep='__')


class BaseRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    # @staticmethod
    # def _render(query_template: str, params: Dict = None):
    #     if params is None:
    #         return RENDERER(query_template)
    #     else:
    #         return RENDERER(query_template, **params)
    #
    # async def execute(self, query_template: str, params: Dict = None):
    #     query, args = self.__class__._render(query_template, params)
    #     return await self._db.execute(query, *args)
    #
    # async def fetch_one(self, query_template: str, params: Dict = None):
    #     query, args = self.__class__._render(query_template, params)
    #     return await self._conn.fetch_one(query, *args)
    #
    # async def fetch_all(self, query_template: str, params: Dict = None):
    #     query, args = self.__class__._render(query_template, params)
    #     return await self._conn.fetch_all(query, *args)
