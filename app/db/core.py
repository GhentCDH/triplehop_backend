from typing import AsyncGenerator, Callable, Type

from asyncpg.pool import Pool
from databases import Database
from fastapi import Depends, FastAPI
from fastapi.dependencies.utils import solve_generator
from starlette.requests import Request

from app.config import DATABASE
from app.db.base import BaseRepository


async def db_connect(app: FastAPI) -> None:
    app.state.db = Database('postgresql://vagrant@127.0.0.1/crdb')
    await app.state.db.connect()


async def db_disconnect(app: FastAPI) -> None:
    await app.state.db.disconnect()


async def get_repository_from_request(request: Request, repo_type: Type[BaseRepository], *_) -> BaseRepository:
    return repo_type(request.app.state.db, *_)
