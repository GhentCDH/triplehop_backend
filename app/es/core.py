import elasticsearch
import fastapi
import starlette
import typing

from app.config import ELASTICSEARCH
from app.es.base import BaseElasticsearch


def es_connect(app: fastapi.FastAPI) -> None:
    app.state.es = elasticsearch.AsyncElasticsearch(**ELASTICSEARCH)


async def es_disconnect(app: fastapi.FastAPI) -> None:
    await app.state.es.close()


def get_es_from_request(
    request: starlette.requests.Request, repo_type: typing.Type[BaseElasticsearch], *_
) -> BaseElasticsearch:
    return repo_type(request.app.state.es, *_)
