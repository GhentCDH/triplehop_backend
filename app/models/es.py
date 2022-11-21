import typing

import pydantic
import typing_extensions

from app.es.base import DEFAULT_FROM, DEFAULT_SIZE, MAX_RESULT_WINDOW


class ElasticSearchBody(pydantic.BaseModel):
    # TODO: add custom validator based on entity type config?
    # https://pydantic-docs.helpmanual.io/usage/validators/
    filters: typing.Dict = None
    page: int = None
    size: int = None
    # TODO: add custom validator based on entity type config?
    sortBy: str = None
    sortOrder: typing_extensions.Literal["asc", "desc"] = None

    @pydantic.root_validator()
    def check_result_window(cls, values):
        if values["size"]:
            es_size = values["size"]
        else:
            es_size = DEFAULT_SIZE

        if values["page"]:
            es_from = (values["page"] - 1) * es_size
        else:
            es_from = DEFAULT_FROM

        if es_from + es_size > MAX_RESULT_WINDOW:
            raise pydantic.ValueError(
                "Only the first 10,000 results can be displayed.",
            )

        return values


class ElasticSuggestBody(pydantic.BaseModel):
    # TODO: add custom validator based on entity type config?
    field: str
    value: str
