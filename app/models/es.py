from pydantic import BaseModel
from typing import Any


class ElasticSearchBody(BaseModel):
    # TODO: add custom validator that allows all aggs possibilities?
    aggs: dict = None
    from_: int = None
    size: int = None
    # TODO: add custom validator that allows all sort possibilities?
    # https://pydantic-docs.helpmanual.io/usage/validators/
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-sort.html
    sort: Any = None
    # TODO: add custom validator that allows all suggest possibilities?
    suggest: dict = None
    # TODO: add custom validator that allows all query possibilities?
    query: dict = None

    class Config:
        fields = {
            'from_': 'from',
        }

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        d['from'] = d.pop('from_')
        return d
