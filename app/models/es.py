import pydantic
import typing


class ElasticSearchBody(pydantic.BaseModel):
    # TODO: add custom validator that allows all aggs possibilities?
    aggs: dict = None
    # from is a python keyword
    from_: int = None
    size: int = None
    # TODO: add custom validator that allows all sort possibilities?
    # https://pydantic-docs.helpmanual.io/usage/validators/
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-sort.html
    sort: typing.Any = None
    # track_total_hits can be a boolean or int valye
    track_total_hits: typing.Union[bool, int] = None
    # TODO: add custom validator that allows all suggest possibilities?
    suggest: dict = None
    # TODO: add custom validator that allows all query possibilities?
    query: dict = None

    class Config:
        fields = {
            # from is a python keyword
            'from_': 'from',
        }

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        # from is a python keyword
        d['from'] = d.pop('from_')
        return d
