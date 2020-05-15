from pydantic import BaseModel


class ElasticSearchBody(BaseModel):
    query: dict = None
    # from_: int = None
    # size: int = None
    #
    # class Config:
    #     fields = {
    #         'from_': 'from',
    #     }
