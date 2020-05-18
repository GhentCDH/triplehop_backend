from pydantic import BaseModel
from typing import List


class ElasticSearchBody(BaseModel):
    query: dict = None
    from_: int = None
    size: int = None
    sort: List[str] = None

    class Config:
        fields = {
            'from_': 'from',
        }

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        d['from'] = d.pop('from_')
        return d
