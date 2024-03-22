from typing import Dict, NamedTuple
from pymongo.collection import Collection
from pymongo.results import InsertOneResult


class InsertResponse(NamedTuple):
    ok: bool
    result: InsertOneResult = None
    error: Exception = None
