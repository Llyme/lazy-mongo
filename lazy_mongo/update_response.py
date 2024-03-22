from typing import NamedTuple
from pymongo.results import UpdateResult


class UpdateResponse(NamedTuple):
    ok: bool
    result: UpdateResult = None
    error: Exception = None
