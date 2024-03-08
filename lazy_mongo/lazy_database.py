from typing import Dict, NamedTuple
from pymongo.database import Database
from .lazy_collection import LazyCollection


class LazyDatabase(NamedTuple):
    database: Database
    default_collection_name: str = None  # type: ignore

    def __getitem__(self, key: str):
        return LazyCollection(self.database[key])

    def find(
        self,
        collection: str = None,  # type: ignore
        query: Dict = None,  # type: ignore
        project: Dict = None,  # type: ignore
    ):
        coll = self[collection or self.default_collection_name]

        return coll.find(query, project)

    def insert_one(
        self,
        collection: str = None,  # type: ignore
        document: Dict = None,  # type: ignore
    ):
        coll = self[collection or self.default_collection_name]

        return coll.insert_one(document)

    def count(
        self,
        collection: str = None,  # type: ignore
        query: Dict = None,  # type: ignore
    ):
        coll = self[collection or self.default_collection_name]

        return coll.count_documents(query)
