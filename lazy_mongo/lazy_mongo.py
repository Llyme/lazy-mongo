from typing import Dict
from pymongo import MongoClient
from .lazy_database import LazyDatabase


class LazyMongo:
    def __init__(self):
        self.mongo: MongoClient = None  # type: ignore
        self.default_database: str = None  # type: ignore
        self.default_collection: str = None  # type: ignore

    def connect(self, uri: str):
        self.mongo = MongoClient(uri)

        return self

    def __getitem__(self, key: str):
        return LazyDatabase(
            database=self.mongo[key or self.default_database],
            default_collection_name=self.default_collection,
        )

    def find(
        self,
        database: str = None,  # type: ignore
        collection: str = None,  # type: ignore
        query: Dict = None,  # type: ignore
        project: Dict = None,  # type: ignore
    ):
        db = self[database or self.default_database]
        coll = db[collection or self.default_collection]

        return coll.find(query, project)

    def insert_one(
        self,
        database: str = None,  # type: ignore
        collection: str = None,  # type: ignore
        document: Dict = None,  # type: ignore
    ):
        db = self[database or self.default_database]
        coll = db[collection or self.default_collection]

        return coll.insert_one(document)

    def count(
        self,
        database: str = None,  # type: ignore
        collection: str = None,  # type: ignore
        query: Dict = None,  # type: ignore
    ):
        db = self[database or self.default_database]
        coll = db[collection or self.default_collection]

        return coll.count(query)
