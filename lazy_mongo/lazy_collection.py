from typing import Dict, NamedTuple
from pymongo.collection import Collection


class LazyCollection(NamedTuple):
    collection: Collection

    def find(
        self,
        query: Dict = None,  # type: ignore
        project: Dict = None,  # type: ignore
    ):
        return self.collection.find(query, project)

    def insert_one(
        self,
        document: Dict = None,  # type: ignore
    ):
        try:
            return self.collection.insert_one(document)

        except Exception as e:
            return e

    def count(
        self,
        query: Dict = None,  # type: ignore
    ):
        return self.collection.count_documents(query)
