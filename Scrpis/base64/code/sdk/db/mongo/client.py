from typing import Dict, Tuple, Type

from pymongo import MongoClient as M
from pymongo.collection import Collection
from pymongo.database import Database

from .setttings import MONGO_DB, MONGO_URL


class MongoClient:

    __cache: Dict[Tuple[str, str, str], Collection] = {}

    @classmethod
    def get_client(cls: Type["MongoClient"], uri: str) -> M:
        """
        尽量不要直接使用这个方法, 使用 MongoClient.get_collection(collection_name)
        """
        return M(uri, connect=False)

    @classmethod
    def get_database(cls: Type["MongoClient"], database: str, uri: str = MONGO_URL) -> Database:
        """
        尽量不要直接使用这个方法, 使用 MongoClient.get_collection(collection_name)
        """
        return cls.get_client(uri)[database]

    @classmethod
    def get_collection(cls: Type["MongoClient"], collection: str, database: str = MONGO_DB, uri: str = MONGO_URL) -> Collection:
        if (uri, database, collection) in cls.__cache:
            return cls.__cache[(uri, database, collection)]

        cls.__cache[(uri, database, collection)] = cls.get_database(database=database, uri=uri)[collection]
        return cls.__cache[(uri, database, collection)]
