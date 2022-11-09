import json
from datetime import datetime
from typing import Any, Type

from pymongo import ReturnDocument
from sdk.auth import Credential
from sdk.datasource import DataSource
from sdk.db.mongo import MongoClient
from sdk.module import (GetModuleCacheRequestSchema,
                        GetModuleCacheResponseSchema,
                        SetModuleCacheRequestSchema)


class ModuleCacheManager:

    MongoCollection = MongoClient.get_collection("module_cache")

    @classmethod
    def get_module_cache(
        cls: Type["ModuleCacheManager"],
        request: GetModuleCacheRequestSchema,
        credential: Credential,
    ) -> GetModuleCacheResponseSchema:

        now = datetime.now()

        update_data = {"last_hit_at": now}
        # add a param to check papertrading
        if request.is_papertrading:
            update_data["is_papertrading"] = True

        result = cls.MongoCollection.find_one_and_update(
            filter={"_id": request.key},
            update={
                "$set": update_data,
                "$inc": {"hits": 1},
            },
            return_document=ReturnDocument.AFTER,
            upsert=False,
        )

        if result is None:
            return None

        if not cls.__is_datasource_exist(json.loads(result["outputs_json"]), request.is_papertrading, credential.user.username):
            cls.MongoCollection.delete_one({"_id": request.key})
            return None

        return GetModuleCacheResponseSchema(outputs_json=result["outputs_json"])

    @classmethod
    def set_module_cache(
        cls: Type["ModuleCacheManager"],
        request: SetModuleCacheRequestSchema,
        credential: Credential,
    ) -> None:
        now = datetime.now()

        update_data = {"last_hit_at": now}

        # add a param to check papertrading
        if request.is_papertrading:
            update_data["is_papertrading"] = True

        cls.MongoCollection.update_one(
            {
                "_id": request.key,
            },
            {
                "$set": update_data,
                "$inc": {"hits": 1},
                "$setOnInsert": {
                    "outputs_json": request.outputs_json,
                    "created_at": now,
                    "owner": credential.user.username,
                },
            },
            upsert=True,
        )

    @classmethod
    def __is_datasource_exist(
        cls: Type["ModuleCacheManager"],
        obj: Any,
        is_papertrading: bool,
        username: str,
    ) -> bool:
        if not isinstance(obj, dict) and not isinstance(obj, list) and not isinstance(obj, tuple):
            return True
        if isinstance(obj, dict) and (obj.get("__class__", "") == "DataSource" or obj.get("source_id", None)):
            if not DataSource(id=obj.get("id", obj.get("source_id"))).exist():
                return False

            # TODO: need this?
            # else:
            #     try:
            #         # 现在也要保存命中缓存的状态；因为即便datasource没被读取，它的id也要作为cache_key被利用
            #         DataSourceImpl.update_visit_stats(key=_id, is_papertrading=is_papertrading, username=username)
            #     except Exception:
            #         pass

        items = list(obj.values()) if isinstance(obj, dict) else obj
        for v in items:
            if not cls.__is_datasource_exist(v, is_papertrading, username):
                return False
        return True
