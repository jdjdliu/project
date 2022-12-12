# from modulemanager import settings
# from modulemanager.extension.bigshared.utils import _is_module_available, bigmodules_db, create_module_to_db, find_one_by_name, log
# from modulemanager.schemas import ModuleSchema
# from modulemanager.utils import category_orders
# from pymongo import DESCENDING
from typing import Any, Dict, List, Type

from sdk.auth import Credential
from sdk.db.mongo import MongoClient

from userservice.settings import CATEGORY_ORDERS


class ModuleManager:

    MongoCollection = MongoClient.get_collection("module_metadata")

    @classmethod
    def create_module(cls: Type["ModuleManager"]) -> None:
        pass

    @classmethod
    def get_modules(cls: Type["ModuleManager"], credential: Credential) -> List:
        modules = list(cls.MongoCollection.find({"$or": [{"owner": {"$in": [credential.user.username]}}, {"shared": True}]}).sort("name", 1))
        return sorted(modules, key=lambda x: (CATEGORY_ORDERS.get(x["data"]["category"], 0), x["_id"], x.get("rank", 1000000)))

    @classmethod
    def get_modules_v2(cls: Type["ModuleManager"], credential: Credential) -> List:
        modules = list(cls.MongoCollection.find({"$or": [{"owner": {"$in": [credential.user.username]}}, {"shared": True}]}).sort("name", 1))

        mongo_modules = []
        available_owners = [credential.user.username]

        last_module_name = ""
        module: Dict[Any, Any] = {}
        versions: List[Any] = []
        all_modules_length = len(modules)
        for index, user_module in enumerate(modules):
            module_data = user_module["data"]

            if (not module_data["visible"]) and (user_module["owner"] not in available_owners):  # 如果该模块不可见，则不加载入自动补全
                continue
            module_name = user_module["name"]
            if not last_module_name:  # 如果是首次生成，给上一个模块赋值
                last_module_name = module_name
            if last_module_name != module_name:  # 如果是下一个模块，重新生成模块字典和版本列表
                last_module_name = module_name
                mongo_modules.append(module)
                module = {}
                versions = []

            version = {}
            version["author"] = user_module["owner"]
            version["version"] = "v{}".format(user_module["version"])

            # 表明是否自定义模块
            version["custom_module"] = module_data.get("custom_module", False)

            version["arguments"] = module_data.get("arguments", "")
            version["doc"] = module_data.get("doc", "")
            version["sourcecode"] = None
            if module_data.get("opensource", None) or user_module["owner"] in available_owners:
                version["sourcecode"] = module_data.get("source_code", "")
            versions.append(version)

            module["name"] = module_name
            module["versions"] = versions
            if index == all_modules_length - 1:  # 如果是最后一个模块，放入模块列表
                mongo_modules.append(module)

        return mongo_modules


# def create_module(user_nme, module: ModuleSchema, keep_source: bool, internal_create: bool):
#     """创建模块, 含更新模块version"""

#     exist_module = find_one_by_name(name=module.name, sorts=[("version", DESCENDING)])

#     if not exist_module:
#         # 防止非法post随意构造version和rank
#         version = 1
#         rank = 0
#     else:
#         if not exist_module.get("owner", settings.DEFAULT_OWNER) == user_nme:
#             raise HTTPExceptions.EXISTED_NAME

#         version = exist_module["version"] + 1
#         rank = exist_module.get("rank", 0)

#     module.version = version
#     module.rank = rank
#     return create_module_to_db(module, keep_source=keep_source, internal_create=internal_create)


# def is_module_available(user_name: str, module_name: str):
#     """
#     查询模块是否可用
#     """
#     _is_module_available(user_name, module_name, internal_create=True)


# def list_modules(user_name: str):
#     """
#     查询所有模块
#     arguments:
#         @user_name: str
#     """

#     cursor = bigmodules_db.modules.aggregate(
#         [
#             {"$sort": {"name": 1, "version": -1}},
#             {
#                 "$group": {
#                     "_id": {"name": "$name"},
#                     "name": {"$first": "$name"},
#                     "data": {"$first": "$data"},
#                     "shared": {"$first": "$shared"},
#                     "rank": {"$first": "$rank"},
#                     "version": {"$first": "$version"},
#                     "owner": {"$first": "$owner"},
#                 }
#             },
#             {"$match": {"owner": user_name, "data.custom_module": True}},
#             {"$project": {"_id": 0}},
#         ]
#     )
#     all_user_modules = list(cursor)
#     return all_user_modules


# def __get_visible_modules_by_owner(owners: list):
#     """
#     通过owner字段查找所有公开模块, 并按照名称排序.
#     arguments:
#         @owners: list of owners
#     """
#     modules = bigmodules_db.modules.find({"$or": [{"owner": {"$in": owners}}, {"shared": True}]}).sort("name", 1)

#     return list(modules)


# def can_use_module_list(user_names: list) -> list:
#     """
#     查询用户模块并序列化 version1
#     """
#     modules = __get_visible_modules_by_owner(user_names)

#     return sorted(
#         modules,
#         key=lambda x: (
#             category_orders.get(x["data"]["category"], 0),
#             x["_id"],
#             x.get("rank", 1000000),
#         ),
#     )


# def can_use_module_list_v2(user_names: list):
#     """
#     查询用户模块并序列化 version2
#     """
#     all_visible_modules = __get_visible_modules_by_owner(user_names)  # 按照模块名进行排序

#     modules = []
#     versions = []
#     last_module_name = ""
#     module = OrderedDict()
#     all_modules_length = len(all_visible_modules)

#     for index, user_module in enumerate(all_visible_modules):
#         module_data = user_module["data"]

#         if (not module_data["visible"]) and (user_module["owner"] not in user_names):  # 如果该模块不可见，则不加载入自动补全
#             continue
#         module_name = user_module["name"]
#         if not last_module_name:  # 如果是首次生成，给上一个模块赋值
#             last_module_name = module_name
#         if last_module_name != module_name:  # 如果是下一个模块，重新生成模块字典和版本列表
#             last_module_name = module_name
#             modules.append(module)
#             module = OrderedDict()
#             versions = []

#         version = OrderedDict()
#         version["author"] = user_module["owner"]
#         version["version"] = "v{}".format(user_module["version"])

#         # 表明是否自定义模块
#         version["custom_module"] = module_data.get("custom_module", False)

#         version["arguments"] = module_data.get("arguments", "")
#         version["doc"] = module_data.get("doc", "")
#         version["sourcecode"] = None
#         if module_data.get("opensource", None) or user_module["owner"] in user_names:
#             version["sourcecode"] = module_data.get("source_code", "")
#         versions.append(version)

#         module["name"] = module_name
#         module["versions"] = versions
#         if index == all_modules_length - 1:  # 如果是最后一个模块，放入模块列表
#             modules.append(module)

#     return modules
