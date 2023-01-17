import datetime
import os
import re
import shutil
import uuid
from typing import Dict, List, Tuple, Type

from pymongo import DESCENDING
from sdk.auth import Credential
from sdk.db.mongo import MongoClient
from userservice.settings import CATEGORY_ORDERS, CUSTOM_MODULE_ROOT, DEFAULT_IMPORT

from .common import ensure_data_dir, ensure_data_dir_for_file


class CustomModuleManager:
    MongoCollection = MongoClient.get_collection("module_metadata")

    @classmethod
    def create(
        cls: Type["CustomModuleManager"],
        name: str,
        shared: bool,
        data: Dict,
        credential: Credential,
    ) -> dict:
        last_version = cls.MongoCollection.find_one(
            {"name": name},
            sort=[("version", DESCENDING)],
            projection={
                "owner": True,
                "version": True,
                "rank": True,
            },
        )

        if last_version is None:
            version = 1
            rank = 0
        else:
            version = last_version["version"] + 1
            rank = last_version.get("rank", 0)
            if last_version.get("owner", "bigquant") != credential.user.username:
                raise Exception("模块{}已由其他用户创建，请使用其他名称命名您的模块".format(name))

        status, module = cls._create_module(
            name=name,
            version=version,
            shared=shared,
            data=data,
            rank=rank,
            keep_source=True,
            credential=credential,
        )
        return module

    @classmethod
    def list(cls: Type["CustomModuleManager"], credential: Credential) -> List:  # TODO: List[modules]
        cursor = cls.MongoCollection.aggregate(
            [
                {"$sort": {"name": 1, "version": -1}},
                {
                    "$group": {
                        "_id": {"name": "$name"},
                        "name": {"$first": "$name"},
                        "data": {"$first": "$data"},
                        "shared": {"$first": "$shared"},
                        "rank": {"$first": "$rank"},
                        "version": {"$first": "$version"},
                        "owner": {"$first": "$owner"},
                    }
                },
                {"$match": {"owner": credential.user.username, "data.custom_module": True}},
                {"$project": {"_id": 0}},
            ]
        )
        user_all_modules = list(cursor)
        print(credential.user.username, user_all_modules)

        return user_all_modules

    @classmethod
    def module_is_valid(cls: Type["CustomModuleManager"], module_name: str, credential: Credential) -> Dict:

        last_version = cls.MongoCollection.find_one(
            {"name": module_name},
            sort=[("version", DESCENDING)],
            projection={"owner": True},
        )

        if last_version:
            if last_version.get("owner", "bigquant") != credential.user.username:
                error_msg = "模块{}已由其他用户创建，请使用其他名称命名您的模块".format(module_name)
            else:
                error_msg = "您已经创建过模块{}，请使用其他名称或者升级已有模块".format(module_name)
            return {"status": False, "err_msg": error_msg}

        return {"status": True, "err_msg": ""}


    @classmethod
    def can_use_module_list(cls: Type["CustomModuleManager"], credential: Credential):
        modules = cls._get_modules(credential.user.username)
        modules = cls._sort_modules(modules)

        return modules

    @classmethod
    def _create_module(
        cls: Type["CustomModuleManager"],
        name: str,
        version: int,
        shared: bool,
        data: Dict,
        rank: int,
        keep_source: bool,
        credential: Credential,
        internal_create: bool = False,
    ) -> Tuple[bool, Dict]:
        # 1. write to temp file and compile
        if not internal_create:  # only store in mongodb, do not compile, do replace files
            data["category"] = "用户模块"
            data["serviceversion"] = version
            # 与内部模块进行区分
            data["custom_module"] = True
            # 替换source_code的函数定义
            code_arguments = []
            # 添加参数字段，便于自动补全
            arguments = []
            data_interfaces = data["interface"]
            for data_interface in data_interfaces:
                if data_interface.get("type_code", "") == "output_port":
                    continue
                formatted = (
                    "%s=%s" % (data_interface["name"], repr(data_interface["default"]))
                    if data_interface.get("default", None)
                    else data_interface["name"] + "="
                )
                arguments.append(formatted)
                if data_interface.get("type_code", "") == "input_port":
                    code_formatted = "%s=None" % (data_interface["name"]) if data_interface.get("optional", False) else data_interface["name"]
                elif data_interface.get("type_code", "") == "str":
                    code_formatted = (
                        "%s='%s'" % (data_interface["name"], data_interface["default"])
                        if data_interface.get("default", None)
                        else "%s=None" % (data_interface["name"])
                    )
                elif data_interface.get("type_code", "") == "code":
                    code_formatted = (
                        '%s="""%s"""' % (data_interface["name"], data_interface["default"])
                        if data_interface.get("default", None)
                        else "%s=None" % (data_interface["name"])
                    )
                elif (
                    data_interface.get("type_code", "") == "choice"
                    and data_interface.get("enumerate_type", None) == "string"
                    and not data_interface.get("multi", False)
                ):
                    code_formatted = (
                        "%s='%s'" % (data_interface["name"], data_interface["default"])
                        if data_interface.get("default", None)
                        else "%s=None" % (data_interface["name"])
                    )
                elif data_interface.get("type_code", "") == "choice" and data_interface.get("enumerate_type", None) is None:
                    code_formatted = "%s=None" % (data_interface["name"])
                else:
                    code_formatted = (
                        "%s=%s" % (data_interface["name"], data_interface["default"])
                        if data_interface.get("default", None)
                        else "%s=None" % (data_interface["name"])
                    )
                code_arguments.append(code_formatted)
            data["arguments"] = "({})".format(", ".join(arguments))

            cls._make_module(  # TODO: [vv]
                name,
                version,
                data["source_code"],
                data["source_deps"],
                "def bigquant_run({}):".format(", ".join(code_arguments)),
                data.get("cacheable", False),
            )

        # 2. wirte to mongo db
        # if not keep_source:   # cmb: we have to keep source code.
        #     del data["source_code"]
        #     del data["source_deps"]
        #     if data.get("main_func"):
        #         del data["main_func"]
        #     if data.get("post_func"):
        #         del data["post_func"]
        if data.get("create_type", None):
            del data["create_type"]
        module_id = "%s.%s" % (name, version)

        module = {
            "_id": module_id,
            "name": name,
            "version": version,
            "owner": credential.user.username,
            "shared": shared,
            "data": data,
            "rank": rank,
            "updated_at": datetime.datetime.now(),
        }
        cls.MongoCollection.replace_one(
            {"_id": module_id},
            module,
            upsert=True,
        )
        return True, module

    @classmethod
    def _get_modules(cls: Type["CustomModuleManager"], user_name: str) -> List:
        owners = [user_name]
        cursor = cls.MongoCollection.find({"$or": [{"owner": {"$in": owners}}, {"shared": True}]}).sort("name", 1)

        return list(cursor)

    @classmethod
    def _sort_modules(self, modules):
        # TODO: performance issue? write order to db?
        return sorted(modules, key=lambda x: (CATEGORY_ORDERS.get(x["data"]["category"], 0), x["_id"], x.get("rank", 1000000)))

    @classmethod
    def _make_module(
        cls: Type["CustomModuleManager"],
        name: str,
        version: int,
        source_code: str,
        source_deps: str,
        code_arguments: str = "",
        cacheable: bool = True,
    ) -> None:
        compile_dir = cls._write_source_files(source_code, source_deps, code_arguments, cacheable)

        module_dir = ensure_data_dir(CUSTOM_MODULE_ROOT, name, "v%s" % version)
        moudle_init_py = os.path.join(CUSTOM_MODULE_ROOT, name, "init.py")
        if not os.path.exists(moudle_init_py):
            print(f"moudle_init_py: {moudle_init_py}")
            os.mknod(moudle_init_py)
            os.chmod(moudle_init_py, mode=0o644)
        if os.path.exists(module_dir):
            shutil.rmtree(module_dir)

        shutil.move(compile_dir, module_dir)

    @classmethod
    def _write_source_files(
        cls: Type["CustomModuleManager"],
        source_code,
        source_deps,
        code_arguments,
        cacheable,
    ) -> str:
        source_root = os.path.join("/tmp/bigmodules", "m%s" % uuid.uuid1().hex)
        if os.path.exists(source_root):
            shutil.rmtree(source_root)
        os.makedirs(source_root)
        for source_type, write_mode, files in [("source_code", "w", source_code), ("source_deps", "wb", source_deps)]:
            for name, content in files:
                path = os.path.join(source_root, name)
                ensure_data_dir_for_file(path)
                with open(path, write_mode) as writer:
                    if source_type == "source_code":
                        if not cacheable:
                            content = DEFAULT_IMPORT + "\n\nbigquant_cacheable = False\n\n" + content
                        else:
                            content = DEFAULT_IMPORT + "\n\n" + content
                        # 替换参数定义，添加默认值，正则匹配非贪婪模式
                        content = re.sub(r"def\s+bigquant_run\s*(.*?)\s*:", code_arguments, content, count=1, flags=re.S)
                    writer.write(content)
        return source_root
