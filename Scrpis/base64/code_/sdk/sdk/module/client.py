import re
from typing import Any, Dict, List, Optional, Type

from sdk.auth import Credential
from sdk.httputils import HTTPClient

from .schemas import (GetModuleCacheRequestSchema,
                      GetModuleCacheResponseSchema,
                      SetModuleCacheRequestSchema)
from .settings import USERSERVICE_HOST


class ModuleClient:

    HttpClient = HTTPClient(USERSERVICE_HOST)

    class API:
        GET_MODULE_CACHE = "/api/userservice/module/v1/cache"
        SET_MODULE_CACHE = "/api/userservice/module/v1/cache"
        GET_MODULES_V1 = "/api/userservice/module/v1/module"
        GET_MODULES_V2 = "/api/userservice/module/v2/module"

    @classmethod
    def get_module_cache(
        cls: Type["ModuleClient"],
        params: GetModuleCacheRequestSchema,
        credential: Credential,
    ) -> Optional[GetModuleCacheResponseSchema]:
        response = cls.HttpClient.get(cls.API.GET_MODULE_CACHE, params=params.dict(), credential=credential)

        if response:
            return GetModuleCacheResponseSchema(**response)
        return GetModuleCacheResponseSchema(outputs_json=None)

    @classmethod
    def set_module_cache(
        cls: Type["ModuleClient"],
        params: SetModuleCacheRequestSchema,
        credential: Credential,
    ) -> None:
        cls.HttpClient.post(
            cls.API.GET_MODULE_CACHE,
            payload=params.dict(),
            credential=credential,
        )

    @classmethod
    def get_modules(cls: Type["ModuleClient"], credential: Credential) -> List[Any]:
        result: List[Any] = cls.HttpClient.get(cls.API.GET_MODULES_V1, credential=credential)
        return result

    @classmethod
    def get_modules_v2(cls: Type["ModuleClient"], credential: Credential) -> List[Any]:
        result: List[Any] = cls.HttpClient.get(cls.API.GET_MODULES_V2, credential=credential)
        return result

    @classmethod
    def to_web_format(cls: Type["ModuleClient"], modules: List[Any], credential: Credential) -> List[Any]:
        web_modules = []
        for module in sorted(modules, key=lambda m: (m["rank"] or 0, m["name"] or "")):
            web_module = cls.__build_module(module, username=credential.user.username)
            if web_module:
                web_modules.append(web_module)
        return web_modules

    @classmethod
    def __build_module(
        cls: Type["ModuleClient"],
        module: Dict[Any, Any],
        is_latest: bool = False,
        # alias_name: str = None,
        # friendly_name: str = None,
        username: Optional[str] = None,
    ) -> Optional[Dict[Any, Any]]:
        module_data = module["data"]
        if (not module_data["visible"]) and (username != module["owner"]):
            return None
        interfaces = module_data.get("interface", [])
        mi = cls.__build_interface(interfaces, module)

        version = {}

        version["Category"] = module_data["category"]
        version["ClientVersion"] = "v%s" % module["version"]
        version["ServiceVersion"] = module_data.get("serviceversion", module["version"])
        version["CreatedDate"] = "/Date(0)/"
        version["Description"] = ""
        if module_data.get("desc"):
            version["Description"] = "\n".join([line.strip() for line in module_data.get("desc", "").strip().split("\n")])
        version["DeterministicByDefault"] = module_data["cacheable"]
        version["FamilyId"] = module["name"]
        version["Id"] = "BigQuantSpace.%s.%s-v%s" % (module["name"], module["name"], module["version"])
        version["InformationUrl"] = module_data["doc_url"]
        version["InterfaceString"] = "%s.v%s" % (module["name"], module["version"])
        version["IsDeprecated"] = module_data.get("deprecated", None)
        version["IsDeterministic"] = module_data["cacheable"]
        version["IsLatest"] = is_latest

        version["ModuleInterface"] = mi

        version["ModuleLanguage"] = {"Language": "Python", "Version": "3"}
        version["ModuleType"] = "DefaultModule"
        version["Name"] = "%s%s" % (module_data["friendly_name"], "" if is_latest else " (v%s)" % module["version"])
        version["Owner"] = module["owner"]

        # 自定义模块优化
        version["Opensource"] = module_data.get("opensource", False)
        version["Cacheable"] = module_data.get("cacheable", True)
        version["Shared"] = module["shared"]
        # 判断模块是否开源或者用户是否为模块作者
        try:
            if version["Opensource"] or username == version["Owner"]:
                if module_data.get("main_func") and module_data.get("post_func"):
                    version["MainFunction"] = module_data["main_func"]
                    version["PostProcessingFunction"] = module_data["post_func"]
                else:
                    # 根据源代码分离出主函数和后处理函数
                    source_code = module_data.get("source_code", "")
                    if source_code:
                        source_code_list = source_code[0][1].split("\n\n# 后处理函数")
                        if len(source_code_list) == 2:
                            version["MainFunction"] = source_code_list[0]
                            version["PostProcessingFunction"] = "# 后处理函数" + source_code_list[1]
                        else:
                            source_code_list = re.split(r"def\s+bigquant_postrun", source_code[0][1])
                            if len(source_code_list) == 2:
                                version["MainFunction"] = source_code_list[0]
                                version["PostProcessingFunction"] = "def bigquant_postrun" + source_code_list[1]
                            else:
                                version["MainFunction"] = source_code[0][1]
                                version["PostProcessingFunction"] = (
                                    "# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。\n"
                                    "def bigquant_run(outputs):\n"
                                    "return outputs"
                                )
        except Exception:
            pass

        return version

    @classmethod
    def __build_interface(cls: Type["ModuleClient"], interface: List[Any], module: Dict[Any, Any]) -> Dict[Any, Any]:
        # This only used in userbox
        import learning.module2.common.interface as I  # noqa: E741
        import learning.module2.common.moduleinvoker as moduleinvoker

        mi: Dict[str, Any] = {
            "TopLevelMarkupElements": None,
            "InterfaceString": None,
            "Parameters": [],
            "InputPorts": [],
            "OutputPorts": [],
        }

        has_lazy_run = False

        for p in interface:
            if p["type_code"] == "output_port":
                r = {
                    "MarkupType": "OutputPort",
                    "Name": p["name"],
                    "FriendlyName": cls.__friendly_name_from_desc(p["desc"]) or p["name"],
                    "Description": p["desc"] or p["name"],
                    "IsOptional": p["optional"],
                    "Type": {"Id": p["type"]},
                    "ScriptName": None,
                }
                mi["OutputPorts"].append(r)

                if p["name"] == I.port_name_lazy_run:
                    has_lazy_run = True
            else:
                item = cls.__build_parameter_or_input_port(p, module)
                if item is None:
                    continue
                if item["MarkupType"] == "InputPort":
                    mi["InputPorts"].append(item)
                else:
                    mi["Parameters"].append(item)

        if has_lazy_run:
            annotation = I.bool("延迟运行，在延迟运行模式下，模块不会直接运行，将被打包，通过端口 延迟运行 输出，可以作为其他模块的输入，并在其他模块里开启运行")
            p = annotation.to_json()
            p["name"] = moduleinvoker.ARG_M_LAZY_RUN
            mi["Parameters"].append(cls.__build_parameter_or_input_port(p, module))

        return mi

    @classmethod
    def __build_parameter_or_input_port(cls: Type["ModuleClient"], data: Dict[Any, Any], module: Dict[Any, Any]) -> Optional[Dict[Any, Any]]:
        has_default_value = "default" in data

        module_name = module["name"]
        module_version = module["version"]

        r = {
            "Name": data["name"],
            "FriendlyName": cls.__friendly_name_from_desc(data["desc"]) or data["name"],
            "Description": data["desc"] or data["name"],
            "IsOptional": has_default_value,
        }

        if module_name == "cached" and module_version == 3:
            if data["name"] == "input_ports":
                r["PlaceHolder"] = "input_1, input_2, input_3"
                r["ParameterRules"] = [
                    {"Regex": {"Pattern": "^[A-Za-z]\\w*(,\\s*[A-Za-z]\\w*)*$", "ErrorMessage": "请按 `input_1, input_2, input_3` 格式输入"}}
                ]
            elif data["name"] == "params":
                r["PlaceHolder"] = (
                    "{\n"
                    "    'param_1': 0,\n"
                    "    'param_2': 0.1,\n"
                    "    'param_3': False,\n"
                    "    'param_4': 'foo',\n"
                    "    'param_5': [\n"
                    "        'foo',\n"
                    "        'bar'\n"
                    "    ]\n"
                    "}\n"
                )
            elif data["name"] == "output_ports":
                r["PlaceHolder"] = "data_1, data_2, data_3"
                r["ParameterRules"] = [
                    {"Regex": {"Pattern": "^[A-Za-z]\\w*(,\\s*[A-Za-z]\\w*)*$", "ErrorMessage": "请按 `data_1, data_2, data_3` 格式输入"}}
                ]

        if data["type_code"] == "input_port":
            r["MarkupType"] = "InputPort"
            r["IsVariadic"] = False
            r["MaxVariadicCount"] = None
            r["Types"] = cls.__make_types(data["type"])
            r["ScriptName"] = None
            # add input_port IsOptional
            r["IsOptional"] = data["optional"]
            return r

        r["MarkupType"] = "Parameter"
        r["CredentialDescriptor"] = None
        r["ModeValuesInfo"] = None
        r["HasDefaultValue"] = has_default_value
        if has_default_value:
            r["DefaultValue"] = data["default"]

        r["ParameterType"] = data["type_name"]
        r["HasRules"] = True
        r["ScriptName"] = None

        if data["type_code"] in {"int", "float"}:
            assert data["min"] is not None
            assert data["max"] is not None
            r["ParameterRules"] = [
                {
                    "Min": data["min"],
                    "Max": data["max"],
                }
            ]
        elif data["type_code"] in {"choice"}:
            assert data["values"]
            r["IsOptional"] = False
            r["ParameterRules"] = [
                {
                    "Values": data["values"],
                }
            ]
            r["IsMulti"] = data.get("multi", False)
        elif data["type_code"] in {"code"}:
            r["ScriptName"] = data["language"]
            r["HasRules"] = False
            r["AutoCompleteType"] = data["auto_complete_type"]
            if data.get("default", None):
                r["DefaultValue"] = data["default"]
        elif data["type_code"] in {"str"}:
            r["CanSetLiverunParam"] = data.get("can_set_liverun_param", None)
            r["HasRules"] = False
        elif data["type_code"] in {"bool"}:
            r["IsOptional"] = True
            r["HasDefaultValue"] = True
            if has_default_value and data["default"]:
                r["DefaultValue"] = "True"
            else:
                r["DefaultValue"] = "False"
        elif data["type_code"] in {"doc"}:
            return None
        else:
            raise Exception("unknown type: %s" % (data))
        return r

    @staticmethod
    def __make_types(type_ids: Any) -> List[Dict[Any, Any]]:
        if not isinstance(type_ids, list):
            type_ids = [type_ids]
        return [{"Id": type_id} for type_id in type_ids]

    @staticmethod
    def __friendly_name_from_desc(desc: Optional[str]) -> Optional[str]:
        if not desc:
            return desc
        s = re.split(r"[,\:\.，：。]", desc, 1)
        return s[0].strip()
