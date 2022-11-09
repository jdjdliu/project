from sdk.auth import Credential
from sdk.module import ModuleClient

from learning.module2.common.moduleinvoker import module_invoke


class BigQuantModules:
    """
    BigQuant模块入口。

    模块调用方式：M.模块名.v版本号(参数1=值1, 参数2=值2, ..)
    示例代码：M.filter.v3(input_data=some_ds, expr='date > "2017-01-01"')
    """

    def __init__(self: "BigQuantModules") -> None:
        for module in ModuleClient.get_modules_v2(Credential.from_env()):
            setattr(self, module["name"], BigQuantModule(module["name"], module["versions"]))

    def __getattr__(self: "BigQuantModules", name: str) -> "BigQuantModule":
        return BigQuantModule(name)

    def __repr__(self: "BigQuantModules") -> str:
        return self.__doc__

    def __getitem__(self: "BigQuantModules", key: str) -> "BigQuantModule":
        s = key.split(".")
        m = getattr(self, s[0])
        if len(s) > 1:
            m = m[s[1]]
        return m

    def m_get_module(self: "BigQuantModules", name: str) -> "BigQuantModule":
        return getattr(self, name)


class BigQuantModule:
    def __init__(self, name, versions=None):
        self.__name = name
        if versions:
            latest_version = versions[0]
            for version in versions:
                setattr(self, version["version"], BigQuantModuleVersion(name, version))
                latest_version = version if version["version"] > latest_version["version"] else latest_version
            setattr(self, "latest", BigQuantModuleVersion(name, latest_version))
        self.__doc__ = "模块：%s\n可用版本(推荐使用最新版本)：%s" % (name, ", ".join([version["version"] for version in reversed(versions)]) if versions else "")

    def __getattr__(self, version):
        return BigQuantModuleVersion(self.__name, version)

    def __repr__(self):
        return self.__doc__

    def __getitem__(self, key):
        return getattr(self, key)

    def m_get_version(self, version):
        return getattr(self, version)


class BigQuantModuleVersion:
    def __init__(self, name, version):
        self.__name = name
        if isinstance(version, str):
            self.__version = version
            self.__doc__ = "helo unkonw"
            self.m_sourcecode = None
            self.m_author = None
            self.m_arguments = None
            self.m_custom_module = False
        else:
            self.__version = version["version"]
            self.__doc__ = version["doc"]
            self.m_arguments = version["arguments"]
            self.m_sourcecode = version["sourcecode"]
            self.m_author = version["author"]
            # 区分自定义模块
            self.m_custom_module = version.get("custom_module", False)

    def __call__(self, **kwargs):
        return module_invoke(self.__name, self.__version, self.m_custom_module, kwargs)

    def __repr__(self):
        return "M.%s.%s%s" % (self.__name, self.__version, self.m_arguments)


M = BigQuantModules()

if __name__ == "__main__":
    # test code
    print(M.hellomodule.v1)
    print(M.hellomodule.v1.m_arguments)
    print(M.hellomodule.v1.m_sourcecode)
