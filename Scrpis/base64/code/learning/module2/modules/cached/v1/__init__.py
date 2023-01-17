# -*- coding: utf-8 -*-
from learning.module2.common.data import Outputs

bigquant_cacheable = True
bigquant_deprecated = "请更新到 ${MODULE_NAME} 最新版本"


class BigQuantModule:
    def __init__(self, run, kwargs={}):
        """
        自定义模块，默认具有自动缓存功能。
        :param run: 主函数
        :param kwargs: 主函数参数
        """
        self.__run = run
        self.__kwargs = kwargs

    def run(self):
        output = self.__run(**self.__kwargs)
        if not isinstance(output, dict):
            raise Exception("主函数必须返回dict类型的结果")
        return Outputs(**output)
