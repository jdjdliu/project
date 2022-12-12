# -*- coding: utf-8 -*-
from learning.module2.common.data import Outputs
from learning.module2.common.utils import isinstance_with_name


bigquant_cacheable = True


class BigQuantModule:
    def __init__(self, run, kwargs={}):
        """
        缓存模块。使用此模块，用户可以快速的对函数做封装，支持缓存等功能。结合缓存，我们可以实现增量运算。重新修改和运行一个实验，只有修改的部分需要重新计算，可以极大的提升运行效率。

        强烈建议，对于需要较多计算资源 (比如运行时间超过10s) 或者存储资源 (比如生成较多或者较大的文件) 的程序，都用cached封装。

        缓存key由run的源代码和kwargs值确定生成。run函数里如何需要用到全局变量，可以通过kwargs参数传入 (推荐) 或者使用m_deps参数传入，否则可能导致缓存获取到错误数据。

        BigQuant平台提供的模块 (M.) 都支持了缓存。

        :param 函数 run: 主函数，需要返回Outputs对象
        :param 字典dict kwargs: run的参数
        :return: run的返回值
        :rtype: Outputs
        """
        self.__run = run
        self.__kwargs = kwargs

    def run(self):
        output = self.__run(**self.__kwargs)
        if not isinstance_with_name(output, Outputs):
            raise Exception("主函数必须返回 Outputs 类型的结果")
        return output
