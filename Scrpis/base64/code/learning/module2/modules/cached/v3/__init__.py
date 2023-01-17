# -*- coding: utf-8 -*-
import learning.module2.common.interface as I  # noqa
from learning.module2.common.data import Outputs
from learning.module2.common.utils import isinstance_with_name, smart_dict, smart_list

bigquant_cacheable = True


DEFAULT_RUN = """# Python 代码入口函数，input_1/2/3 对应三个输入端，data_1/2/3 对应三个输出端
def bigquant_run(input_1, input_2, input_3):
    # 示例代码如下。在这里编写您的代码
    df = pd.DataFrame({'data': [1, 2, 3]})
    data_1 = DataSource.write_df(df)
    data_2 = DataSource.write_pickle(df)
    return Outputs(data_1=data_1, data_2=data_2, data_3=None)
"""

DEFAULT_POST_RUN = """# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_run(outputs):
    return outputs
"""

# 模块接口定义
bigquant_category = "自定义模块"
bigquant_friendly_name = "自定义Python模块"
bigquant_doc_url = "https://bigquant.com/docs/develop/modules/cache.html"


class BigQuantModule:
    def __init__(
        self,
        run: I.code(
            "主函数，返回Outputs对象",
            I.code_python,
            default=DEFAULT_RUN,
            specific_type_name="函数",
            auto_complete_type="python,history_data_fields,feature_fields,bigexpr_functions",
        ),
        post_run: I.code(
            "后处理函数，输入是主函数的输出，此函数输出不会被缓存",
            I.code_python,
            default=DEFAULT_POST_RUN,
            specific_type_name="函数",
            auto_complete_type="python,history_data_fields,feature_fields,bigexpr_functions",
        ) = None,
        input_1: I.port("输入1，传入到函数的参数 input_1", optional=True) = None,
        input_2: I.port("输入2，传入到函数的参数 input_2", optional=True) = None,
        input_3: I.port("输入3，传入到函数的参数 input_3", optional=True) = None,
        input_ports: I.str("模块输入端，另存为模块时使用，示例input1,input2...") = "",
        params: I.code("模块参数，字典形式，给出参数的值。比如{'param1':1,'param2':2}", default="{}", specific_type_name="字典") = {},
        output_ports: I.str("模块输出端，另存为模块时使用，示例data1,data2...") = "",
        kwargs: I.doc(
            "主函数参数，run的参数，如果函数接受参数 input_1/2/3，如上的 input_1/2/3也将被加入到此参数里。在可视化模式下，只有参数 input_1/2/3 可用。", I.code_python, specific_type_name="字典"
        ) = None,
    ) -> [
        I.port("输出1，对应函数输出的 data_1", "data_1", optional=True),
        I.port("输出2，对应函数输出的 data_2", "data_2", optional=True),
        I.port("输出3，对应函数输出的 data_3", "data_3", optional=True),
    ]:
        """
        执行任意Python代码，支持缓存加速。此模块支持1-3个输入端和1-3个输出端。使用此模块，我们可以快速的自定义模块，并支持缓存加速和增量运算。对于需要较多计算资源 (比如运行时间超过10s) 或者存储资源 (比如生成较多或者较大的文件) 的程序，都用此模块封装。
        """
        self.__run = run

        self.__post_run = post_run

        self.__kwargs = kwargs

        self.__kwargs = self.__kwargs or {}

        self.__input_ports_name = smart_list(input_ports, sep=",")

        self.__params = smart_dict(params)

        # 将 input_* 添加到 kwargs 里
        import inspect

        run_parameters = inspect.signature(run).parameters
        inputs = locals()

        # 添加bigquant_postrun函数
        # get postrun in module_invoke, 2019-02-19
        # if self.__post_run:
        #     globals()['bigquant_postrun'] = self.__post_run

        # 将 params 添加到 kwargs 里
        for key, value in self.__params.items():
            if key in run_parameters and key not in self.__kwargs:
                self.__kwargs[key] = value

        if self.__input_ports_name:
            for i in range(0, len(self.__input_ports_name)):
                if self.__input_ports_name[i] in run_parameters and self.__input_ports_name[i] not in self.__kwargs:
                    self.__kwargs[self.__input_ports_name[i]] = inputs["input_%s" % (i + 1)]
        else:
            run_parameters_keys_list = list(run_parameters.keys())
            for i in range(0, 3):
                param = "input_%s" % (i + 1)
                # 按照参数顺序来确定输入端口
                # param not in inputs case: chunksize. IS this a good way to set param?????????
                #     run=partial(_parallel_map_remote_run_func, func=func, chunksize=chunksize),
                if len(run_parameters_keys_list) >= i + 1 and run_parameters_keys_list[i] not in self.__kwargs:
                    self.__kwargs[run_parameters_keys_list[i]] = inputs[param]
                elif param in run_parameters and param not in self.__kwargs and param in inputs:
                    self.__kwargs[param] = inputs[param]

    def run(self):
        outputs = self.__run(**self.__kwargs)
        if not isinstance_with_name(outputs, Outputs):
            raise Exception("主函数必须返回 Outputs 类型的结果")
        # outputs.bigquant_custom_post_run = self.__post_run
        return outputs


# 这种方式命中缓存后没法执行post_run
# def bigquant_postrun(outputs):
#     if hasattr(outputs, 'bigquant_custom_post_run') and outputs.bigquant_custom_post_run:
#         custom_post_run = outputs.bigquant_custom_post_run
#         outputs = custom_post_run(outputs)
#     return outputs
