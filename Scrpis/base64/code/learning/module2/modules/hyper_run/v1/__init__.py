# -*- coding: utf-8 -*-
from functools import partial

from sdk.utils import BigLogger

import learning.api.tools as T
import learning.module2.common.interface as I
from learning.module2.common.data import Outputs

# log = logbook.Logger('自定义运行')
log = BigLogger("自定义运行")
bigquant_cacheable = False

# 模块接口定义
bigquant_category = "高级优化"
bigquant_friendly_name = "自定义运行"
bigquant_doc_url = "https://bigquant.com/docs/"


DEFAULT_RUN = r"""def bigquant_run(bq_graph, inputs):
    features =['pe_ttm_0', 'shift(close_0,5)/close_0','mean(close_0,10)/close_0']

    parameters_list = []
     
    for feature in features:
        parameters = {'m3.features':feature}
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            print(parameters)
            return g.run(parameters)
        except Exception as e:
            print('ERROR --------', e)
            return None
        
    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=True, silent=True)

    return results
"""


def bigquant_run(
    bq_graph_port: I.port("graph，可以重写全局传入的graph", optional=True) = None,
    input_1: I.port("输入1，run函数参数inputs的第1个元素", optional=True) = None,
    input_2: I.port("输入2，run函数参数inputs的第2个元素", optional=True) = None,
    input_3: I.port("输入3，run函数参数inputs的第3个元素", optional=True) = None,
    run: I.code("run函数", I.code_python, DEFAULT_RUN, specific_type_name="函数", auto_complete_type="python") = None,
    run_now: I.bool("即时执行，如果不勾选，此模块不会即时执行，并将当前行为打包为graph传入到后续模块执行") = True,
    bq_graph: I.bool("bq_graph，用于接收全局传入的graph，用户设置值无效") = True,
) -> [I.port("结果", "result"),]:
    """
    自定义运行，可以在这里批量设置参数，批量运行，比如对某因子池进行单个因子验证，比如对训练模块学习率在某个范围按一定步长单个测试验证。该模块也可结合超参数调优、滚动训练等场景使用。

    """

    inputs = [input_1, input_2, input_3]

    if bq_graph_port is not None:
        bq_graph = bq_graph_port

    if run_now:
        result = run(bq_graph, inputs)
    else:
        result = T.GraphContinue(bq_graph, partial(run, inputs=inputs))

    return Outputs(result=result)


def bigquant_postrun(outputs):
    return outputs
