# -*- coding: utf-8 -*-
from functools import partial
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, ShuffleSplit

import learning.api.tools as T
import learning.module2.common.interface as I  # noqa
from learning.module2.common.utils import check_user_max_workers
from learning.module2.common.data import Outputs
from sdk.utils import BigLogger

from .graphestimator import GraphEstimator

# log = logbook.Logger('超参搜索')
log = BigLogger("超参搜索")
bigquant_cacheable = False

# 模块接口定义
bigquant_category = "高级优化"
bigquant_friendly_name = "超参搜索"
bigquant_doc_url = "https://bigquant.com/docs/"


SEARCH_ALGORITHMS = {"网格搜索": "grid_search", "随机搜索": "random_search", "grid_search": "grid_search", "random_search": "random_search"}


DEFAULT_PARAM_GRID_BUILDER = r"""def bigquant_run():
    param_grid = {}

    # 在这里设置需要调优的参数备选
    # param_grid['m3.features'] = ['close_1/close_0', 'close_2/close_0\nclose_3/close_0']
    # param_grid['m6.number_of_trees'] = [5, 10, 20]

    return param_grid
"""


DEFAULT_SCORING = r"""def bigquant_run(result):
    score = result.get('m19').read_raw_perf()['sharpe'].tail(1)[0]

    return {'score': score}
"""


def _search(algorithm, estimator, parameters, verbose=100, n_jobs=1, pre_dispatch="2*n_jobs", random_state=None, n_iter=10):
    if algorithm == "grid_search":
        return GridSearchCV(
            estimator, parameters, cv=ShuffleSplit(n_splits=1, test_size=0.1), verbose=verbose, n_jobs=n_jobs, pre_dispatch=pre_dispatch
        )
    elif algorithm == "random_search":
        return RandomizedSearchCV(
            estimator,
            parameters,
            cv=ShuffleSplit(n_splits=1, test_size=0.1),
            verbose=verbose,
            n_jobs=n_jobs,
            pre_dispatch=pre_dispatch,
            random_state=random_state,
            n_iter=n_iter,
        )
    else:
        raise Exception("不支持的算法：%s" % algorithm)


def _run(bq_graph, search_algorithm, param_grid, scoring, inputs, search_iterations, workers, worker_distributed_run, worker_silent, random_state):
    g = bq_graph
    estimator = GraphEstimator(g, scoring=scoring, remote_run=worker_distributed_run, silent=worker_silent)
    clf = _search(search_algorithm, estimator, param_grid, n_jobs=workers, n_iter=search_iterations, random_state=random_state)
    results = clf.fit([1] * 10, [1] * 10)
    return results


def bigquant_run(
    param_grid_builder: I.code(
        "超参数输入，构建需要搜索的超参数列表", I.code_python, DEFAULT_PARAM_GRID_BUILDER, specific_type_name="函数", auto_complete_type="python"
    ) = None,
    scoring: I.code("评分函数", I.code_python, DEFAULT_SCORING, specific_type_name="函数", auto_complete_type="python") = None,
    search_algorithm: I.choice("参数搜索算法", values=["网格搜索", "随机搜索"]) = "网格搜索",
    search_iterations: I.int("搜索迭代次数，用于随机搜索") = 10,
    random_state: I.int("随机数种子，用于随机搜索，不填则默认使用np.random") = None,
    workers: I.int("并行运行作业数，会员可以使用更多的并行作业，请联系微信客服 bigq100 开通") = 1,
    worker_distributed_run: I.bool("作业分布式运行，在集群里分布式运行参数搜索作业") = True,
    worker_silent: I.bool("不显示作业日志，如果作业日志太多，可以选择不显示") = True,
    bq_graph_port: I.port("graph，可以重写全局传入的graph", optional=True) = None,
    input_1: I.port("输入1，run函数参数inputs的第1个元素", optional=True) = None,
    input_2: I.port("输入2，run函数参数inputs的第2个元素", optional=True) = None,
    input_3: I.port("输入3，run函数参数inputs的第3个元素", optional=True) = None,
    # run: I.code('run函数', I.code_python, DEFAULT_RUN, specific_type_name='函数', auto_complete_type='python')=None,
    run_now: I.bool("即时执行，如果不勾选，此模块不会即时执行，而是将当前行为打包为graph传入到后续模块执行") = True,
    bq_graph: I.bool("bq_graph，用于接收全局传入的graph，用户设置值无效") = True,
) -> [I.port("结果", "result"),]:
    """
    设置超参数范围和评分函数，自动进行网格搜索。
    """

    inputs = [input_1, input_2, input_3]
    if bq_graph_port is not None:
        bq_graph = bq_graph_port
    search_algorithm = SEARCH_ALGORITHMS.get(search_algorithm, search_algorithm)

    check_user_max_workers_result = check_user_max_workers(workers=workers, log=log, remote_run=worker_distributed_run)
    workers = check_user_max_workers_result.get("workers")
    worker_distributed_run = check_user_max_workers_result.get("remote_run")

    param_grid = param_grid_builder()
    if not param_grid:
        raise Exception("超参数输入不能为空")

    run_kwargs = dict(
        # bq_graph=bq_graph,    # DO NOT set value for this one
        search_algorithm=search_algorithm,
        param_grid=param_grid,
        scoring=scoring,
        inputs=inputs,
        search_iterations=search_iterations,
        random_state=random_state,
        workers=workers,
        worker_distributed_run=worker_distributed_run,
        worker_silent=worker_silent,
    )

    if run_now:
        result = _run(bq_graph, **run_kwargs)
    else:
        result = T.GraphContinue(bq_graph, partial(_run, **run_kwargs))

    return Outputs(result=result)


def bigquant_postrun(outputs):
    return outputs
