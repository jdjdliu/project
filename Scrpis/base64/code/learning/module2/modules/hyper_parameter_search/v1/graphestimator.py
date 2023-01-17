import copy


def remote_run_func(bq_graph, parameters):
    from learning.module2.common.data import Outputs

    result = bq_graph.run(parameters)
    return Outputs(data=result)


class GraphEstimator:
    def __init__(self, bq_graph, scoring, remote_run=False, silent=False, **sk_params):
        self.__bq_graph = bq_graph  # T.Graph
        self.__sk_params = sk_params
        self.__scoring = scoring
        self.__remote_run = remote_run
        self.__silent = silent
        self.__result = None

    def get_params(self, **params):
        res = copy.deepcopy(self.__sk_params)
        res.update({"bq_graph": self.__bq_graph, "scoring": self.__scoring, "remote_run": self.__remote_run, "silent": self.__silent})
        return res

    def set_params(self, **params):
        self.__sk_params.update(params)
        return self

    def fit(self, X=None, y=None, **kwargs):
        if self.__remote_run:
            from learning.api import M

            m_cached = False
            result = M.cached.v3(
                run=remote_run_func,
                kwargs={"bq_graph": self.__bq_graph, "parameters": self.__sk_params},
                m_cached=m_cached,
                m_remote=True,
                m_silent=self.__silent,
            )
            self.__result = result.data if result else None
        else:
            if self.__silent:
                self.__sk_params.update({"*.m_silent": True})
            self.__result = self.__bq_graph.run(self.__sk_params)
        return self.__result

    def score(self, X=None, y=None, **kwargs):
        if self.__result:
            return self.__scoring(self.__result)
            # return self.__result.get(self.__score_module).read_raw_perf()['sharpe'].tail(1)[0]
        else:
            return -float("inf")

    @property
    def result(self):
        return self.__result
