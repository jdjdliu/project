import copy


def _split_module_and_param(s, param_required=False):
    s = s.split(".")
    if param_required and len(s) != 2:
        raise Exception("错误：不合法数据 %s，正确示例：m1.datasource_id" % s)
    if len(s) == 1:
        s.append(None)
    return s


class GraphExecution:
    ALL = "*"
    META_KEY_PREFIX = "__"
    KEY_ENABLED = META_KEY_PREFIX + "enabled__"
    KEY_TYPE = META_KEY_PREFIX + "type__"
    KEY_DEPENDEES = META_KEY_PREFIX + "dependees__"
    KEY_DEPENDERS = META_KEY_PREFIX + "dependers__"
    KEY_OUTPUTS = META_KEY_PREFIX + "outputs__"
    TYPE_PREFIX = "M."

    def __init__(self, data, parameters):
        self.__data = data
        self.__parameters = parameters

    def _to_dict(self, data):
        g = {}
        for k, v in data.items():
            module, param = _split_module_and_param(k, False)
            if module.startswith("__"):
                # debug param: skip
                continue
            if module not in g:
                g[module] = {}
            if not param:
                if not v.startswith(GraphExecution.TYPE_PREFIX):
                    raise Exception("不支持的模块: %s" % v)
                g[module][GraphExecution.KEY_TYPE] = v[len(GraphExecution.TYPE_PREFIX) :]
                continue
            g[module][param] = v
        return g

    def remove_disabled(self, graph):
        disabled = set()
        queue = []
        for module, args in graph.items():
            if not args.get(GraphExecution.KEY_ENABLED, True):
                disabled.add(module)
                queue.append(module)
        i = 0
        while i < len(queue):
            for depender in graph[queue[i]][GraphExecution.KEY_DEPENDERS]:
                if depender in disabled:
                    continue
                disabled.add(depender)
                queue.append(depender)
            i += 1

        for module in disabled:
            del graph[module]

        for v in graph.values():
            v[GraphExecution.KEY_DEPENDEES] -= disabled
            v[GraphExecution.KEY_DEPENDERS] -= disabled

        return graph

    def build_graph(self):
        graph = self._to_dict(self.__data)
        parameters = self._to_dict(self.__parameters)

        # apply: *
        if GraphExecution.ALL in parameters:
            for module, args in graph.items():
                args.update(parameters[GraphExecution.ALL])
            del parameters[GraphExecution.ALL]

        # apply: parameters
        for module, args in parameters.items():
            graph[module].update(args)

        # build dependees and dependers, dependee的输出作为depender的输入
        for module, args in graph.items():
            args[GraphExecution.KEY_DEPENDEES] = set()
            args[GraphExecution.KEY_DEPENDERS] = set()

        for module, args in graph.items():
            for v in args.values():
                if not isinstance(v, Graph.OutputPort):
                    continue
                graph[module][GraphExecution.KEY_DEPENDEES].add(v.module)
                graph[v.module][GraphExecution.KEY_DEPENDERS].add(module)

        # remove disabled
        graph = self.remove_disabled(graph)

        self.__graph = graph

        # import pprint
        # pprint.pprint(graph)

    def run_module(self, module_type, args):
        from learning.module2.common.modulemanagerv2 import M

        return M[module_type](**args)

    def exec_module(self, module):
        args = self.__graph[module]
        module_type = args[GraphExecution.KEY_TYPE]

        module_args = {}
        for k, v in args.items():
            if k.startswith(GraphExecution.META_KEY_PREFIX):
                continue
            if isinstance(v, Graph.OutputPort):
                v = getattr(self.__graph[v.module][GraphExecution.KEY_OUTPUTS], v.parameter)
            module_args[k] = v

        outputs = self.run_module(module_type, module_args)
        args[GraphExecution.KEY_OUTPUTS] = outputs
        globals()[module] = outputs

    def _exec_graph(self):
        queue = []
        for module, args in self.__graph.items():
            if len(args[GraphExecution.KEY_DEPENDEES]) == 0:
                queue.append(module)
        i = 0
        while i < len(queue):
            # TODO: parallel execute
            self.exec_module(queue[i])
            for depender in self.__graph[queue[i]][GraphExecution.KEY_DEPENDERS]:
                self.__graph[depender][GraphExecution.KEY_DEPENDEES].remove(queue[i])
                if len(self.__graph[depender][GraphExecution.KEY_DEPENDEES]) == 0:
                    queue.append(depender)
            i += 1

    def run(self):
        self.build_graph()
        self._exec_graph()

        results = {}
        for module, args in self.__graph.items():
            results[module] = args[GraphExecution.KEY_OUTPUTS]

        return results


class GraphBase:
    DefaultGraphExecution = GraphExecution

    class OutputPort:
        def __init__(self, s):
            self.module, self.parameter = _split_module_and_param(s, True)


class Graph(GraphBase):
    def __init__(self, data):
        self.__data = data

    @property
    def data(self):
        return self.__data

    def run(self, parameters, execution_class=None):
        if execution_class is None:
            execution_class = GraphBase.DefaultGraphExecution

        return execution_class(copy.deepcopy(self.__data), copy.deepcopy(parameters)).run()


class GraphRun(GraphBase):
    def __init__(self, g, parameters, execution_class):
        self.__g = g
        self.base_parameters = parameters
        self.__execution_class = execution_class

    @property
    def data(self):
        return self.__g.data

    def run(self, parameters, execution_class=None):
        parameters = copy.deepcopy(parameters)
        if self.base_parameters:
            parameters.update(self.base_parameters)
        if self.__execution_class is not None:
            execution_class = self.__execution_class

        return self.__g.run(parameters, execution_class)


class GraphContinue(GraphBase):
    def __init__(self, g, run):
        self.__run = run
        self.base_graph = g

    @property
    def data(self):
        return self.base_graph.data

    def run(self, parameters, execution_class=None):
        g = GraphRun(self.base_graph, parameters, execution_class)
        return self.__run(g)


if __name__ == "__main__":
    # test code here
    g = Graph(
        {"m1": "M.use_datasource.v1", "m1.datasource_id": "abc", "m2": "M.use_datasource.v1", "m2.instruments": Graph.OutputPort("m1.instruments")}
    )
    r = g.run(
        {
            "*.__enabled__": False,
            "m1.__enabled__": True,
            "m1.datasource_id": "def",
        }
    )
    print(r)
    print(globals())
