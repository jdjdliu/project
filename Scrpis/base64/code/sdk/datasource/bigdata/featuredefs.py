import numpy as np


class Transformer:
    def __init__(self, expr_s):
        self.__expr_s = expr_s
        self.__expr = None

    def __call__(self, x):
        if self.__expr_s is None:
            return x

        import numexpr as ne

        if self.__expr is None:
            # accelerate with numexpr
            s = self.__expr_s
            if "replace" not in s and "clip" not in s and "ne." not in s:
                s = s.replace("np.", "")
                s = s.replace("lambda x: ", "")
                self.__expr = lambda x: ne.evaluate(s)
            else:
                self.__expr = eval(s, {"ne": ne, "np": np}, {})
        return self.__expr(x)

    def __repr__(self):
        return str(self.__expr_s)
