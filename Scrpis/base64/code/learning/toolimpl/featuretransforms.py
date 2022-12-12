import numpy as np


class TransformBase:
    def __init__(self):
        self.__next_transform = None
        pass

    def transfrom(self, s):
        raise Exception('not implemented')
        pass

    def process(self, s):
        s = self.transfrom(s)
        if self.__next_transform:
            s = self.__next_transform.process(s)
        return s

    def then(self, next_transform):
        self.__next_transform = next_transform


class TransformLinear(TransformBase):
    def __init__(self, slope, intercept):
        self.__slope = slope
        self.__intercept = intercept

    def transfrom(self, s):
        return s * self.__slope + self.__intercept


class TransformInt(TransformBase):
    def transfrom(self, s):
        return s.astype(np.int32)


def get_stock_ranker_default_transforms():
    return [
        ('rank_.*', '(f * 10000).astype(np.int32)')
    ]
