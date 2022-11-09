import numpy as np


def MinMaxNorm(x):
    """MinMax Normalization, 最小最大值标准化至[0,1]范围"""
    min_val = np.min(x)
    max_val = np.max(x)

    # 最大最小值相同时，分母为0
    if min_val != max_val:
        x = (x - min_val) / (max_val - min_val)
    return x


def ZScoreNorm(x):
    """ZScore Normalization, Z分数标准化至标准正态分布，即对原始数据减去均值除以标准差"""

    return (x - x.mean()) / x.std()


def RobustZScoreNorm(x):
    """Robust ZScore Normalization, 稳健Z分数标准化，即对原始数据减去中位数除以1.48倍MAD统计量

    Use robust statistics for Z-Score normalization:
        mean(x) = median(x)
        std(x) = MAD(x) * 1.4826

    Reference:
        https://en.wikipedia.org/wiki/Median_absolute_deviation.

    """
    EPS = 1e-12
    mean_train = np.median(x)
    # mad统计量
    std_train = np.median(np.abs(x - mean_train))
    std_train += EPS
    std_train *= 1.4826

    x -= mean_train
    x / std_train

    return x


def CSZScoreNorm(x):
    """Cross Sectional ZScore Normalization, 截面Z分数标准化至标准正态分布
    Note:
        在数据标准化模块，默认是在截面数据上进行的标准化，以避免全局标准化导致的未来函数。
    """
    return (x - x.mean()) / x.std()


def CSRankNorm(x):
    """Cross Sectional Rank Normalization, 截面先转换为rank序数，再Z分数化至标准正态分布"""
    # 调用numpy.array排名
    array = x
    temp = array.argsort()
    ranks = temp.argsort()

    # ZScoreNorm
    return ZScoreNorm(ranks)
