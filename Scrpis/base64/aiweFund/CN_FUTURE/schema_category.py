"""
用于管理期货的目录分类以及排序信息，既 schema中的category 和 rank字段
以免每次修改整个分类的信息都要去各个脚本中修改
"""
import os
import sys

BASE_DIR = os.path.dirname((os.path.dirname(os.path.abspath(__file__))))
# print(BASE_DIR)
sys.path.append(BASE_DIR)
# 原表:原表的schema信息暂时放在一个文件CN_FUTURE/schema/__init__.py中,这里不再统一管理
SOURCE_TABLE = {}

# bigquant表
#
# 1.基本数据
basic_info_CN_FUTURE = ('期货/基本信息', 1001001, "期货基本信息")
dominant_CN_FUTURE = ('期货/基本信息', 1001002, "期货主力与连续合约")
instruments_CN_FUTURE = ('期货/基本信息', 1001003, "期货每日合约列表")


# 2.行情数据
bar1d_CN_FUTURE = ('期货/行情数据', 2001001, "期货日线行情")

if __name__ == "__main__":
    print(BASE_DIR)