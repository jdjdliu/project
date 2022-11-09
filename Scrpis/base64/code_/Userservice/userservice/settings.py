from sdk.common import OSEnv
from sdk.db import sql

API_PREFIX = "/api/userservice"

TORTOISE_ORM = sql.build_config(sql.TORTOISE_ORM, enabled_apps=["userservice"])

WEB_HOST = OSEnv.str("WEB_HOST", "")
GATEWAY_HOST = OSEnv.str("GATEWAY_HOST", "")

REDIS_USER_URL = OSEnv.str("REDIS_USER_URL", default="redis://localhost", description="链接大财富用户数据 Redis")
REDIS_USER_CLUSTER_MODE = OSEnv.bool("REDIS_USER_CLUSTER_MODE", default=False, description="链接大财富用户数据 Redis")


category_orders_str = r"""
数据输入输出:-10000
数据处理
数据标注
特征抽取
机器学习
机器学习\分类
机器学习\回归
机器学习\排序
机器学习\聚类
机器学习\特征预处理
机器学习\其他
深度学习\模型
深度学习\包装器
深度学习\卷积层
深度学习\噪声层
深度学习\局部连接层
深度学习\嵌入层
深度学习\常用层
深度学习\循环层
深度学习\数据处理
深度学习\池化层
深度学习\融合层
深度学习\规范层
深度学习\高级激活层
深度学习\自定义层
模型评估
回测与交易
回测与交易\初始化函数
回测与交易\交易函数
量化分析
策略池
高级优化
高级运行
自定义模块
用户模块:10000
示例模块
"""


def _build_category_orders(category_orders_str: str) -> dict:
    categories = {}
    order = 0
    for c in category_orders_str.split():
        c = c.strip()
        if not c:
            continue
        cc = c.split(":")
        if len(cc) > 1:
            # 数据输入输出:10000
            order = int(cc[1])

        categories[cc[0]] = order
        order += 1
    return categories


CATEGORY_ORDERS = _build_category_orders(category_orders_str)
