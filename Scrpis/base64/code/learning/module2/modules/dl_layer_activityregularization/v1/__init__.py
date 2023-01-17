from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
import learning.module2.common.dlutils as DL


# 是否自动缓存结果
bigquant_cacheable = False

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = r"深度学习\常用层"
bigquant_friendly_name = "ActivityRegularization层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    l1: I.float("L1范数，l1，L1范数正则因子（正浮点数）", min=0) = 0.0,
    l2: I.float("L2范数，l2，L2范数正则因子（正浮点数）", min=0) = 0.0,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    ActivityRegularizer层，经过本层的数据不会有任何变化，但会基于其激活值更新损失函数值，可设置L1/L2范数更新损失函数。
    """

    from tensorflow.keras.layers import ActivityRegularization

    layer = ActivityRegularization(l1=l1, l2=l2)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
