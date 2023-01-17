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
bigquant_friendly_name = "激活层(Activation)"
bigquant_doc_url = "https://bigquant.com/docs/"


DEFAULT_FUNCTION = """def bigquant_run(x):
    # x为输入，即上一层的输出
    # 在这里添加您的代码
    return x + 1
"""


def bigquant_run(
    activation: DL.PARAM_ACTIVATION = "tanh",
    user_activation: DL.PARAM_USER_ACTIVATION = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    激活层，对一个层的输出施加激活函数
    """

    activation = DL.smart_choice(activation, user_activation)

    from tensorflow.keras.layers import Activation

    layer = Activation(activation)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
