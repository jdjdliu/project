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
bigquant_friendly_name = "Lambda层"
bigquant_doc_url = "https://bigquant.com/docs/"


DEFAULT_FUNCTION = """def bigquant_run(x):
    # x为输入，即上一层的输出
    # 在这里添加您的代码
    return x + 1
"""


def bigquant_run(
    function: I.code("函数，求值函数，以输入张量作为参数", I.code_python, default=DEFAULT_FUNCTION), name: DL.PARAM_NAME = None, inputs: I.port("输入") = None
) -> [I.port("输出", "data"),]:
    """
    Lambda层，实现将任意函数/表达式封装为Layer/层。
    """

    from tensorflow.keras.layers import Lambda

    layer = Lambda(function)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
