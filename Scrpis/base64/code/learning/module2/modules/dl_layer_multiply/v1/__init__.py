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
bigquant_category = r"深度学习\融合层"
bigquant_friendly_name = "Multiply层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    input1: I.port("输入1"), input2: I.port("输入2"), input3: I.port("输入3", optional=True) = None, name: DL.PARAM_NAME = None
) -> [I.port("输出", "data"),]:
    """
    该层接收一个列表的同shape张量，并返回它们的逐元素积的张量，shape不变。
    """

    from tensorflow.keras.layers import Multiply

    layer = Multiply()
    layer = DL.post_build_layer(layer, name, [input1, input2, input3])

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
