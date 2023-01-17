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
bigquant_friendly_name = "Reshape层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    target_shape: I.str("target_shape, 目标shape，不包含样本数目的维度（batch大小），用英文逗号(,)分隔的整数列表，例如 32,24，表示输入数据是一系列的 32x24的矩阵"),
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    任意，但输入的shape必须固定。当使用该层为模型首层时，需要指定input_shape参数
    """
    target_shape = [int(x) for x in target_shape.split(",")]
    from tensorflow.keras.layers import Reshape

    layer = Reshape(target_shape=target_shape)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
