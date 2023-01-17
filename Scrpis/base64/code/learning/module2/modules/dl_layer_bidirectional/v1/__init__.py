from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
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
bigquant_category = r"深度学习\包装器"
bigquant_friendly_name = "Bidirectional层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    layer: I.port("输入层"),
    merge_mode: I.choice(
        "merge_mode，Mode by which outputs of the forward and backward RNNs will be combined. 前向和后向RNN输出的结合方式，为sum,mul,concat,ave和None之一，若设为None，则返回值不结合，而是以列表的形式返回",
        ["sum", "mul", "concat", "ave", DL.TEXT_NONE],
    ) = DL.TEXT_NONE,
    weights: I.str("weights，列表，列表用英文逗号(,)分隔，例如 1,2") = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    双向RNN包装器
    """

    weights = smart_list(weights, sep=",", cast_type=float, drop_empty_lines=True) or None

    from tensorflow.keras.layers import Bidirectional

    layer = Bidirectional(layer=layer, merge_mode=merge_mode, weights=weights)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
