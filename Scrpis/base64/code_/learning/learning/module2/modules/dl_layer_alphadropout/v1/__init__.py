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
bigquant_category = r"深度学习\噪声层"
bigquant_friendly_name = "AlphaDropout层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    rate: I.float("dropout率，断连概率,与Dropout层相同"),
    noise_shape: DL.param_int_list("noise_shape", None, "noise_shape") = None,
    seed: I.int("随机数种子，整数") = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    对输入施加Alpha Dropout。Alpha Dropout是一种保持输入均值和方差不变的Dropout，该层的作用是即使在dropout时也保持数据的自规范性。 通过随机对负的饱和值进行激活，Alphe Drpout与selu激活函数配合较好。
    """

    noise_shape = smart_list(noise_shape, sep=",", cast_type=int, drop_empty_lines=True) or None

    from tensorflow.keras.layers import AlphaDropout

    layer = AlphaDropout(rate=rate, noise_shape=noise_shape, seed=seed)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
