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
bigquant_category = r"深度学习\池化层"
bigquant_friendly_name = "MaxPooling2D层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    pool_size: DL.param_int_list("pool_size", 2, "代表在两个方向（竖直，水平）上的下采样因子，如取（2，2）将使图片在两个维度上均变为原长的一半。为整数意为各个维度值相同且为该数字。") = "2,2",
    strides: DL.param_int_list("步长", 2, "strides，步长值，如果不设置，默认是pool_size") = None,
    padding: I.choice("padding", ["valid", "same"]) = "valid",
    data_format: I.choice(
        "通道维位置，data_format，代表图像的通道维的位置，该参数是Keras 1.x中的image_dim_ordering，“channels_last”对应原本的“tf”，“channels_first”对应原本的“th”。以128x128的RGB图像为例，“channels_first”应将数据组织为（3,128,128），而“channels_last”应将数据组织为（128,128,3）。该参数的默认值是~/.keras/keras.json中设置的值，若从未设置过，则为“channels_last”。",
        ["channels_last", "channels_first"],
    ) = "channels_last",
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    为空域信号施加最大值池化。
    """

    pool_size = smart_list(pool_size, sep=",", cast_type=int, drop_empty_lines=True) or None
    strides = smart_list(strides, sep=",", cast_type=int, drop_empty_lines=True) or None

    from tensorflow.keras.layers import MaxPooling2D

    layer = MaxPooling2D(pool_size=pool_size, strides=strides, padding=padding, data_format=data_format)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
