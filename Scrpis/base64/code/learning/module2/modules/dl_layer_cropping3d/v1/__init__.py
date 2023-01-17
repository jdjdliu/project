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
bigquant_category = r"深度学习\卷积层"
bigquant_friendly_name = "Cropping3D层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    cropping: I.str("cropping，长为6的列表，2个一组，分别为三个方向上头部与尾部需要裁剪掉的元素数。列表用英文逗号(,)分隔的整数列表，例如 10 或者 32,24") = "1,1,1,1,1,1",
    data_format: I.choice(
        "通道维位置，data_format，代表图像的通道维的位置，该参数是Keras 1.x中的image_dim_ordering，“channels_last”对应原本的“tf”，“channels_first”对应原本的“th”。以128x128的RGB图像为例，“channels_first”应将数据组织为（3,128,128），而“channels_last”应将数据组织为（128,128,3）。该参数的默认值是~/.keras/keras.json中设置的值，若从未设置过，则为“channels_last”。",
        ["channels_last", "channels_first"],
    ) = "channels_last",
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    对3D输入（图像）进行裁剪 (e.g. spatial or spatio-temporal).
    """

    cropping = smart_list(cropping, sep=",", cast_type=int, drop_empty_lines=True) or None
    if len(cropping) != 6:
        raise Exception("cropping：必须为6个整数")
    cropping = ((cropping[0], cropping[1]), (cropping[2], cropping[3]), (cropping[4], cropping[5]))

    from tensorflow.keras.layers import Cropping3D

    layer = Cropping3D(cropping=cropping, data_format=data_format)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
