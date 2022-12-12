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
bigquant_friendly_name = "Cropping1D层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    cropping: I.str("cropping，长为2的tuple，指定在序列的首尾要裁剪掉多少个元素，用英文逗号(,)分隔的整数列表，例如 10 或者 32,24") = "1,1",
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    在时间轴（axis1）上对1D输入（即时间序列）进行裁剪
    """

    cropping = smart_list(cropping, sep=",", cast_type=int, drop_empty_lines=True) or None

    from tensorflow.keras.layers import Cropping1D

    layer = Cropping1D(cropping=cropping)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
