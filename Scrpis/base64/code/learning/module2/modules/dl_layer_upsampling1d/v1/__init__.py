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
bigquant_category = r"深度学习\卷积层"
bigquant_friendly_name = "UpSampling1D层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    size: I.int("上采样因子，size") = 2, name: DL.PARAM_NAME = None, inputs: I.port("输入") = None
) -> [I.port("输出", "data"),]:
    """
    Upsampling layer for 1D inputs。在时间轴上，将每个时间步重复length次。输入形如（samples，steps，features）的3D张量，输出形如（samples，upsampled_steps，features）的3D张量。
    """

    from tensorflow.keras.layers import UpSampling1D

    layer = UpSampling1D(size=size)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
