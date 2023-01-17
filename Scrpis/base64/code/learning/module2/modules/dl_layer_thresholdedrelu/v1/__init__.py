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
bigquant_category = r"深度学习\高级激活层"
bigquant_friendly_name = "ThresholdedReLU层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    theta: I.float("theta，Threshold location of activation，激活门限位置", min=0) = 1.0, name: DL.PARAM_NAME = None, inputs: I.port("输入") = None
) -> [I.port("输出", "data"),]:
    """
    该层是带有门限的ReLU，表达式是：f(x) = x for x > theta,f(x) = 0 otherwise。
    """

    from tensorflow.keras.layers import ThresholdedReLU

    layer = ThresholdedReLU(theta=theta)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
