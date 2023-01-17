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
bigquant_friendly_name = "ELU层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    alpha: I.float("alpha，scale for the negative factor，控制负因子的参数") = 1.0, name: DL.PARAM_NAME = None, inputs: I.port("输入") = None
) -> [I.port("输出", "data"),]:
    """
    ELU层是指数线性单元（Exponential Linera Unit），表达式为： 该层为参数化的ReLU（Parametric ReLU），表达式是：f(x) = alpha * (exp(x) - 1.) for x < 0, f(x) = x for x>=0
    """

    from tensorflow.keras.layers import ELU

    layer = ELU(alpha=alpha)
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
