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
bigquant_category = r"深度学习\高级激活层"
bigquant_friendly_name = "PReLU层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    alpha_initializer: DL.param_initializer("alpha_initializer", "alpha初始化") = "zeros",
    user_alpha_initializer: DL.param_user_initializer("alpha初始化") = None,
    alpha_regularizer: DL.param_regularizer("alpha_regularizer", "alpha正则项") = DL.TEXT_NONE,
    alpha_regularizer_l1: DL.param_regularizer_l1("alpha正则项") = 0,
    alpha_regularizer_l2: DL.param_regularizer_l2("alpha正则项") = 0,
    user_alpha_regularizer: DL.param_user_regularizer("alpha正则项") = None,
    alpha_constraint: DL.param_constraint("alpha_constraint", "alpha约束项") = DL.TEXT_NONE,
    user_alpha_constraint: DL.param_user_constraint("alpha约束项") = None,
    shared_axes: DL.param_int_list(
        "共享轴",
        None,
        "shared_axes，该参数指定的轴将共享同一组科学系参数，例如假如输入特征图是从2D卷积过来的，具有形如(batch, height, width, channels)这样的shape，则或许你会希望在空域共享参数，这样每个filter就只有一组参数，设定shared_axes=[1,2]可完成该目标",
    ) = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    该层为参数化的ReLU（Parametric ReLU），表达式是：f(x) = alpha * x for x < 0, f(x) = x for x>=0，此处的alpha为一个与xshape相同的可学习的参数向量。
    """

    alpha_initializer = DL.smart_choice(alpha_initializer, user_alpha_initializer)
    alpha_regularizer = DL.smart_regularizer_choice(alpha_regularizer, user_alpha_regularizer, alpha_regularizer_l1, alpha_regularizer_l2)
    alpha_constraint = DL.smart_choice(alpha_constraint, user_alpha_constraint)
    shared_axes = smart_list(shared_axes, sep=",", cast_type=int, drop_empty_lines=True) or None

    from tensorflow.keras.layers import PReLU

    layer = PReLU(
        alpha_initializer=alpha_initializer, alpha_regularizer=alpha_regularizer, alpha_constraint=alpha_constraint, shared_axes=shared_axes
    )
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
