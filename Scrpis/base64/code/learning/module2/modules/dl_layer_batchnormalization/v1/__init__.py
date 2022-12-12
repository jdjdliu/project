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
bigquant_category = r"深度学习\规范层"
bigquant_friendly_name = "BatchNormalization层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    axis: I.int('需要规范化的轴，axis，指定要规范化的轴，通常为特征轴。例如在进行data_format="channels_first的2D卷积后，一般会设axis=1') = -1,
    momentum: I.float("动量，momentum，动态均值的动量") = 0.99,
    epsilon: I.float("epsilon，大于0的小浮点数，用于防止除0错误") = 0.001,
    center: I.bool("中心化，center，若设为True，将会将beta作为偏置加上去，否则忽略参数beta") = True,
    scale: I.bool("规范化，scale，若设为True，则会乘以gamma，否则不使用gamma。当下一层是线性的时，可以设False，因为scaling的操作将被下一层执行。") = True,
    beta_initializer: DL.param_initializer("beta_initializer", "beta初始化") = "zeros",
    user_beta_initializer: DL.param_user_initializer("beta初始化") = None,
    gamma_initializer: DL.param_initializer("gamma_initializer", "gamma初始化") = "ones",
    user_gamma_initializer: DL.param_user_initializer("gamma初始化") = None,
    moving_mean_initializer: DL.param_initializer("moving_mean_initializer", "moving_mean初始化") = "zeros",
    user_moving_mean_initializer: DL.param_user_initializer("moving_mean初始化") = None,
    moving_variance_initializer: DL.param_initializer("moving_variance_initializer", "moving_variance初始化") = "ones",
    user_moving_variance_initializer: DL.param_user_initializer("moving_variance初始化") = None,
    beta_regularizer: DL.param_regularizer("beta_regularizer", "beta正则项") = DL.TEXT_NONE,
    beta_regularizer_l1: DL.param_regularizer_l1("beta正则项") = 0,
    beta_regularizer_l2: DL.param_regularizer_l2("beta正则项") = 0,
    user_beta_regularizer: DL.param_user_regularizer("beta正则项") = None,
    gamma_regularizer: DL.param_regularizer("gamma_regularizer", "gamma正则项") = DL.TEXT_NONE,
    gamma_regularizer_l1: DL.param_regularizer_l1("gamma正则项") = 0,
    gamma_regularizer_l2: DL.param_regularizer_l2("gamma正则项") = 0,
    user_gamma_regularizer: DL.param_user_regularizer("gamma正则项") = None,
    beta_constraint: DL.param_constraint("beta_constraint", "beta约束项") = DL.TEXT_NONE,
    user_beta_constraint: DL.param_user_constraint("beta约束项") = None,
    gamma_constraint: DL.param_constraint("gamma_constraint", "gamma约束项") = DL.TEXT_NONE,
    user_gamma_constraint: DL.param_user_constraint("gamma约束项") = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
        该层在每个batch上将前一层的激活值重新规范化，使其输出数据的均值接近0，其标准差接近1。
    BN层的作用：加速收敛；控制过拟合，可以少用或不用Dropout和正则；降低网络对初始化权重不敏感；允许使用较大的学习率。
    """

    beta_initializer = DL.smart_choice(beta_initializer, user_beta_initializer)
    gamma_initializer = DL.smart_choice(gamma_initializer, user_gamma_initializer)
    moving_mean_initializer = DL.smart_choice(moving_mean_initializer, user_moving_mean_initializer)
    moving_variance_initializer = DL.smart_choice(moving_variance_initializer, user_moving_variance_initializer)
    beta_regularizer = DL.smart_regularizer_choice(beta_regularizer, user_beta_regularizer, beta_regularizer_l1, beta_regularizer_l2)
    gamma_regularizer = DL.smart_regularizer_choice(gamma_regularizer, user_gamma_regularizer, gamma_regularizer_l1, gamma_regularizer_l2)
    beta_constraint = DL.smart_choice(beta_constraint, user_beta_constraint)
    gamma_constraint = DL.smart_choice(gamma_constraint, user_gamma_constraint)

    from tensorflow.keras.layers import BatchNormalization

    layer = BatchNormalization(
        axis=axis,
        momentum=momentum,
        epsilon=epsilon,
        center=center,
        scale=scale,
        beta_initializer=beta_initializer,
        gamma_initializer=gamma_initializer,
        moving_mean_initializer=moving_mean_initializer,
        moving_variance_initializer=moving_variance_initializer,
        beta_regularizer=beta_regularizer,
        gamma_regularizer=gamma_regularizer,
        beta_constraint=beta_constraint,
        gamma_constraint=gamma_constraint,
    )
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
