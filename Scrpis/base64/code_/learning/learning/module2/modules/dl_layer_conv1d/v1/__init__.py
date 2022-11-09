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
bigquant_friendly_name = "Conv1D层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    filters: I.int("卷积核数目，filters，即输出的维度", min=1),
    kernel_size: DL.PARAM_USER_KERNEL_SIZE,
    strides: DL.param_int_list("步长", None, "strides，卷积的步长") = "1",
    padding: I.choice(
        "padding，补0策略，为“valid”, “same” 或“causal”，“causal”将产生因果（膨胀的）卷积，即output[t]不依赖于input[t+1：]。当对不能违反时间顺序的时序信号建模时有用。参考WaveNet: A Generative Model for Raw Audio, section 2.1.。“valid”代表只进行有效的卷积，即对边界数据不处理。“same”代表保留边界处的卷积结果，通常会导致输出shape与输入shape相同。",
        ["valid", "causal", "same"],
    ) = "valid",
    dilation_rate: I.int("dilation_rate，整数，指定dilated convolution中的膨胀比例。任何不为1的dilation_rate均与任何不为1的strides均不兼容。") = 1,
    activation: DL.PARAM_ACTIVATION = "None",
    user_activation: DL.PARAM_USER_ACTIVATION = None,
    use_bias: I.bool("use_bias，是否使用偏置项") = True,
    kernel_initializer: I.choice(
        "权值初始化方法，kernel_initializer，使用的是使用默认参数",
        [
            "Zeros",
            "Ones",
            "Constant",
            "RandomNormal",
            "RandomUniform",
            "TruncatedNormal",
            "VarianceScaling",
            "Orthogonal",
            "Identiy",
            "lecun_uniform",
            "lecun_normal",
            "glorot_normal",
            "glorot_uniform",
            "he_normal",
            "he_uniform",
            DL.TEXT_USER,
        ],
    ) = "glorot_uniform",
    user_kernel_initializer: I.code(
        "自定义权值初始化方法，示例：\ndef bigquant_run(shape, dtype=None):\n    import tensorflow.keras.backend as K\n    return K.random_normal(shape, dtype=dtype)",
        I.code_python,
    ) = None,
    bias_initializer: I.choice(
        "偏置向量初始化方法，bias_initializer，使用的是使用默认参数",
        [
            "Zeros",
            "Ones",
            "Constant",
            "RandomNormal",
            "RandomUniform",
            "TruncatedNormal",
            "VarianceScaling",
            "Orthogonal",
            "Identiy",
            "lecun_uniform",
            "lecun_normal",
            "glorot_normal",
            "glorot_uniform",
            "he_normal",
            "he_uniform",
            DL.TEXT_USER,
        ],
    ) = "Zeros",
    user_bias_initializer: I.code(
        "自定义偏置向量初始化方法，示例：\ndef bigquant_run(shape, dtype=None):\n    import tensorflow.keras.backend as K\n    return K.random_normal(shape, dtype=dtype)",
        I.code_python,
    ) = None,
    kernel_regularizer: I.choice("权值正则项，kernel_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]) = DL.TEXT_NONE,
    kernel_regularizer_l1: I.float("权值正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    kernel_regularizer_l2: I.float("权值正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_kernel_regularizer: I.code(
        "自定义权值正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    bias_regularizer: I.choice("偏置向量正则项，bias_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]) = DL.TEXT_NONE,
    bias_regularizer_l1: I.float("偏置向量正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    bias_regularizer_l2: I.float("偏置向量正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_bias_regularizer: I.code(
        "自定义偏置向量正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    activity_regularizer: I.choice("输出正则项，activity_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]) = DL.TEXT_NONE,
    activity_regularizer_l1: I.float("输出正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    activity_regularizer_l2: I.float("输出正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_activity_regularizer: I.code(
        "自定义输出正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    kernel_constraint: I.choice(
        "权值约束项，kernel_constraint，在优化过程中为网络的参数施加约束", ["max_norm", "non_neg", "unit_norm", "min_max_norm", DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    user_kernel_constraint: I.code(
        "自定义权值约束项，示例：\ndef bigquant_run(w):\n    from keras.constraints import max_norm\n    return max_norm(2.)(w)", I.code_python
    ) = None,
    bias_constraint: I.choice(
        "偏置向量约束项，bias_constraint，在优化过程中为网络的参数施加约束", ["max_norm", "non_neg", "unit_norm", "min_max_norm", DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    user_bias_constraint: I.code(
        "自定义偏置向量约束项，示例：\ndef bigquant_run(w):\n    from keras.constraints import max_norm\n    return max_norm(2.)(w)", I.code_python
    ) = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
        一维卷积层（即时域卷积），用以在一维输入信号上进行邻域滤波。当使用该层作为首层时，需要提供关键字参数input_shape。例如(10,128)代表一个长为10的序列，序列中每个信号为128向量。而(None, 128)代表变长的128维向量序列。
    该层生成将输入信号与卷积核按照单一的空域（或时域）方向进行卷积。如果use_bias=True，则还会加上一个偏置项，若activation不为None，则输出为经过激活函数的输出。
    """

    kernel_size = smart_list(kernel_size, sep=",", cast_type=int, drop_empty_lines=True) or None
    strides = smart_list(strides, sep=",", cast_type=int, drop_empty_lines=True) or None
    activation = DL.smart_choice(activation, user_activation)
    kernel_initializer = DL.smart_choice(kernel_initializer, user_kernel_initializer)
    bias_initializer = DL.smart_choice(bias_initializer, user_bias_initializer)
    kernel_regularizer = DL.smart_regularizer_choice(kernel_regularizer, user_kernel_regularizer, kernel_regularizer_l1, kernel_regularizer_l2)
    bias_regularizer = DL.smart_regularizer_choice(bias_regularizer, user_bias_regularizer, bias_regularizer_l1, bias_regularizer_l2)
    activity_regularizer = DL.smart_regularizer_choice(
        activity_regularizer, user_activity_regularizer, activity_regularizer_l1, activity_regularizer_l2
    )
    kernel_constraint = DL.smart_choice(kernel_constraint, user_kernel_constraint)
    bias_constraint = DL.smart_choice(bias_constraint, user_bias_constraint)

    from tensorflow.keras.layers import Conv1D

    layer = Conv1D(
        filters=filters,
        kernel_size=kernel_size,
        strides=strides,
        padding=padding,
        dilation_rate=dilation_rate,
        activation=activation,
        use_bias=use_bias,
        kernel_initializer=kernel_initializer,
        bias_initializer=bias_initializer,
        kernel_regularizer=kernel_regularizer,
        bias_regularizer=bias_regularizer,
        activity_regularizer=activity_regularizer,
        kernel_constraint=kernel_constraint,
        bias_constraint=bias_constraint,
    )
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
