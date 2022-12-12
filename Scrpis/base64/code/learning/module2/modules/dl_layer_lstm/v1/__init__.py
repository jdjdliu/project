import learning.module2.common.dlutils as DL
import learning.module2.common.interface as I
from learning.module2.common.data import Outputs

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
bigquant_category = r"深度学习\循环层"
bigquant_friendly_name = "LSTM层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    units: I.int("输出空间维度，units，正整数", min=1),
    activation: DL.PARAM_ACTIVATION = "tanh",
    user_activation: DL.PARAM_USER_ACTIVATION = None,
    recurrent_activation: I.choice(
        "recurrent激活函数，激活函数使用的是使用默认参数，如果需要修改参数，可以使用recurrent自定义激活函数",
        ["softmax", "elu", "selu", "softplus", "softsign", "relu", "tanh", "sigmoid", "hard_sigmoid", "linear", DL.TEXT_NONE, DL.TEXT_USER],
    ) = "sigmoid",
    user_recurrent_activation: I.code(
        "recurrent自定义激活函数，示例：\ndef bigquant_run(x):\n    import tensorflow as tf\n    return tf.atan(x)", I.code_python
    ) = None,
    use_bias: I.bool("是否使用偏置项，use_bias") = True,
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
    recurrent_initializer: I.choice(
        "循环核初始化方法，recurrent_initializer，使用的是使用默认参数",
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
    ) = "Orthogonal",
    user_recurrent_initializer: I.code(
        "自定义循环核初始化方法，示例：\ndef bigquant_run(shape, dtype=None):\n    import tensorflow.keras.backend as K\n    return K.random_normal(shape, dtype=dtype)",
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
    unit_forget_bias: I.bool(
        'unit_forget_bias，Boolean. If True, add 1 to the bias of the forget gate at initialization. Setting it to true will also force  bias_initializer="Zeros". This is recommended in Jozefowicz et al.'
    ) = True,
    kernel_regularizer: I.choice("权值正则项，kernel_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]) = DL.TEXT_NONE,
    kernel_regularizer_l1: I.float("权值正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    kernel_regularizer_l2: I.float("权值正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_kernel_regularizer: I.code(
        "自定义权值正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    recurrent_regularizer: I.choice(
        "循环核正则项，recurrent_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    recurrent_regularizer_l1: I.float("循环核正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    recurrent_regularizer_l2: I.float("循环核正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_recurrent_regularizer: I.code(
        "自定义循环核正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
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
    recurrent_constraint: I.choice(
        "循环核约束项，recurrent_constraint，在优化过程中为网络的参数施加约束", ["max_norm", "non_neg", "unit_norm", "min_max_norm", DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    user_recurrent_constraint: I.code(
        "自定义循环核约束项，示例：\ndef bigquant_run(w):\n    from keras.constraints import max_norm\n    return max_norm(2.)(w)", I.code_python
    ) = None,
    bias_constraint: I.choice(
        "偏置向量约束项，bias_constraint，在优化过程中为网络的参数施加约束", ["max_norm", "non_neg", "unit_norm", "min_max_norm", DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    user_bias_constraint: I.code(
        "自定义偏置向量约束项，示例：\ndef bigquant_run(w):\n    from keras.constraints import max_norm\n    return max_norm(2.)(w)", I.code_python
    ) = None,
    dropout: I.float("dropout，0~1之间的浮点数，控制输入线性变换的神经元断开比例", 0, 1) = 0,
    recurrent_dropout: I.float("recurrent_dropout，0~1之间的浮点数，控制循环状态的线性变换的神经元断开比例", 0, 1) = 0,
    return_sequences: I.bool("返回整个输出序列，若为True则返回整个序列，否则仅返回输出序列的最后一个输出") = False,
    implementation: I.choice(
        "implementation, 1或2，如果设为1，则RNN将以更多但更小的矩阵乘法实现，因此在CPU上运行更慢，在GPU上运行更快，并且消耗更少的内存。如果设为2，则RNN将以更少但是更大的矩阵乘法实现，因此在CPU上运行更快，但消耗更多的内存。", ["1", "2"]
    ) = "2",
    name: I.str("层名，名字, name, 可选，层的名字，在模型里不能有两个层同名。如果不指定，将自动分配名字") = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
    LSTM层，长短期记忆模型，RNN的一种，可以解决梯度爆炸的问题。关于此算法的详情，请参考 [教程](http://deeplearning.net/tutorial/lstm.html)
    - [Long short-term memory](http://deeplearning.cs.cmu.edu/pdfs/Hochreiter97_lstm.pdf) (original 1997 paper)
    - [Learning to forget: Continual prediction with LSTM](http://www.mitpressjournals.org/doi/pdf/10.1162/089976600300015015)
    - [Supervised sequence labeling with recurrent neural networks](http://www.cs.toronto.edu/~graves/preprint.pdf)
    - [A Theoretically Grounded Application of Dropout in Recurrent Neural Networks](http://arxiv.org/abs/1512.05287)
    """

    activation = DL.smart_choice(activation, user_activation)
    recurrent_activation = DL.smart_choice(recurrent_activation, user_recurrent_activation)
    kernel_initializer = DL.smart_choice(kernel_initializer, user_kernel_initializer)
    recurrent_initializer = DL.smart_choice(recurrent_initializer, user_recurrent_initializer)
    bias_initializer = DL.smart_choice(bias_initializer, user_bias_initializer)
    kernel_regularizer = DL.smart_regularizer_choice(kernel_regularizer, user_kernel_regularizer, kernel_regularizer_l1, kernel_regularizer_l2)
    recurrent_regularizer = DL.smart_regularizer_choice(
        recurrent_regularizer, user_recurrent_regularizer, recurrent_regularizer_l1, recurrent_regularizer_l2
    )
    bias_regularizer = DL.smart_regularizer_choice(bias_regularizer, user_bias_regularizer, bias_regularizer_l1, bias_regularizer_l2)
    activity_regularizer = DL.smart_regularizer_choice(
        activity_regularizer, user_activity_regularizer, activity_regularizer_l1, activity_regularizer_l2
    )
    kernel_constraint = DL.smart_choice(kernel_constraint, user_kernel_constraint)
    recurrent_constraint = DL.smart_choice(recurrent_constraint, user_recurrent_constraint)
    bias_constraint = DL.smart_choice(bias_constraint, user_bias_constraint)
    implementation = int(implementation)

    from tensorflow.keras.layers import LSTM

    layer = LSTM(
        units=units,
        activation=activation,
        recurrent_activation=recurrent_activation,
        use_bias=use_bias,
        kernel_initializer=kernel_initializer,
        recurrent_initializer=recurrent_initializer,
        bias_initializer=bias_initializer,
        unit_forget_bias=unit_forget_bias,
        kernel_regularizer=kernel_regularizer,
        recurrent_regularizer=recurrent_regularizer,
        bias_regularizer=bias_regularizer,
        activity_regularizer=activity_regularizer,
        kernel_constraint=kernel_constraint,
        recurrent_constraint=recurrent_constraint,
        bias_constraint=bias_constraint,
        dropout=dropout,
        recurrent_dropout=recurrent_dropout,
        return_sequences=return_sequences,
        implementation=implementation,
    )
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
