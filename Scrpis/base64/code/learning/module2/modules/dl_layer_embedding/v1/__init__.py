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
bigquant_category = r"深度学习\嵌入层"
bigquant_friendly_name = "Embedding层"
bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    input_dim: I.int("input_dim，字典长度，即输入数据最大下标+1", min=1),
    output_dim: I.int("output_dim，全连接嵌入的维度"),
    embeddings_initializer: DL.param_initializer("embeddings_initializer", "嵌入矩阵初始化") = "uniform",
    user_embeddings_initializer: DL.param_user_initializer("嵌入矩阵初始化") = None,
    embeddings_regularizer: I.choice(
        "嵌入矩阵正则项，embeddings_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    embeddings_regularizer_l1: I.float("嵌入矩阵正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    embeddings_regularizer_l2: I.float("嵌入矩阵正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_embeddings_regularizer: I.code(
        "自定义嵌入矩阵正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    activity_regularizer: I.choice("输出正则项，activity_regularizer，如果使用L1L2，可配置如下L1，L2参数", [DL.TEXT_REG_L1L2, DL.TEXT_NONE, DL.TEXT_USER]) = DL.TEXT_NONE,
    activity_regularizer_l1: I.float("输出正则项L1，如果使用L1L2正则化，此参数有效") = 0,
    activity_regularizer_l2: I.float("输出正则项L2，如果使用L1L2正则化，此参数有效") = 0,
    user_activity_regularizer: I.code(
        "自定义输出正则项，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))",
        I.code_python,
    ) = None,
    embeddings_constraint: I.choice(
        "嵌入矩阵约束项，embeddings_constraint，在优化过程中为网络的参数施加约束", ["max_norm", "non_neg", "unit_norm", "min_max_norm", DL.TEXT_NONE, DL.TEXT_USER]
    ) = DL.TEXT_NONE,
    user_embeddings_constraint: I.code(
        "自定义嵌入矩阵约束项，示例：\ndef bigquant_run(w):\n    from keras.constraints import max_norm\n    return max_norm(2.)(w)", I.code_python
    ) = None,
    mask_zero: I.bool(
        "mask_zero，确定是否将输入中的‘0’看作是应该被忽略的‘填充’（padding）值，该参数在使用递归层处理变长输入时有用。设置为True的话，模型中后续的层必须都支持masking，否则会抛出异常。如果该值为True，则下标0在字典中不可用，input_dim应设置为|vocabulary| + 1"
    ) = False,
    input_length: I.int("input_length，当输入序列的长度固定时，该值为其长度。如果要在该层后接Flatten层，然后接Dense层，则必须指定该参数，否则Dense层的输出维度无法自动推断") = None,
    name: DL.PARAM_NAME = None,
    inputs: I.port("输入") = None,
) -> [I.port("输出", "data"),]:
    """
        嵌入层将正整数（下标）转换为具有固定大小的向量，如[[4],[20]]->[[0.25,0.1],[0.6,-0.2]]
    Embedding层只能作为模型的第一层
    """

    embeddings_initializer = DL.smart_choice(embeddings_initializer, user_embeddings_initializer)
    embeddings_regularizer = DL.smart_regularizer_choice(
        embeddings_regularizer, user_embeddings_regularizer, embeddings_regularizer_l1, embeddings_regularizer_l2
    )
    activity_regularizer = DL.smart_regularizer_choice(
        activity_regularizer, user_activity_regularizer, activity_regularizer_l1, activity_regularizer_l2
    )
    embeddings_constraint = DL.smart_choice(embeddings_constraint, user_embeddings_constraint)

    from tensorflow.keras.layers import Embedding

    layer = Embedding(
        input_dim=input_dim,
        output_dim=output_dim,
        embeddings_initializer=embeddings_initializer,
        embeddings_regularizer=embeddings_regularizer,
        activity_regularizer=activity_regularizer,
        embeddings_constraint=embeddings_constraint,
        mask_zero=mask_zero,
        input_length=input_length,
    )
    layer = DL.post_build_layer(layer, name, inputs)

    return Outputs(data=layer)


if __name__ == "__main__":
    # 测试代码
    pass
