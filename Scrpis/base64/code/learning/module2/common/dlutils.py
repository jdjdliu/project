import re
import uuid

import learning.module2.common.interface as I

_BIGQUANT_TEMP_NAME_RE = re.compile(r"BIGQUANT_TEMP_NAME_[\da-f]+__")

TEXT_NONE = "None"
TEXT_USER = "自定义"
TEXT_REG_L1L2 = "L1L2"


def param_int_list(name, length, desc):
    if length is not None:
        example = ",".join(["2"] * length)
        len_desc = "长为 %s 的" % length
    else:
        example = "12,34"
        len_desc = ""
    return I.str("%s，%s。%s整数列表，列表用英文逗号(,)分隔，例如 %s" % (name, desc, len_desc, example))


def param_initializer(param_name, display_name):
    return I.choice(
        "%s，%s，本选项里的函数使用的是默认参数，如果需要使用自定义参数或者其他函数，可以选择自定义，然后在下面输入自定义函数" % (display_name, param_name),
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
            TEXT_USER,
        ],
    )


def param_user_initializer(display_name):
    return I.code(
        "自定义%s，示例：\ndef bigquant_run(shape, dtype=None):\n    import tensorflow.keras.backend as K\n    return K.random_normal(shape, dtype=dtype)"
        % display_name,
        I.code_python,
    )


def param_regularizer(param_name, display_name):
    return I.choice("%s，%s，如果使用L1L2，可配置如下L1，L2参数" % (display_name, param_name), [TEXT_REG_L1L2, TEXT_NONE, TEXT_USER])


def param_regularizer_l1(display_name):
    return I.float("%s L1，如果使用L1L2正则化，此参数有效" % display_name)


def param_regularizer_l2(display_name):
    return I.float("%s L2，如果使用L1L2正则化，此参数有效" % display_name)


def param_user_regularizer(display_name):
    return I.code(
        "自定义%s，示例：\ndef bigquant_run(weight_matrix):\n    import tensorflow.keras.backend as K\n    return 0.01 * K.sum(K.abs(weight_matrix))"
        % display_name,
        I.code_python,
    )


def param_constraint(param_name, display_name):
    return I.choice(
        "%s，%s，在优化过程中为网络的参数施加约束。本选项里的函数使用的是默认参数，如果需要使用自定义参数或者其他函数，可以选择自定义，然后在下面输入自定义函数" % (display_name, param_name),
        ["max_norm", "non_neg", "unit_norm", "min_max_norm", TEXT_NONE, TEXT_USER],
    )


def param_user_constraint(display_name):
    return I.code(
        "自定义%s，示例：\ndef bigquant_run(w):\n    from tensorflow.keras.constraints import max_norm\n    return max_norm(2.)(w)" % display_name,
        I.code_python,
    )


PARAM_NAME = I.str("名字, name, 可选，层的名字，在模型里不能有两个层同名。如果不指定，将自动分配名字")
PARAM_ACTIVATION = I.choice(
    "激活函数，激活函数使用的是使用默认参数，如果需要修改参数，可以使用自定义激活函数",
    ["softmax", "elu", "selu", "softplus", "softsign", "relu", "tanh", "sigmoid", "hard_sigmoid", "linear", TEXT_NONE, TEXT_USER],
)
PARAM_USER_ACTIVATION = I.code("自定义激活函数，示例：\ndef bigquant_run(x):\n    import tensorflow as tf\n    return tf.atan(x)", I.code_python)
PARAM_USER_KERNEL_SIZE = param_int_list("kernel_size", None, "卷积核的空域或时域窗长度")


def smart_choice(value, user_value):
    if value is None or value == TEXT_NONE:
        return None
    if value == TEXT_USER:
        return user_value
    return value


def drop_none_input(inputs):
    inputs = list(filter(lambda x: x is not None, inputs))
    if len(inputs) == 1:
        return inputs[0]
    return inputs


def smart_regularizer_choice(value, user_value, l1, l2):
    value = smart_choice(value, user_value)
    if value == TEXT_REG_L1L2:
        from tensorflow.keras.regularizers import l1_l2

        value = l1_l2(l1, l2)
    return value


def drop_none_input(inputs):
    inputs = list(filter(lambda x: x is not None, inputs))
    if len(inputs) == 1:
        return inputs[0]
    return inputs


def make_name():
    return "BIGQUANT_TEMP_NAME_%s__" % uuid.uuid1().hex


def rename_layers(yaml):
    seen_names = {}

    def _rep(m):
        name = m.group(0)
        if name not in seen_names:
            seen_names[name] = "L%s" % len(seen_names)
        return seen_names[name]

    return _BIGQUANT_TEMP_NAME_RE.sub(_rep, yaml)


def post_build_layer(layer, layer_name, inputs):
    import tensorflow as tf

    physical_devices = tf.config.experimental.list_physical_devices("GPU")
    for i in range(len(physical_devices)):
        tf.config.experimental.set_memory_growth(physical_devices[i], True)
    layer_name = layer_name or make_name()
    layer.layer_name = layer_name
    if inputs is not None:
        layer = layer(inputs)
    return layer


def model_from_yaml(yaml, custom_objects=None):
    from tensorflow.keras.models import model_from_yaml
    from .dllayernormalization import LayerNormalization

    custom_objects_ = {"LayerNormalization": LayerNormalization}
    if custom_objects:
        custom_objects_.update(custom_objects)
    return model_from_yaml(yaml, custom_objects=custom_objects_)
