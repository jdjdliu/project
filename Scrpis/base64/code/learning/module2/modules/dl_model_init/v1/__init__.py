from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
from sdk.utils.func import extend_class_methods
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
bigquant_category = r"深度学习\模型"
bigquant_friendly_name = "构建(深度学习)"
bigquant_doc_url = "https://bigquant.com/docs/"


def save_model(model_yaml):
    return Outputs(data=DataSource.write_pickle(model_yaml))


def bigquant_run(
    inputs: I.port("输入") = None, outputs: I.port("输出") = None
) -> [I.port("模型", "data"),]:
    """

    构造一个拥有输入和输出的模型。我们使用Model来初始化构造一个函数式模型。

    """

    name = "BigQuantDL"

    from tensorflow.keras.models import Model

    model = Model(inputs=inputs, outputs=outputs, name=name)

    # 找到函数依赖，在缓存的时候使用函数源代码
    deps = []
    import types

    for layer in model.layers:
        config = layer.get_config()
        for k, _ in config.items():
            v = getattr(layer, k, None)
            if isinstance(v, types.FunctionType):
                deps.append(v)

    # 给模型名字重新命名，每次运行，layer的名字都会不同，给默认名字重新设置
    yaml = DL.rename_layers(model.to_yaml())

    outputs = M.cached.v3(run=save_model, kwargs=dict(model_yaml=yaml), m_deps=deps)

    return Outputs(data=outputs.data)


def bigquant_postrun(outputs):
    from .postrun import plot

    extend_class_methods(outputs, plot=plot)

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
