from learning.module2.common.data import Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import fork_run
import learning.module2.common.dlutils as DL


# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = False

# 是否开源
bigquant_opensource = False
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = r"深度学习\模型"
bigquant_friendly_name = "效果评估(深度学习)"
bigquant_doc_url = "https://bigquant.com/docs/"


TEXT_NONE = "None"
TEXT_USER = "自定义"


def _smart_choice(value, user_value):
    if value is None or value == TEXT_NONE:
        return None
    if value == TEXT_USER:
        return user_value
    return value


def bigquant_run(
    trained_model: I.port("模型"),
    input_data: I.port("数据，pickle格式dict，包含x和y"),
    batch_size: I.int("batch_size，进行梯度下降时每个batch包含的样本数。训练时一个batch的样本会被计算一次梯度下降，使目标函数优化一步。") = 32,
    n_gpus: I.int("gpu个数，本模块使用的gpu个数") = 0,
    verbose: I.choice("日志输出", ["0:不显示", "1:输出进度条记录", "2:每个epoch输出一行记录"]) = "2:每个epoch输出一行记录",
) -> [I.port("评估结果", "data"),]:
    """

    在评估数据集上，计算loss value和metrics values

    """

    def do_run(trained_model, input_data, batch_size, verbose):
        if verbose and isinstance(verbose, str):
            verbose = int(verbose[0])

        model_dict = trained_model.read_pickle()
        model = DL.model_from_yaml(model_dict["model_graph"])
        model.set_weights(model_dict["model_weights"])

        input_data = input_data.read_pickle()

        result = model.evaluate(x=input_data["x"], y=input_data["y"], batch_size=batch_size, verbose=verbose)
        return result

    result = fork_run(do_run, trained_model, input_data, batch_size, verbose)
    return Outputs(data=result)


def bigquant_postrun(outputs):
    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
