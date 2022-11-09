from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

import learning.module2.common.dlutils as DL
import learning.module2.common.interface as I  # noqa
from learning.api import M
from learning.module2.common.data import DataSource, Outputs

log = BigLogger("dl_models_tabnet_train")

# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False
bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = r"深度学习\模型库"
bigquant_friendly_name = "TabNet训练"
# bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    training_data: I.port("训练数据，pickle格式dict，包含x和y"),
    validation_data: I.port("验证数据，pickle格式dict，包含x和y", optional=True) = None,
    input_dim: I.int("输入特征维度") = 98,
    n_steps: I.int("决策的步数，通常为{3，10}") = 3,
    n_d: I.int("预测阶段的特征数，通常为{4 ~ 8}，且n_d=n_a") = 8,
    n_a: I.int("Attentive的特征数，通常为{4 ~ 8}") = 8,
    gamma: I.float("注意力更新的比例，通常为{1.0 ~ 2.0}") = 1.3,
    momentum: I.float("归一化层的动量，通常为{0 ~ 1}") = 0.02,
    batch_size: I.int("一个批次训练的样本数，通常为{256 ~ 2048}") = 1024,
    virtual_batch_size: I.int("GBN模块的虚拟批次大小，通常为{128 ~ 2048}") = 128,
    epochs: I.int("迭代次数") = 20,
    num_workers: I.int("数据读取的进程数，默认全部调用") = 0,
    device_name: I.choice("是否调用GPU", ["auto:自动调用GPU", "cpu:使用cpu训练", "cuda:使用GPU训练"]) = "auto:自动调用GPU",
    verbose: I.choice("日志输出", ["0:不显示", "1:输出进度条记录"]) = "1:输出进度条记录",
) -> [I.port("训练后的模型", "data"),]:
    """
    TabNet模型训练。
    TabNet（TabNet: Attentive Interpretable Tabular Learning）是适用于表格类型数据的模型，结合了树模型和DNN模型的优点。

    """
    # 准备训练数据
    training_data = training_data.read_pickle()
    if validation_data:
        validation_data = validation_data.read_pickle()
        validation_data = (validation_data["x"], validation_data["y"])

    if device_name:
        device_name = device_name.split(":")[0]

    if verbose and isinstance(verbose, str):
        verbose = int(verbose[0])

    # 定义模型
    parameters = {
        "input_dim": input_dim,
        "output_dim": 1,
        "n_steps": n_steps,
        "n_d": n_d,
        "n_a": n_a,
        "gamma": gamma,
        "momentum": momentum,
        "virtual_batch_size": virtual_batch_size,
        "device_name": device_name,
        "verbose": verbose,
    }
    from learning.module2.common.models.pytorch_tabnet import tab_model

    model = tab_model.TabNetRegressor(**parameters)

    # 模型训练
    log.info("准备训练，训练样本个数：%s，迭代次数：%s" % (len(training_data["x"]), epochs))
    model.fit(
        training_data["x"],
        training_data["y"],
        eval_set=[validation_data],
        batch_size=batch_size,
        max_epochs=epochs,
        num_workers=num_workers,
    )

    # 保存模型的权重
    state_dict = model.network.state_dict()

    checkpoint = {"state_dic": state_dict, "parameters": parameters}
    ds = DataSource.write_pickle(checkpoint)

    return Outputs(data=ds)


def bigquant_postrun(outputs):

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
