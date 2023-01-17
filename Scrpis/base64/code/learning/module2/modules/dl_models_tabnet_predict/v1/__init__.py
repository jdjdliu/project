from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

import learning.module2.common.dlutils as DL
import learning.module2.common.interface as I  # noqa
from learning.api import M
from learning.module2.common.data import DataSource, Outputs

log = BigLogger("dl_models_tabnet_pred")

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
bigquant_friendly_name = "TabNet预测"
# bigquant_doc_url = "https://bigquant.com/docs/"


def bigquant_run(
    trained_model: I.port("训练好的模型权重和参数"),
    input_data: I.port("预测数据，包含'x'"),
) -> [I.port("预测结果", "data"),]:
    """
    TabNet模型预测。

    """

    # 载入预训练权重
    checkpoint = trained_model.read_pickle()
    state_dic = checkpoint["state_dic"]
    parameters = checkpoint["parameters"]

    from learning.module2.common.models.pytorch_tabnet import tab_model

    model = tab_model.TabNetRegressor(**parameters)
    model._set_network()
    model.network.load_state_dict(state_dict=state_dic)

    # 预测
    x_test = input_data.read_pickle()["x"]
    log.info("模型预测，样本个数：%s" % (len(x_test)))
    predict = model.predict(x_test)
    ds = DataSource.write_pickle(predict)

    return Outputs(data=ds)


def bigquant_postrun(outputs):

    return outputs


if __name__ == "__main__":
    # 测试代码
    pass
