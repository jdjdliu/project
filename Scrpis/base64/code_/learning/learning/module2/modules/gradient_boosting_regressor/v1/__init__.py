import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.mlutils import predict
from learning.module2.common.mlutils import train
from learning.module2.common.utils import smart_list
from sdk.utils.func import extend_class_methods
from sdk.utils import BigLogger

# log = logbook.Logger('gradient_boosting_regressor')
log = BigLogger("gradient_boosting_regressor")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\回归"
bigquant_friendly_name = "梯度提升树-回归"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要指定模型输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    loss: I.choice("损失函数，对于分类模型，有对数似然损失函数deviance和指数损失函数exponential", values=["ls", "lad", "huber", "quantile"]) = "ls",
    learning_rate: I.float("学习率，这个参数决定着每一个决策树对于最终结果的影响。GBM设定了初始的权重值之后，每一次树分类都会更新这个值，较小的值使得模型对不同的树更加稳健", min=0) = 0.1,
    iterations: I.int("树的数量，数量越大，则模型越复杂，学习能力越强，更有可能过拟合，需要更多的计算资源", min=1) = 10,
    subsample: I.float("训练每个决策树所用到的子样本占总样本的比例，稍小于1的值能够使模型更稳健，因为这样减少了方差", min=0, max=1.0) = 1.0,
    min_samples_per_leaf: I.int("叶子节点最小样本数，int类型", min=1) = 1,
    max_depth: I.int("树的最大深度，限制每棵树的最大深度，数值大拟合能力强，数值小泛化能力强", min=1) = 3,
    feature_fraction: I.float("特征使用率，寻找最佳分割时要考虑的特征比率，float类型，默认考虑所有特征数，即取值1.0", min=0, max=1.0) = 1.0,
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列用英文逗号分隔") = "date,instrument",
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [I.port("训练出来的模型", "output_model"), I.port("预测结果，如果predict_ds为None，predictions也为None", "predictions")]:
    """
    梯度提升树-回归。

    """
    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")
        if not (0 < feature_fraction <= 1):
            raise ValueError("请重新输入feature_fraction。feature_fraction 为特征使用率，取值范围0 < feature_fraction  <= 1")
        if not isinstance(min_samples_per_leaf, (int)) or min_samples_per_leaf < 1:
            raise ValueError("参数每叶节点最小样本数仅接受int类型，且最小值为1")

        features = smart_list(features)
        model = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(
                    loss=loss,
                    learning_rate=learning_rate,
                    n_estimators=iterations,
                    subsample=subsample,
                    min_samples_leaf=min_samples_per_leaf,
                    max_depth=max_depth,
                    max_features=feature_fraction,
                ),
                other_train_parameters=other_train_parameters,
            ),
            m_silent=True,
        ).model

    if predict_ds:
        predictions = M.cached.v2(run=_predict, kwargs=dict(model=model, predict_ds=predict_ds, key_cols=key_cols), m_silent=True).predictions
    else:
        predictions = None

    outputs = Outputs(output_model=model, predictions=predictions)
    return outputs


def _train(training_ds, features, train_parameters, other_train_parameters):
    outputs = train(
        training_ds=training_ds,
        features=features,
        train_algorithm=GradientBoostingRegressor,
        train_parameters=dict(train_parameters, **other_train_parameters),
    )
    return outputs


def _predict(model, predict_ds, key_cols):
    outputs = predict(model=model, predict_ds=predict_ds, key_cols=key_cols, is_predict_proba=False)
    return outputs


def bigquant_postrun(outputs):
    from learning.module2.common.mlutils import score

    extend_class_methods(outputs, score=score)
    return outputs


if __name__ == "__main__":
    # test code
    # 本代码由可视化策略环境自动生成 2018年10月30日 17:07
    # 本代码单元只能在可视化模式下编辑。您也可以拷贝代码，粘贴到新建的代码单元或者策略，然后修改。
    import datetime

    def m1_run_bigquant_run(input_1, input_2, input_3):
        now = pd.to_datetime("2018-01-01")
        data = []
        for i in range(0, 100):
            for j in range(0, 50):
                data.append(((now + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "%s.SZA" % j, i / (j + 1), i - j, (i + j) % 2))
        df = pd.DataFrame(data, columns=["date", "instrument", "f1", "f2", "label"])

        data_1 = DataSource.write_df(df)

        return Outputs(data_1=data_1)

    # 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
    def m1_post_run_bigquant_run(outputs):
        return outputs

    m1 = M.cached.v3(run=m1_run_bigquant_run, post_run=m1_post_run_bigquant_run, input_ports="", params="{}", output_ports="", m_cached=False)

    m3 = M.input_features.v1(
        features="""f1
        f2""",
        m_cached=False,
    )

    m2 = bigquant_run(
        training_ds=m1.data_1,
        features=m3.data,
        predict_ds=m1.data_1,
        feature_fraction=0.99,
        max_depth=12,
        iterations=10,
        key_cols="date,instrument",
        other_train_parameters={},
        # m_cached=False
    )
    print(m2.predictions.read())
