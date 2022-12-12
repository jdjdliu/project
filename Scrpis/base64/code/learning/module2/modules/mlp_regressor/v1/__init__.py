import pandas as pd
from sklearn.neural_network import MLPRegressor

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.mlutils import prepare_data
from learning.module2.common.mlutils import predict
from learning.module2.common.mlutils import train
from learning.module2.common.utils import smart_list
from sdk.utils.func import extend_class_methods
from sdk.utils import BigLogger

# log = logbook.Logger('mlp_regressor')
log = BigLogger("mlp_regressor")

bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\回归"
bigquant_friendly_name = "多层感知器-回归"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要特征指定输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    hidden_layer_sizes: I.str("输入该隐藏层的神经元个数，用英文逗号(,)分隔的整数元组，例如 100,100，表示有两层隐藏层，第一层隐藏层有100个神经元，第二层也有100个神经元") = "100",
    activation: I.choice("隐藏层的激活函数类型", ["identity", "logistic", "tanh", "relu"]) = "relu",
    solver: I.choice("优化器，用于优化权重，默认为adam", ["lbfgs", "sgd", "adam"]) = "adam",
    alpha: I.float("L2 惩罚项(正则项) 参数") = 0.0001,
    batch_size: I.int("随机优化算法的批量大小，如果优化器是 ‘lbfgs’, 将不会生效") = 200,
    learning_rate_init: I.float("学习率的初始默认值，当且仅当优化策略是SGD或者ADAM时，才有意义。本参数控制着更新权重时的步长") = 0.001,
    max_iter: I.int("最大可迭代次数，优化器直至收敛（视最小容忍度而定）或者到达最大迭代次数时，算法停止") = 200,
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列用英文逗号分隔") = "date,instrument",
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [I.port("训练出来的模型", "output_model"), I.port("预测结果， 如果predict_ds为None， predictions也为None", "predictions")]:
    """
    用于回归的机器学习算法。多层感知器（Multilayer Perceptron,缩写MLP）是一种前向结构的人工神经网络，映射一组输入向量到一组输出向量。可用于线性不可分数据。
    """

    hidden_layer_sizes = smart_list(hidden_layer_sizes, sep=",", cast_type=int, drop_empty_lines=True) or None

    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")

        features = smart_list(features)
        df = training_ds.read_df()
        train_data, train_label = prepare_data(df, features)
        batch_size = min(batch_size, len(train_data))
        model = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(
                    hidden_layer_sizes=hidden_layer_sizes,
                    activation=activation,
                    solver=solver,
                    alpha=alpha,
                    batch_size=batch_size,
                    learning_rate_init=learning_rate_init,
                    max_iter=max_iter,
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
        training_ds=training_ds, features=features, train_algorithm=MLPRegressor, train_parameters=dict(train_parameters, **other_train_parameters)
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
        key_cols="date,instrument",
        hidden_layer_sizes="100",
        activation="relu",
        solver="adam",
        alpha=0.0001,
        batch_size=200,
        learning_rate_init=0.001,
        max_iter=200,
        # m_cached=False
    )
    m2.predictions.read()
