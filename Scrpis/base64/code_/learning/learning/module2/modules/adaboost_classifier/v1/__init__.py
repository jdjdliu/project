from sdk.utils import BigLogger

import learning.module2.common.interface as I
import pandas as pd

# from learning import settings
from sdk.utils.func import extend_class_methods
from sklearn.ensemble import AdaBoostClassifier

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.mlutils import predict, train
from learning.module2.common.utils import smart_list

# log = logbook.Logger('adaboost_classifier')
log = BigLogger("adaboost_classifier")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\分类"
bigquant_friendly_name = "自适应提升树-分类"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要指定模型输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    n_estimators: I.int("弱学习器个数，弱学习器最大迭代次数，太小容易欠拟合，太大容易过拟合，默认为50", min=1) = 50,
    algorithm: I.choice("分类算法，SAMME.R使用了对样本集分类的预测概率大小来作为弱学习器权重，迭代一般比SAMME快", values=["SAMME.R", "SAMME"]) = "SAMME.R",
    learning_rate: I.float("学习率，该值缩减每个弱学习器的权重，通常较小的系数需要更多的迭代，因此n_estimators和learning_rate要一起调参", 0.0, 1.0) = 1.0,
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列名用英文逗号分隔") = "date,instrument",
    other_train_parameters: I.code(
        "其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{'base_estimator': None}", specific_type_name="字典", auto_complete_type=""
    ) = {"base_estimator": None},
) -> [I.port("训练出来的模型", "output_model"), I.port("预测结果，如果predict_ds为None，predictions也为None", "predictions")]:
    """
    adaboost自适应提升树算法，用于分类，此算法基于boosting，根据上次分类的准确率确定下次训练每个样本的权值，将修改过权值的新数据集送给下层分类器进行训练，最后将每次训练得到的分类器融合，作为最后的决策分类器。

    """
    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")

        features = smart_list(features)
        model = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(n_estimators=n_estimators, algorithm=algorithm, learning_rate=learning_rate),
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
        train_algorithm=AdaBoostClassifier,
        train_parameters=dict(train_parameters, **other_train_parameters),
    )
    return outputs


def _predict(model, predict_ds, key_cols):
    outputs = predict(model=model, predict_ds=predict_ds, key_cols=key_cols, is_predict_proba=True)
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

    # m2 = bigquant_run(
    #     training_ds=m1.data_1,
    #     features=m3.data,
    #     predict_ds=m1.data_1,
    #     key_cols='date,instrument',
    #     other_train_parameters={},
    #     # m_cached=False
    # )
    m2 = M.adaboost_classifier.v1(
        training_ds=m1.data_1,
        features=m3.data,
        predict_ds=m1.data_1,
        key_cols="date,instrument",
        n_estimators=1,
        learning_rate=0.9,
        algorithm="SAMME",
        other_train_parameters={},
        # m_cached=False
    )
    print(m2.score(data=m1.data_1))
    print(m2.predictions.read())
