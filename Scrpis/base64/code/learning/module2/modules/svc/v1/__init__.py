import pandas as pd
from sklearn.svm import SVC

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.mlutils import predict
from learning.module2.common.mlutils import train
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

# log = logbook.Logger('svc')
log = BigLogger("svc")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\分类"
bigquant_friendly_name = "支持向量机-分类"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要指定特征输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    C: I.float("C-SVC惩罚系数，即对误差的宽容度,C越高，说明越不能容忍出现误差,容易过拟合。C越小，容易欠拟合。C过大或过小，泛化能力变差") = 1.0,
    kernel: I.choice("核函数类型", ["linear", "poly", "rbf", "sigmoid", "precomputed"]) = "rbf",
    degree: I.int("多项式poly函数的维度，默认是3，选择其他核函数时会被忽略") = 3,
    gamma: I.float(
        "核函数参数gamma，gamma是选择RBF函数作为kernel后，该函数自带的一个参数。隐含地决定了数据映射到新的特征空间后的分布，gamma越大，支持向量越少，gamma值越小，支持向量越多。支持向量的个数影响训练与预测的速度，-1表示默认值为1/n_features"
    ) = -1,
    coef0: I.float("核函数的常数项，对于poly和 sigmoid有用") = 0.0,
    tol: I.float("停止训练的误差值大小，默认为1e-3") = 0.001,
    max_iter: I.int("最大迭代次数，-1为无限制") = -1,
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列用英文逗号分隔") = "date,instrument",
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [I.port("训练出来的模型", "output_model"), I.port("预测结果，如果predict_ds为None，predictions也为None", "predictions")]:
    """
    支持向量分类。
    """

    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if gamma < 0:
        gamma = "auto"

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")

        features = smart_list(features)
        probability = True
        model = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(
                    C=C, kernel=kernel, degree=degree, gamma=gamma, coef0=coef0, tol=tol, max_iter=max_iter, probability=probability
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
        training_ds=training_ds, features=features, train_algorithm=SVC, train_parameters=dict(train_parameters, **other_train_parameters)
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

    m2 = bigquant_run(
        training_ds=m1.data_1,
        features=m3.data,
        predict_ds=m1.data_1,
        key_cols="date,instrument",
        # m_cached=False
    )
    m2_post = bigquant_postrun(m2)
    m2_post.score(data=m1.data_1)
    # m2.predictions.read()
