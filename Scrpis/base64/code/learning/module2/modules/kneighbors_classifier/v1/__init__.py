import learning.module2.common.interface as I  # noqa
import pandas as pd
from learning.api import M
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.mlutils import predict, train
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods
from sklearn.neighbors import VALID_METRICS, KNeighborsClassifier

# log = logbook.Logger('kneighbors_classifier')
log = BigLogger("kneighbors_classifier")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\分类"
bigquant_friendly_name = "k近邻-分类"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要指定模型输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    n_neighbors: I.int("近邻数，int类型，可选参数，默认值为5") = 5,
    weights: I.choice("K近邻权重类型，用于预测。默认值为uniform", values=["uniform", "distance"]) = "uniform",
    algorithm: I.choice(
        "计算分类使用的算法，可选参数，ball_tree为算法BallTree，kd_tree为KDTree，brute使用暴力搜索，auto基于传入内容使用合适算法。默认值为auto", values=["ball_tree", "kd_tree", "brute", "auto"]
    ) = "auto",
    leaf_size: I.int("BallTree或者KDTree算法的叶子数量，int类型，可选参数，此参数会影响构建、查询BallTree或者KDTree的速度，以及存储BallTree或KDTree所需要的内存大小。默认值30") = 30,
    metric: I.choice(
        "距离度量，默认为minkowski，也称欧式距离",
        values=[
            "braycurtis",
            "canberra",
            "chebyshev",
            "cityblock",
            "correlation",
            "cosine",
            "dice",
            "euclidean",
            "hamming",
            "haversine",
            "infinity",
            "jaccard",
            "kulsinski",
            "l1",
            "l2",
            "mahalanobis",
            "manhattan",
            "matching",
            "minkowski",
            "p",
            "precomputed",
            "pyfunc",
            "rogerstanimoto",
            "russellrao",
            "seuclidean",
            "sokalmichener",
            "sokalsneath",
            "sqeuclidean",
            "wminkowski",
            "yule",
        ],
    ) = "minkowski",
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列用英文逗号分隔") = "date,instrument",
    workers: I.int("并行作业数，同时使用多少个进程进行计算，默认是1") = 1,
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [I.port("训练出来的模型", "output_model"), I.port("预测结果，如果predict_ds为None，predictions也为None", "predictions")]:
    """
    用于分类
    """
    if training_ds is None and model is None:
        raise Exception("训练数据和输入模型不能都为空。请输入训练数据training_ds和特征features或者已经训练好的模型model")

    if training_ds:
        if not (training_ds and features):
            log.error("training_ds不为空，准备训练，请输入参数features")
            raise Exception("请输入参数features")
        if model:
            log.warn("training_ds和model都不为空，准备重新训练")
        if algorithm != "auto" and metric not in VALID_METRICS.get(algorithm, []):
            raise ValueError("参数metric不在参数algorithm的可选范围里。查看sklearn.neighbors.VALID_METRICS获取")

        features = smart_list(features)
        model = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(
                    n_neighbors=n_neighbors, weights=weights, algorithm=algorithm, leaf_size=leaf_size, metric=metric, n_jobs=workers
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
        train_algorithm=KNeighborsClassifier,
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

    m2 = bigquant_run(
        training_ds=m1.data_1,
        features=m3.data,
        predict_ds=m1.data_1,
        key_cols="date,instrument",
        workers=1,
        metric="jaccard",
        algorithm="kd_tree",
        other_train_parameters={},
        # m_cached=False
    )
    print(m2.predictions.read())
