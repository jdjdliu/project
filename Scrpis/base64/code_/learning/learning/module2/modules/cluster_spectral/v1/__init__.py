import pandas as pd
from sklearn.cluster import SpectralClustering

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I  # noqa
from learning.module2.common.mlutils import cluster_predict
from learning.module2.common.mlutils import cluster_train
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger

# from sdk.utils.func import extend_class_methods

# log = logbook.Logger('cluster_spectral')
log = BigLogger("cluster_spectral")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\聚类"
bigquant_friendly_name = "谱-聚类"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要指定模型输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    n_clusters: I.int("分类簇的数量，切图时降到的维数", min=1) = 3,
    affinity: I.choice(
        "相似矩阵的建立方式，为nearest_neighbors时忽略参数gamma，默认为rbf", values=["rbf", "nearest_neighbors", "precomputed", "linear", "poly", "sigmoid"]
    ) = "rbf",
    n_neighbors: I.int("近邻个数，KNN算法的K的个数，默认是10，affinity为nearest_neighbors忽略此参数", min=1) = 10,
    eigen_solver: I.choice("特征值分解策略，amg存在稳定性问题，平台暂不支持", values=[None, "arpack", "lobpcg"]) = None,
    eigen_tol: I.float("停止拉普拉斯矩阵特征分解条件，当eigen_solver为eigen_solver时生效") = 0.0,
    n_init: I.int("K-Means聚类的运行次数，最终结果是连续运行后的最佳输出", min=0) = 10,
    gamma: I.float("核系数，rbf，poly，sigmoid，laplacian和chi2的核系数") = 1.0,
    assign_labels: I.choice("聚类方法，常用kmeans，但对质心初始化较敏感，discretize则不敏感", values=["kmeans", "discretize"]) = "kmeans",
    key_cols: I.str("关键列，关键列的数据会复制到预测结果里，多个列用英文逗号分隔") = "date,instrument",
    workers: I.int("并行作业数，同时使用多少个进程进行计算，默认是1") = 1,
    random_state: I.int("随机数种子，默认是0") = 0,
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [
    I.port("训练出来的模型", "output_model"),
    I.port("训练数据集，聚类训练结果为列label", "transform_trainds"),
    I.port("预测结果，如果predict_ds为None，predictions也为None", "predictions"),
]:
    """
    用于聚类，将图论应用于聚类。把所有的数据看做空间中的点，这些点之间可以用边连接起来。距离较远的两个点之间的边权重值较低，而距离较近的两个点之间的边权重值较高，通过对所有数据点组成的图进行切图，让切图后不同的子图间边权重和尽可能的低，而子图内的边权重和尽可能的高，从而达到聚类的目的。

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
        result_outputs = M.cached.v2(
            run=_train,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                train_parameters=dict(
                    n_clusters=n_clusters,
                    affinity=affinity,
                    n_neighbors=n_neighbors,
                    eigen_solver=eigen_solver,
                    eigen_tol=eigen_tol,
                    n_init=n_init,
                    gamma=gamma,
                    assign_labels=assign_labels,
                    n_jobs=workers,
                    random_state=random_state,
                ),
                other_train_parameters=other_train_parameters,
            ),
            m_silent=True,
        )
        model = result_outputs.model
        transform_trainds = result_outputs.transform_trainds
    else:
        transform_trainds = None

    if predict_ds:
        predictions = M.cached.v2(run=_predict, kwargs=dict(model=model, predict_ds=predict_ds, key_cols=key_cols), m_silent=True).predictions
    else:
        predictions = None

    outputs = Outputs(output_model=model, transform_trainds=transform_trainds, predictions=predictions)
    return outputs


def _train(training_ds, features, train_parameters, other_train_parameters):
    outputs = cluster_train(
        training_ds=training_ds,
        features=features,
        train_algorithm=SpectralClustering,
        train_parameters=dict(train_parameters, **other_train_parameters),
    )
    return outputs


def _predict(model, predict_ds, key_cols):
    outputs = cluster_predict(model=model, predict_ds=predict_ds, key_cols=key_cols)
    return outputs


def bigquant_postrun(outputs):
    # from learning.module2.common.mlutils import score
    # extend_class_methods(outputs, score=score)
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

    m2 = M.cluster_spectral.v1(
        training_ds=m1.data_1,
        features=m3.data,
        # predict_ds=m1.data_1,
        # key_cols='date,instrument',
        n_clusters=3,
        affinity="rbf",
        n_neighbors=10,
        eigen_solver=None,
        eigen_tol=0.1,
        n_init=10,
        gamma=1.0,
        assign_labels="discretize",
        # workers=1,
        other_train_parameters={},
        # m_cached=False
    )
    print(m2.transform_trainds.read())
    print(m2.predictions.read())
    dir(m2.output_model.read()["model"])
    print(1)
