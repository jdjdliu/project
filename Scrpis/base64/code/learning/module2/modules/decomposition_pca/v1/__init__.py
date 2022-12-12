import pandas as pd
from sklearn.decomposition import PCA

from learning.api import M
from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list
from sdk.utils import BigLogger
from sdk.utils.func import extend_class_methods

# log = logbook.Logger('decomposition_pca')
log = BigLogger("decomposition_pca")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = r"机器学习\特征预处理"
bigquant_friendly_name = "主成分分析降维"
bigquant_doc_url = ""


def bigquant_run(
    training_ds: I.port("训练数据，如果传入，则需要特征指定输入", specific_type_name="DataSource", optional=True) = None,
    features: I.port("特征，用于训练", specific_type_name="列表|DataSource", optional=True) = None,
    model: I.port("模型，用于预测，如果不指定训练数据，则使用此模型预测", specific_type_name="DataSource", optional=True) = None,
    predict_ds: I.port("预测数据，如果不设置，则不做预测", specific_type_name="DataSource", optional=True) = None,
    n_components: I.int("主成分特征个数，希望PCA降维后的特征维度数目", min=1) = 1,
    whiten: I.bool("白化，使得每个特征具有相同的方差") = False,
    other_train_parameters: I.code("其他训练参数，字典格式，例:{'criterion': 'mse'}", I.code_python, "{}", specific_type_name="字典", auto_complete_type="") = {},
) -> [
    I.port("训练出来的模型", "output_model"),
    I.port("降维后的特征", "pca_features"),
    I.port("降维后训练集，降维特征通过属性pca_features查看", "transform_trainds"),
    I.port("降维后预测集，降维特征通过属性pca_features查看", "transform_predictds"),
]:
    """
    主成分分析，主要功能为降维，是数据预处理的一个步骤。通过主成分分析对于原特征进行线性组合，得到影响力最大的新特征并输出。

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
        train_outputs = M.cached.v2(
            run=_transform,
            kwargs=dict(
                training_ds=training_ds,
                features=features,
                model=model,
                train_parameters=dict(n_components=n_components, whiten=whiten, **other_train_parameters),
            ),
            m_silent=True,
        )
        model = train_outputs.model
        transform_trainds = train_outputs.transform_feature
        pca_features = train_outputs.pca_features
    else:
        transform_trainds = None

    if predict_ds:
        predict_outputs = M.cached.v2(
            run=_transform,
            kwargs=dict(
                training_ds=predict_ds, model=model, train_parameters=dict(n_components=n_components, whiten=whiten, **other_train_parameters)
            ),
            m_silent=True,
        )
        transform_predictds = predict_outputs.transform_feature
        pca_features = predict_outputs.pca_features
    else:
        transform_predictds = None

    outputs = Outputs(output_model=model, pca_features=pca_features, transform_trainds=transform_trainds, transform_predictds=transform_predictds)
    return outputs


def _transform(training_ds, features=None, train_algorithm=PCA, train_parameters={}, model=None):
    df = training_ds.read_df()
    if not model:
        # log.warning("model为空，重新运行模型训练")
        model = train_algorithm(**train_parameters)
        model.fit(df[features])
    else:
        model_info = model.read_pickle()
        model = model_info["model"]
        features = model_info["features"]
    new_data = model.transform(df[features])
    pca_features = ["".join(["pca_component_", str(i)]) for i in range(model.n_components_)]

    pca_df = pd.DataFrame(new_data, index=df.index)
    pca_df.columns = pca_features
    df = pd.concat([df, pca_df], axis=1)

    model_info = {"model": model, "features": features, "pca_features": pca_features, "explained_variance_ratio": model.explained_variance_ratio_}
    model = DataSource.write_pickle(model_info)

    return Outputs(model=model, pca_features=DataSource.write_pickle(pca_features), transform_feature=DataSource.write_df(df))


def bigquant_postrun(outputs):
    # def pca_features(outputs):
    #     model_info = outputs.output_model.read_pickle()
    #     return model_info['pca_features']

    def explained_variance_ratio(outputs):
        model_info = outputs.output_model.read_pickle()
        return model_info["explained_variance_ratio"]

    # extend_class_methods(outputs, pca_features=pca_features)
    extend_class_methods(outputs, explained_variance_ratio=explained_variance_ratio)
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

    m2 = M.decomposition_pca.v1(
        training_ds=m1.data_1,
        features=m3.data,
        predict_ds=m1.data_1,
        n_components=2,
        other_train_parameters={"svd_solver": "randomized"},
        m_cached=True,
    )
    m3 = M.decomposition_pca.v1(model=m2.output_model, predict_ds=m1.data_1, other_train_parameters={"svd_solver": "auto"}, m_cached=True)
    print(1)
