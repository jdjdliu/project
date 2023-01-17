from sdk.utils import BigLogger
from learning.api import M
from learning.module2.common.data import Outputs
import learning.module2.common.interface as I


# log = logbook.Logger('metrics_regression')
log = BigLogger("metrics_regression")
bigquant_cacheable = False


# 模块接口定义
bigquant_category = "模型评估"
bigquant_friendly_name = "回归-评估"
bigquant_doc_url = ""


def smart_list(*args, **kwargs):
    from learning.module2.common.utils import smart_list

    return_value = smart_list(*args, **kwargs)
    return None if return_value == [] else return_value


def _explained_variance_score(y_true, y_pred):
    from sklearn.metrics import explained_variance_score

    evs = explained_variance_score(y_true, y_pred)
    print("可解释方差: %s" % evs)
    return evs


def _mean_absolute_error(y_true, y_pred):
    from sklearn.metrics import mean_absolute_error

    m = mean_absolute_error(y_true, y_pred)
    print("平均绝对误差: %s" % m)
    return m


def _mean_squared_error(y_true, y_pred):
    from sklearn.metrics import mean_squared_error

    m = mean_squared_error(y_true, y_pred)
    print("均方误差: %s" % m)
    return m


def _mean_squared_log_error(y_true, y_pred):
    from sklearn.metrics import mean_squared_log_error

    m = mean_squared_log_error(y_true, y_pred)
    print("均方对数误差: %s" % m)
    return m


def _median_absolute_error(y_true, y_pred):
    from sklearn.metrics import median_absolute_error

    m = median_absolute_error(y_true, y_pred)
    print("均方绝对误差: %s" % m)
    return m


def _r2_score(y_true, y_pred):
    from sklearn.metrics import r2_score

    r = r2_score(y_true, y_pred)
    print("确定系数(r^2): %s" % r)
    return r


def bigquant_run(
    predictions: I.port("预测结果，提供模型预测出的分类及分类对应概率", specific_type_name="DataSource", optional=False) = None,
    explained_variance_score: I.bool("可解释方差权重，") = True,
    mean_absolute_error: I.bool("平均绝对误差") = True,
    mean_squared_error: I.bool("均方误差权重") = True,
    mean_squared_log_error: I.bool("均方对数误差权重") = True,
    median_absolute_error: I.bool("均方绝对误差") = True,
    r2_score: I.bool("确定系数(r^2)权重，列表类型") = True,
) -> [I.port("评估报告", "report", optional=True)]:
    """
    回归评估。输入模型结果，输出对于回归的评估结果。具体评估标准包括R2，MSE，MAE等。

    """
    df_data = predictions.read_df()
    report = {}
    if "label" not in df_data.keys():
        raise ValueError("训练数据中无label，无法评估模型")
    if "pred_label" not in df_data.keys():
        raise ValueError("model中无pred_label，无法评估模型")

    y_true = df_data["label"]
    y_pred = df_data["pred_label"]

    if explained_variance_score:
        explained_variance_score = _explained_variance_score(y_true=y_true, y_pred=y_pred)
        report["explained_variance_score"] = explained_variance_score
    if mean_absolute_error:
        mean_absolute_error = _mean_absolute_error(y_true=y_true, y_pred=y_pred)
        report["mean_absolute_error"] = mean_absolute_error
    if mean_squared_error:
        mean_squared_error = _mean_squared_error(y_true=y_true, y_pred=y_pred)
        report["mean_squared_error"] = mean_squared_error
    if mean_squared_log_error:
        mean_squared_log_error = _mean_squared_log_error(y_true=y_true, y_pred=y_pred)
        report["mean_squared_log_error"] = mean_squared_log_error
    if median_absolute_error:
        median_absolute_error = _median_absolute_error(y_true=y_true, y_pred=y_pred)
        report["median_absolute_error"] = median_absolute_error
    if r2_score:
        r2_score = _r2_score(y_true=y_true, y_pred=y_pred)
        report["r2_score"] = r2_score

    return Outputs(report=report)


if __name__ == "__main__":
    m1 = M.instruments.v2(start_date="2016-02-02", end_date="2016-05-05", market="CN_STOCK_A", instrument_list="", max_count=0)

    m5 = M.advanced_auto_labeler.v2(
        instruments=m1.data,
        label_expr="""# #号开始的表示注释
    # 0. 每行一个，顺序执行，从第二个开始，可以使用label字段
    # 1. 可用数据字段见 https://bigquant.com/docs/develop/datasource/deprecated/history_data.html
    #   添加benchmark_前缀，可使用对应的benchmark数据
    # 2. 可用操作符和函数见 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_

    # 计算收益：5日收盘价(作为卖出价格)除以明日开盘价(作为买入价格)
    shift(close, -5) / shift(open, -1)

    # 极值处理：用1%和99%分位的值做clip
    clip(label, all_quantile(label, 0.01), all_quantile(label, 0.99))

    # 将分数映射到分类，这里使用20个分类
    all_wbins(label, 5)

    # 过滤掉一字涨停的情况 (设置label为NaN，在后续处理和训练中会忽略NaN的label)
    where(shift(high, -1) == shift(low, -1), NaN, label)
    """,
        start_date="",
        end_date="",
        benchmark="000300.SHA",
        drop_na_label=True,
        cast_label_int=True,
        user_functions={},
    )

    m3 = M.input_features.v1(
        features="""
    # #号开始的表示注释
    # 多个特征，每行一个，可以包含基础特征和衍生特征
    return_5
    close_0 / close_1
    """
    )

    m2 = M.general_feature_extractor.v7(instruments=m1.data, features=m3.data, start_date="", end_date="", before_start_days=90)

    m4 = M.derived_feature_extractor.v3(input_data=m2.data, features=m3.data, date_col="date", instrument_col="instrument", user_functions={})

    m6 = M.join.v3(data1=m5.data, data2=m4.data, on="date,instrument", how="inner", sort=False)

    m7 = M.dropnan.v1(input_data=m6.data)

    m9 = M.instruments.v2(start_date="2018-05-05", end_date="2018-06-06", market="CN_STOCK_A", instrument_list="", max_count=0)

    m10 = M.general_feature_extractor.v7(instruments=m9.data, features=m3.data, start_date="", end_date="", before_start_days=90)

    m11 = M.derived_feature_extractor.v3(input_data=m10.data, features=m3.data, date_col="date", instrument_col="instrument", user_functions={})

    m13 = M.advanced_auto_labeler.v2(
        instruments=m9.data,
        label_expr="""# #号开始的表示注释
    # 0. 每行一个，顺序执行，从第二个开始，可以使用label字段
    # 1. 可用数据字段见 https://bigquant.com/docs/develop/datasource/deprecated/history_data.html
    #   添加benchmark_前缀，可使用对应的benchmark数据
    # 2. 可用操作符和函数见 `表达式引擎 <https://bigquant.com/docs/develop/bigexpr/usage.html>`_

    # 计算收益：5日收盘价(作为卖出价格)除以明日开盘价(作为买入价格)
    shift(close, -5) / shift(open, -1)

    # 极值处理：用1%和99%分位的值做clip
    clip(label, all_quantile(label, 0.01), all_quantile(label, 0.99))

    # 将分数映射到分类，这里使用20个分类
    all_wbins(label, 5)

    # 过滤掉一字涨停的情况 (设置label为NaN，在后续处理和训练中会忽略NaN的label)
    where(shift(high, -1) == shift(low, -1), NaN, label)
    """,
        start_date="",
        end_date="",
        benchmark="000300.SHA",
        drop_na_label=True,
        cast_label_int=True,
        user_functions={},
    )

    m15 = M.join.v3(data1=m11.data, data2=m13.data, on="date,instrument", how="inner", sort=False)

    m12 = M.dropnan.v1(input_data=m15.data)

    m8 = M.logistic_regression.v1(
        training_ds=m7.data,
        features=m3.data,
        predict_ds=m12.data,
        penalty="l2",
        dual=False,
        fit_intercept=True,
        C=1,
        key_cols="date,instrument",
        workers=1,
        other_train_parameters={},
    )

    m14 = M.metrics_regression.v1(predictions=m8.predictions, explained_variance_score_weights="")
