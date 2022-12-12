from collections import OrderedDict

import numpy as np
import pandas as pd
from numpy.lib.function_base import interp
from sklearn.metrics import (
    accuracy_score,
    auc,
    average_precision_score,
    classification_report,
    confusion_matrix,
    hamming_loss,
    log_loss,
    precision_recall_curve,
    precision_recall_fscore_support,
    roc_curve,
    zero_one_loss,
)

import learning.module2.common.interface as I
from sdk.utils import BigLogger

log = BigLogger("metrics_classification")
bigquant_cacheable = True
# 模块接口定义
bigquant_category = "模型评估"
bigquant_friendly_name = "分类-评估"
bigquant_doc_url = ""


def _classification_report(y_true, y_pred, target_names):
    data = classification_report(y_true, y_pred, target_names=target_names, output_dict=True)
    if "accuracy" in data:
        del data["accuracy"]

    return pd.DataFrame(data).T


def _resample(df, on, bins=1000):
    on_min = df[on].min()
    on_max = df[on].max()
    assert on_max > on_min
    by = (((df[on] - on_min) / (on_max - on_min)) * 1000).astype(int)

    return df.groupby(by=pd.Series(by)).agg(np.nanmean)


def _roc(y_true, y_scores, n_classes, size=10):
    fpr, tpr, roc_auc = dict(), dict(), dict()

    if y_true.shape[1] == 1:
        fpr, tpr, _ = roc_curve(y_true, y_scores.max(axis=1))
        df = pd.DataFrame({"fpr": fpr, "ROC curve (area={0:0.2f})".format(auc(fpr, tpr)): tpr})
    else:
        # multi-class
        fpr = [None] * n_classes
        tpr = [None] * n_classes
        roc_auc = [None] * n_classes
        for i in range(n_classes):
            fpr[i], tpr[i], _ = roc_curve(y_true[:, i], y_scores[:, i])
            roc_auc[i] = "ROC curve of class {0}(area={1:0.2f})".format(i, auc(fpr[i], tpr[i]))

        # Compute macro-average ROC curve and ROC area
        # First aggregate all false positive rates
        all_fpr = np.unique(np.concatenate(fpr))
        # Then interpolate all ROC curves at this points
        mean_tpr = np.zeros_like(all_fpr)
        for i in range(n_classes):
            mean_tpr += interp(all_fpr, fpr[i], tpr[i])
        # Finally average it and compute AUC
        mean_tpr /= n_classes

        fpr_macro = all_fpr
        tpr_macro = mean_tpr
        auc_macro = "macro avg ROC curve (area={0:0.2f})".format(auc(fpr_macro, tpr_macro))

        fpr_micro, tpr_micro, _ = roc_curve(y_true.ravel(), y_scores.ravel())
        auc_micro = "micro avg ROC curve (area={0:0.2f})".format(auc(fpr_micro, tpr_micro))

        df = pd.DataFrame({"fpr": fpr_macro, auc_macro: tpr_macro}).merge(
            pd.DataFrame({"fpr": fpr_micro, auc_micro: tpr_micro}), on="fpr", how="outer"
        )
        for i in range(n_classes):
            df = df.merge(pd.DataFrame({"fpr": fpr[i], roc_auc[i]: tpr[i]}), on="fpr", how="outer")

    df = _resample(df, "fpr")
    df.interpolate(method="linear", inplace=True)

    return df


def _precision_recall_curve(y_true, y_scores, n_classes):
    if n_classes == 2:
        # binary-class
        y_scores = y_scores.max(axis=1)
        average_precision = average_precision_score(y_true, y_scores)
        precision, recall, _ = precision_recall_curve(y_true, y_scores)
        df = pd.DataFrame({"recall": recall, "precision recall AP={0:0.2f}".format(average_precision): precision})
    else:
        precision = [None] * n_classes
        recall = [None] * n_classes
        average_precision = [None] * n_classes

        for i in range(n_classes):
            precision[i], recall[i], _ = precision_recall_curve(y_true[:, i], y_scores[:, i])
            average_precision[i] = "precision recall for class {0} AP={1:0.2f}".format(i, average_precision_score(y_true[:, i], y_scores[:, i]))

        # A "micro-average": quantifying score on all classes jointly
        precision_micro, recall_micro, _ = precision_recall_curve(y_true.ravel(), y_scores.ravel())
        average_precision_micro = "micro avg precision recall AP={0:0.2f}".format(average_precision_score(y_true, y_scores, average="micro"))
        # Average precision score, micro-averaged over all classes

        df = pd.DataFrame({"recall": recall_micro, average_precision_micro: precision_micro})
        for i in range(n_classes):
            df = df.merge(pd.DataFrame({"recall": recall[i], average_precision[i]: precision[i]}), on="recall", how="outer")

    df = _resample(df, "recall")
    df.interpolate(method="linear", inplace=True)

    return df


def _read_predictions(predictions):
    df = predictions.read_df()
    if "label" not in df.keys():
        raise ValueError("训练数据中无label，无法评估模型")
    if "pred_label" not in df.keys():
        raise ValueError("model中无pred_label，无法评估模型")
    return df


def bigquant_run(
    predictions: I.port("预测结果，提供模型预测出的分类及分类对应概率", specific_type_name="DataSource", optional=False), **kwargs
) -> [I.port("评估报告", "data")]:
    """
    分类评估。输入模型结果，输出评估结果：精准率-召回率曲线、ROC-AUC、常用指标、准确率与错误率、混淆矩阵。
    """
    from sklearn.preprocessing._label import label_binarize

    df_data = _read_predictions(predictions)
    df = df_data
    y_true = df["label"]
    y_pred = df["pred_label"]
    true_labels = sorted(set(y_true))
    y_score_dict = {k: v for k, v in df.items() if "classes_prob_" in k}
    target_names = sorted(y_score_dict.keys())

    if target_names:
        v_classification_report = _classification_report(y_true, y_pred, target_names)
        v_log_loss = log_loss(y_true=y_true, y_pred=df[target_names].values)
    else:
        v_log_loss = "模型中无分类概率，不能评估对数损失"
        target_names = [str(i) for i in set(y_pred)]
        if len(true_labels) != len(target_names):
            v_classification_report = "预测分类数与实际分类数不等，不能评估准确率/召回率/f1值"
        else:
            v_classification_report = _classification_report(y_true, y_pred, target_names)

    accuracy_and_loss = pd.DataFrame(
        {
            "value": [
                accuracy_score(y_true, y_pred),
                v_log_loss,
                zero_one_loss(y_true, y_pred),
                hamming_loss(y_true=y_true, y_pred=y_pred),
                precision_recall_fscore_support(y_true=y_true, y_pred=y_pred)[2],
            ]
        },
        index=["accu_score", "log_loss", "zero_one_loss", "hamming_loss", "fbeta_score"],
    )

    if y_score_dict:
        y_scores = pd.DataFrame(y_score_dict).values
        y_true_one_vs_all = label_binarize(y_true, classes=true_labels)
        roc_df = _roc(y_true_one_vs_all, y_scores, len(true_labels))
        pr_df = _precision_recall_curve(y_true_one_vs_all, y_scores, len(true_labels))
    else:
        log.warn("模型中无分类概率，无法绘制ROC曲线图及精准率-召回率图")

    confusion_matrix_data = confusion_matrix(y_true, y_pred, labels=true_labels)
    normalized_confusion_matrix_data = confusion_matrix_data.astype("float") / confusion_matrix_data.sum(axis=1)[:, np.newaxis]

    data = {
        "classification_report": v_classification_report,
        "accuracy_and_loss": accuracy_and_loss,
        "confusion_matrix": pd.DataFrame(data=confusion_matrix_data, columns=true_labels, index=true_labels),
        "normalized_confusion_matrix": pd.DataFrame(data=normalized_confusion_matrix_data, columns=true_labels, index=true_labels),
        "roc": roc_df,
        "precision_recall": pr_df,
    }

    from learning.module2.common.data import DataSource, Outputs

    return Outputs(data=DataSource.write_pickle(data))


def _translate_one(s, trans):
    if isinstance(trans, dict):
        return s.map(lambda x: trans.get(x, x))
    if isinstance(trans, str):
        return s.map(lambda x: trans.format(x))
    raise Exception(type(trans))


def _add_instr_trans(trans, s, instrs):
    if not instrs:
        return

    for k in s:
        if not isinstance(k, str):
            continue
        new_k = k
        for p in instrs:
            new_k = new_k.replace(p, trans[p])
        if k != new_k:
            trans[k] = new_k


# translate to Chinese
def _translate(df, col_trans=None, index_trans=None, reset_index=None, instrs=[]):
    default_trans = {
        "accu_score": "准确率",
        "log_loss": "对数损失",
        "zero_one_loss": "0-1损失",
        "hamming_loss": "汉明损失",
        "fbeta_score": "FBeta",
        "macro avg": "宏平均",
        "micro avg": "微平均",
        "micro avg ": "微平均",
        "weighted avg": "加权平均",
        "accuracy": "准确率",
        "f1-score": "f1值",
        "precision": "精确率",
        "recall": "召回率",
        "support": "样本数",
        "classes_prob_": "分类",
        " ROC curve ": "ROC曲线",
        "ROC curve of class ": "ROC曲线-分类",
        "precision recall": "精准率召回率",
        " for ": "-",
        "class ": "分类",
    }
    if col_trans is None:
        # default trans
        col_trans = default_trans
    if index_trans is None:
        index_trans = default_trans

    if col_trans:
        _add_instr_trans(col_trans, df.columns, instrs)
        df.columns = _translate_one(df.columns, col_trans)
    if index_trans:
        _add_instr_trans(index_trans, df.index, instrs)
        df.index = _translate_one(df.index, index_trans)
    if reset_index is not None:
        df.index.name = reset_index
        df.reset_index(drop=False, inplace=True)

    return df


def _plot_table(df):
    from bigcharts.datatale import plot as plot_table

    if isinstance(df, str):
        return df

    return plot_table(df, output="script")


def bigquant_postrun(outputs):
    from bigcharts.tabs import plot as plot_tabs

    import learning.api.tools as T

    data = outputs.data.read_pickle()

    confusion_matrix_html = _plot_table(_translate(data["confusion_matrix"], index_trans="实际 {}", col_trans="预测 {}", reset_index="混淆矩阵"))
    confusion_matrix_html += _plot_table(
        _translate(data["normalized_confusion_matrix"], index_trans="实际 {}", col_trans="预测 {}", reset_index="正则化混淆矩阵")
    )

    roc_df = _translate(data["roc"], index_trans=False, instrs=["macro avg", "micro avg", " ROC curve ", "ROC curve of class "])
    roc_options = {
        "yAxis": [
            {
                "title": {
                    "text": "True Positive Rate",
                },
                "min": 0,
                "max": 1,
            },
        ],
        "xAxis": {
            "title": {
                "text": "False Positive Rate",
            },
        },
    }
    roc_html = T.plot(roc_df, x="fpr", output="script", title="ROC/AUC", options=roc_options)

    pr_df = _translate(data["precision_recall"], index_trans=False, instrs=["precision recall", " for ", "micro avg ", "class "])
    pr_options = {
        "yAxis": [
            {
                "title": {
                    "text": "精准率",
                },
                "min": 0,
                "max": 1,
            },
        ],
        "xAxis": {
            "title": {
                "text": "召回率",
            },
        },
    }
    pr_html = T.plot(pr_df, x="召回率", output="script", title="精准率-召回率", options=pr_options)

    plot_tabs(
        OrderedDict(
            [
                ("精准率-召回率", pr_html),
                ("ROC-AUC", roc_html),
                (
                    "常用指标",
                    _plot_table(_translate(data["classification_report"], instrs=["classes_prob_"], reset_index="")),
                ),
                ("准确率与损失值", _plot_table(_translate(data["accuracy_and_loss"], reset_index=""))),
                ("混淆矩阵", confusion_matrix_html),
            ]
        ),
        title=bigquant_friendly_name,
    )

    return outputs
