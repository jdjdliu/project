from learning.module2.common.data import DataSource, Outputs
import learning.module2.common.interface as I
from learning.module2.common.utils import smart_list, smart_object
import re


# 是否自动缓存结果
bigquant_cacheable = True

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = True
bigquant_author = "BigQuant"

DEFAULT_FEATURES = """
# #号开始的表示注释，注释需单独一行
# 多个特征，每行一个，可以包含基础特征和衍生特征，特征须为本平台特征
return_5
return_10
return_20
avg_amount_0/avg_amount_5
avg_amount_5/avg_amount_20
rank_avg_amount_0/rank_avg_amount_5
rank_avg_amount_5/rank_avg_amount_10
rank_return_0
rank_return_5
rank_return_10
rank_return_0/rank_return_5
rank_return_5/rank_return_10
pe_ttm_0
"""

# 模块接口定义
bigquant_category = "数据输入输出"
bigquant_friendly_name = "输入特征列表"
bigquant_doc_url = "https://bigquant.com/docs/"


def range_prama(range_prama_list):
    if len(range_prama_list) == 1:
        start = 0
        stop = range_prama_list[0]
        step = 1
    elif len(range_prama_list) == 2:
        start = range_prama_list[0]
        stop = range_prama_list[1]
        step = 1
    else:
        start = range_prama_list[0]
        stop = range_prama_list[1]
        step = range_prama_list[2]
    return start, stop, step


# WTF, TODO remove this code
def range_in_features(feature):
    try:
        range_features = []
        join_str = ""
        rename_str = ""
        find_equal = feature.find("=")
        find_left_brackets = feature.find("[")
        if find_equal != -1 and find_equal < find_left_brackets:
            # abc="+".join(['close_{}+turn_{}'.format(k,k) for k in range(0,4,1)])
            rename_str = feature[: find_equal + 1]
            feature = feature[find_equal + 1 :]
        find_join = feature.find(".join")
        if find_join != -1:
            join_str = feature[:find_join].replace('"', "").replace(" ", "")
            feature = feature[find_join + 5 :].strip()[1:-1].strip()
        ex_str = (
            feature[2:-1]
            .split("for ")[0]
            .replace(" ", "")
            .replace("'", "")
            .replace('"', "")
            .replace("+str", "str")
            .replace("+*", "*")
            .replace("++", "+")
            .replace("+-", "-")
            .replace("+-", "-")
        )
        if feature.find("zip") != -1:
            # ['exp(-1*{})*return_{}*return_{})'.format(k,j,l) for k,j,l in zip(range(5),range(1,10,2),range(1,4,1))]
            patterns = re.compile(r"\(.*?\)").findall(feature[2:-1].split("for ")[1].split("zip")[-1].strip()[1:-1])
            range_lsit = []
            for pattern in patterns:
                range_prama_list = pattern.replace(" ", "")[1:-1].split(",")
                start, stop, step = range_prama(range_prama_list)
                range_lsit.append(range(int(start), int(stop), int(step)))
            range_features.extend([ex_str[: ex_str.find(".format")].format(*N) for N in zip(*range_lsit)])
        else:
            # ['close_{}+turn_{}'.format(k,k) for k in range(0,4,1)]
            # ['close_'+str(N)+'*turn_'+str(N)+'*'+str(N) for N in range(10)]
            range_prama_list = feature[2:-1].split("for ")[1].split("range")[-1].replace(" ", "").replace("(", "").replace(")", "").split(",")
            start, stop, step = range_prama(range_prama_list)

            for N in range(int(start), int(stop), int(step)):
                if ex_str.find(".format") != -1:
                    new_ex_str = ex_str[: ex_str.find(".format")].replace("{}", str(N))
                else:
                    new_ex_str = re.sub(re.compile(r"str\(.*?\)"), str(N), ex_str)
                range_features.append(new_ex_str)
        if join_str:
            # "+".join(['close_{}'.format(k) for k in range(3)])
            range_features = join_str.join(range_features).replace("'", "").replace('"', "")
            if rename_str:
                range_features = rename_str + range_features

        return range_features
    except BaseException:
        return [feature]


def features_duplicate_removal(features):
    result_features = list()
    seen_features = set()
    for feature in features:
        if feature in seen_features:
            continue
        else:
            if feature.find("range") != -1:
                feature = range_in_features(feature)
                if isinstance(feature, list):
                    seen_features.update(feature)
                    result_features.extend(feature)
                    continue
            seen_features.add(feature)
            result_features.append(feature)
    return result_features


def bigquant_run(
    features: I.code("特征数据", default=DEFAULT_FEATURES, auto_complete_type="feature_fields,bigexpr_functions"),
    features_ds: I.port("特征输入，通过输入端和参数方式输入的特征将做合并", optional=True) = None,
) -> [I.port("输出数据", "data"),]:
    """

    输入特征（因子）数据

    """
    features = smart_list(features)
    if features_ds:
        features += smart_object(features_ds)

        can_sort = True
        for feature in features:
            if "=" in feature:
                can_sort = False
        if can_sort:
            features = sorted(set(features))
        else:
            features = features_duplicate_removal(features)
    else:
        features = features_duplicate_removal(features)
    return Outputs(data=DataSource.write_pickle(features))


def bigquant_postrun(outputs):
    return outputs


def bigquant_cache_key(kwargs):
    if kwargs.get("features", None) is not None:
        features = smart_list(kwargs["features"], sort=False)

        # 如果没有任何带别名的特征，则可以排序缓存key，否则不排序
        can_sort = True
        for feature in features:
            if "=" in feature:
                can_sort = False
        if can_sort:
            features = sorted(set(features))
        else:
            features = features_duplicate_removal(features)

        kwargs["features"] = features

    return kwargs


if __name__ == "__main__":
    # 测试代码
    pass
