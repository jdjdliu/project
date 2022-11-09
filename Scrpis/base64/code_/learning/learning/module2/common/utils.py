import ast
import json
from multiprocessing import Process, Queue

import pandas as pd
from IPython.core.display import HTML
from IPython.display import Markdown, display

from learning.settings import jpy_user_name


def fork_run(func, *args, **kwargs):
    def func_wrapper(queue, func, args, kwargs):
        try:
            queue.put(func(*args, **kwargs))
        except Exception as e:
            queue.put(e)

    queue = Queue()
    p = Process(target=func_wrapper, args=(queue, func, args, kwargs))
    p.start()
    p.join()
    result = queue.get()
    if isinstance(result, Exception):
        raise result
    return result


def isinstance_with_name(obj, class_or_tuple):
    if isinstance(obj, class_or_tuple):
        return True
    else:
        if not isinstance(class_or_tuple, tuple):
            class_or_tuple = (class_or_tuple,)
        for _class in class_or_tuple:
            if str(type(obj)) == str(_class):
                return True
        return False


def smart_object(data):
    from learning.module2.common.data import DataSource

    if data is None:
        return data

    if isinstance_with_name(data, DataSource):
        data = data.read_pickle()

    return data


def smart_dict(text):
    if isinstance(text, str):
        if not text:
            ret = {}
        else:
            try:
                ret = json.loads(text.replace("'", '"'))
            except BaseException:
                ret = ast.literal_eval(text)
    else:
        ret = text
    assert isinstance(ret, dict)
    return ret


def smart_list(text, sep="\n", strip=True, drop_empty_lines=True, comment_prefix="#", cast_type=None, sort=False):
    if text is None:
        return text

    text = smart_object(text)

    if isinstance(text, str):
        import re

        lines = re.split(sep, text)
        if strip:
            lines = [line.strip() for line in lines]
        if drop_empty_lines:
            lines = [line for line in lines if line]
        if comment_prefix is not None:
            lines = [line for line in lines if not line.startswith(comment_prefix)]
    else:
        lines = text

    if cast_type:
        lines = [cast_type(line) for line in lines]
    if sort:
        lines = sorted(lines)

    return lines


def append_function(functions, func, **partial_kwargs):
    if functions is None:
        functions = []
    else:
        functions = functions.copy()

    from functools import partial

    functions.append(partial(func, **partial_kwargs))
    return functions


def check_sql(sql):
    # 检查sql的合法性,避免sql注入和修改数据库中的数据
    illegal_words = ["drop", "alter", "delete", "truncate", "update", "insert"]
    sql = sql.lower().replace("\n", " ")
    for words in illegal_words:
        if "{} ".format(words) in sql:
            raise Exception("您的SQL含有非法字符: {}, SQL: {}".format(words, sql))


def display_df(df, module_name="", lines=5, color="black", sub_title_color="DimGray", display_counter=True):
    """
    展示数据

    Args:
        df: DataFrame
        module_name: str 模块名称
        lines: int  default 5 数据预览行数
        color: str 颜色
        sub_title_color: str 颜色
        display_counter: bool 是否展示数据统计

    """
    if not jpy_user_name:
        return

    text = "###  <font color=" + color + ">{title} {kind}</font>"
    kind_text = "<font color='{color}' size=3> {sub_title} <font color='gray' size=3> (前 {count} 行)</font> </font>"
    # 展示列数据统计, 主要统计空值和数据类型
    if display_counter:
        display(Markdown(text.format(title=module_name, kind=kind_text.format(color=sub_title_color, sub_title="数据统计", count=len(df)))))
        dtypes = pd.DataFrame(df.dtypes, columns=["type"])
        count = pd.DataFrame(len(df) - df.count(), columns=["count(Nan)"])
        _df = pd.concat([count, dtypes], axis=1).stack().unstack(0)
        display(HTML(_df.to_html()))

    # 数据预览
    display(Markdown(text.format(title=module_name, kind=kind_text.format(color=sub_title_color, sub_title="数据预览", count=lines))))
    display(HTML(df.head(lines).to_html()))


def display_html(html, output="display"):
    """
    绘制html图表
    Params:
    -----
    html: 传入拼接好的 html
    output: 输出，script\display，script 输出html，不进行展示

    """
    if output == "script":
        return html
    from IPython.display import HTML, display

    display(HTML(html))


def generate_ds_with_chunk(chunk_df):
    """
    数据生成DataSource,保存

    Args:
        chunk_df: iter  pd.DataFrame对象iter

    Returns: ds, first_df

    """
    from sdk.datasource import STORAGE_KEY

    from learning.module2.common.data import DataSource

    ds = DataSource()
    store = ds.open_df_store()
    _first_df = pd.DataFrame()
    for index, df in enumerate(chunk_df):
        _first_df = _first_df if not _first_df.empty else df
        key = STORAGE_KEY if not index else "{}_{}".format(STORAGE_KEY, index + 1)
        store[key] = df
    ds.close_df_store()
    return ds, _first_df


def smart_datasource(input_data):
    from sdk.datasource import STORAGE_KEY

    from learning.module2.common.data import DataSource

    """转化不同类型的输入数据为DataSource"""
    if isinstance(input_data, pd.DataFrame):
        ds = DataSource()
        store = ds.open_df_store()
        store[STORAGE_KEY] = input_data
        ds.close_df_store()
        return ds
    return input_data


def check_user_max_workers(workers, log=None, remote_run=None):
    return {"workers": workers, "remote_run": remote_run}


if __name__ == "__main__":
    pass
