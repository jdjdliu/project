import pandas as pd


def truncate(df: pd.DataFrame, start, end, col="date", reset_index=True):
    col = df[col]
    if start is not None and end is not None:
        # [start, end]: include end for compatiable with truncate. TODO: refine this?
        cond = (col >= start) & (col <= end)
    elif start is not None:
        cond = col >= start
    elif end is not None:
        cond = col <= end
    else:
        cond = None

    if cond is not None:
        df = df[cond]

    if reset_index:
        df.reset_index(drop=True, inplace=True)

    return df


# depercated, TODO: use evalex
def queryex(df: pd.DataFrame, expr, **kwargs):
    # process column with special char quotes by `
    old_columns = df.columns

    special_columns = {}

    def _handle_special_column(m):
        column = m.group(1)
        if column not in df.columns:
            raise Exception("未知的列：%s" % column)
        if column not in special_columns:
            special_columns[column] = "__BIGQUANT_SPECIAL__%s" % len(special_columns)
        return special_columns[column]

    import re

    expr = re.sub("`([^`]*)`", _handle_special_column, expr)
    df.columns = [special_columns.get(col, col) for col in df.columns]

    result = df.query(expr, **kwargs)
    result.columns = old_columns
    df.columns = old_columns
    return result


def evalex(df: pd.DataFrame, expr, **kwargs):
    # process column with special char quotes by `
    old_columns = df.columns

    special_columns = {}

    def _handle_special_column(m):
        column = m.group(1)
        if column not in df.columns:
            raise Exception("未知的列：%s" % column)
        if column not in special_columns:
            special_columns[column] = "__BIGQUANT_SPECIAL__%s" % len(special_columns)
        return special_columns[column]

    import re

    expr = re.sub("`([^`]*)`", _handle_special_column, expr)
    df.columns = [special_columns.get(col, col) for col in df.columns]
    try:
        import bigexpr

        result = bigexpr.evaluate(df, expr)
    except Exception:
        result = df.eval(expr, **kwargs)
    df.columns = old_columns

    return result
