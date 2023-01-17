ALPHA_STOCK_TEMPLATE = """# 本代码由因子平台自动生成 {generated_at}
# 本代码单元只能在可视化模式下编辑。您也可以拷贝代码，粘贴到新建的代码单元或者策略，然后修改。


m1 = M.input_features.v1(
    features=\"\"\"
# #号开始的表示注释，注释需单独一行
# 多个特征，每行一个，可以包含基础特征和衍生特征，特征须为本平台特征
{expression}\"\"\"
)

m4 = M.factorlens.v2(
    features=m1.data,
    title='因子分析: {name}',
    start_date='{start_date}',
    end_date=T.live_run_param('trading_date', '{end_date}'),
    rebalance_period={rebalance_period},
    delay_rebalance_days={delay_rebalance_days},
    rebalance_price='{rebalance_price}',
    stock_pool='{stock_pool}',
    quantile_count={quantile_count},
    commission_rate={commission_rate:.4f},
    returns_calculation_method='{returns_calculation_method}',
    benchmark='{benchmark}',
    drop_new_stocks={drop_new_stocks},
    drop_price_limit_stocks={drop_price_limit_stocks},
    drop_st_stocks={drop_st_stocks},
    drop_suspended_stocks={drop_suspended_stocks},
    cutoutliers={cutoutliers},
    normalization={normalization},
    neutralization={neutralization},
    metrics={metrics},
    factor_coverage={factor_coverage:.2f},
    user_data_merge='{user_data_merge}'
)

"""

INDEX_STOCK_TEMPLATE = """# 本代码由因子平台自动生成 {generated_at}
from sdk.datasource import DataSource

m4 = M.index_custom.v1(
    input_1=DataSource('{factor_name}'),
    factor_name='alpha',
    weight_method='{weight_method}',
    stock_pool='{stock_pool}',
    benchmark='{benchmark}',
    sort='{sort}',
    rebalance_days={rebalance_days},
    cost={cost},
    quantile_ratio={quantile_ratio},
    stock_num={stock_num}
)
"""
