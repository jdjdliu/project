from .perf_render import render as render_stock
from .perf_render_future import render as render_future
from .globalvalue import GlobalValue

# try:
#     from bigfolio import pyfolio as pf
#     pyfolio_import = True
# except ImportError:
#     pyfolio_import = False


def display(outputs, plot_series_compare=None):
    algo_result = outputs.raw_perf.read()
    # @20190215 修复期货回测命中缓存时GlobalValue默认仍为股票的问题
    product_type = outputs.get_attr('product_type', 'stock')
    data_frequency = outputs.get_attr('data_frequency', 'daily')
    perf_raw_object = outputs.get_attr('perf_raw_object', 0)
    if product_type == 'stock' or product_type == 'dcc':
        render_stock(algo_result, outputs.order_price_field_buy, outputs.order_price_field_sell,
                     round_num=GlobalValue.get_round_num(), data_frequency=data_frequency,
                     plot_series_compare=plot_series_compare, perf_raw_object=perf_raw_object)
    else:
        render_future(algo_result, outputs.order_price_field_buy, outputs.order_price_field_sell,
                      round_num=GlobalValue.get_round_num(), data_frequency=data_frequency, perf_raw_object=perf_raw_object)


def read_raw_perf(outputs):
    return outputs.raw_perf.read()


def read_data_panel(outputs):
    return outputs.data_panel.read()


def pyfolio_full_tear_sheet(outputs):
    try:
        from bigfolio import pyfolio as pf
    except ImportError:
        print("import pyfolio")
        import pyfolio as pf

    perf = outputs.read_raw_perf()

    # @20180920 benchmark daily returns
    perf['benchmark_returns'] = perf.benchmark_period_return.diff()
    perf['benchmark_returns'].iloc[0] = 0.0

    returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(
        perf)
    pf.tears.create_full_tear_sheet(returns, positions, transactions, gross_lev=gross_lev,
                                    benchmark_rets=perf.benchmark_returns)


def risk_analyze(outputs, return_values=False):
    result_df = outputs.read_raw_perf()
    from .risk_analyze import factor_analyze, industry_analyze
    rank_factor_values, absolute_factor_values = factor_analyze(
        result_df, return_values=True)
    industry_factor = industry_analyze(result_df, return_values=True)
    if return_values:
        return rank_factor_values, absolute_factor_values, industry_factor


def factor_profit_analyze(outputs):
    result_df = outputs.read_raw_perf()
    from .factor_return_analyze import factor_return_analyze
    factor_return_analyze(result_df)


def brinson_analysis(outputs):
    result_df = outputs.read_raw_perf()
    result_df = result_df[outputs.__start_date:outputs.__end_date]
    from .brinson_analysis import BrinsonAnalysis
    ret = BrinsonAnalysis(result_df)
    return ret.plot_periods_return_analysis()
