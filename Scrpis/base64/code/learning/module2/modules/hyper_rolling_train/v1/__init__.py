# -*- coding: utf-8 -*-
from functools import partial

import learning.api.tools as T
import learning.module2.common.interface as I
from learning.module2.common.data import Outputs
from sdk.utils import BigLogger

# log = logbook.Logger('自定义运行')
log = BigLogger("自定义运行")
bigquant_cacheable = False

# 模块接口定义
bigquant_category = "高级优化"
bigquant_friendly_name = "滚动训练"
bigquant_doc_url = "https://bigquant.com/docs/"


DEFAULT_RUN = r"""def bigquant_run(
    bq_graph,
    inputs,
    trading_days_market='CN', # 使用那个市场的交易日历, TODO
    train_instruments_mid='m1', # 训练数据 证券代码列表 模块id
    test_instruments_mid='m9', # 测试数据 证券代码列表 模块id
    predict_mid='m8', # 预测 模块id
    trade_mid='m19', # 回测 模块id
    start_date='2014-01-01', # 数据开始日期
    end_date=T.live_run_param('trading_date', '2017-01-01'), # 数据结束日期
    train_update_days=250, # 更新周期，按交易日计算，每多少天更新一次
    train_update_days_for_live=None, #模拟实盘模式下的更新周期，按交易日计算，每多少天更新一次。如果需要在模拟实盘阶段使用不同的模型更新周期，可以设置这个参数
    train_data_min_days=250, # 最小数据天数，按交易日计算，所以第一个滚动的结束日期是 从开始日期到开始日期+最小数据天数
    train_data_max_days=250, # 最大数据天数，按交易日计算，0，表示没有限制，否则每一个滚动的开始日期=max(此滚动的结束日期-最大数据天数, 开始日期
    rolling_count_for_live=1, #实盘模式下滚动次数，模拟实盘模式下，取最后多少次滚动。一般在模拟实盘模式下，只用到最后一次滚动训练的模型，这里可以设置为1；如果你的滚动训练数据时间段很短，以至于期间可能没有训练数据，这里可以设置大一点。0表示没有限制
):
    def merge_datasources(input_1):
        df_list = [ds[0].read_df().set_index('date').loc[ds[1]:].reset_index() for ds in input_1]
        df = pd.concat(df_list)
        instrument_data = {
            'start_date': df['date'].min().strftime('%Y-%m-%d'),
            'end_date': df['date'].max().strftime('%Y-%m-%d'),
            'instruments': list(set(df['instrument'])),
        }
        return Outputs(data=DataSource.write_df(df), instrument_data=DataSource.write_pickle(instrument_data))

    def gen_rolling_dates(trading_days_market, start_date, end_date, train_update_days, train_update_days_for_live, train_data_min_days, train_data_max_days, rolling_count_for_live):
        # 是否实盘模式
        tdays = list(D.trading_days(market=trading_days_market, start_date=start_date, end_date=end_date)['date'])
        is_live_run = T.live_run_param('trading_date', None) is not None

        if is_live_run and train_update_days_for_live:
            train_update_days = train_update_days_for_live

        rollings = []
        train_end_date = train_data_min_days
        while train_end_date < len(tdays):
            if train_data_max_days is not None and train_data_max_days > 0:
                train_start_date = max(train_end_date - train_data_max_days, 0)
            else:
                train_start_date = 0
            rollings.append({
                'train_start_date': tdays[train_start_date].strftime('%Y-%m-%d'),
                'train_end_date': tdays[train_end_date - 1].strftime('%Y-%m-%d'),
                'test_start_date': tdays[train_end_date].strftime('%Y-%m-%d'),
                'test_end_date': tdays[min(train_end_date + train_update_days, len(tdays)) - 1].strftime('%Y-%m-%d'),
            })
            train_end_date += train_update_days

        if not rollings:
            raise Exception('没有滚动需要执行，请检查配置')

        if is_live_run and rolling_count_for_live:
            rollings = rollings[-rolling_count_for_live:]

        return rollings

    g = bq_graph

    rolling_dates = gen_rolling_dates(
        trading_days_market, start_date, end_date, train_update_days, train_update_days_for_live, train_data_min_days, train_data_max_days, rolling_count_for_live)

    # 训练和预测
    results = []
    for rolling in rolling_dates:
        parameters = {}
        # 先禁用回测
        parameters[trade_mid + '.__enabled__'] = False
        parameters[train_instruments_mid + '.start_date'] = rolling['train_start_date']
        parameters[train_instruments_mid + '.end_date'] = rolling['train_end_date']
        parameters[test_instruments_mid + '.start_date'] = rolling['test_start_date']
        parameters[test_instruments_mid + '.end_date'] = rolling['test_end_date']
        # print('------ rolling_train:', parameters)
        results.append(g.run(parameters))

    # 合并预测结果并回测
    mx = M.cached.v3(run=merge_datasources, input_1=[[result[predict_mid].predictions, result[test_instruments_mid].data.read_pickle()['start_date']] for result in results])
    parameters = {}
    parameters['*.__enabled__'] = False
    parameters[trade_mid + '.__enabled__'] = True
    parameters[trade_mid + '.instruments'] = mx.instrument_data
    parameters[trade_mid + '.options_data'] = mx.data

    trade = g.run(parameters)

    return {'rollings': results, 'trade': trade}
"""


def bigquant_run(
    bq_graph_port: I.port("graph，可以重写全局传入的graph", optional=True) = None,
    input_1: I.port("输入1，run函数参数inputs的第1个元素", optional=True) = None,
    input_2: I.port("输入1，run函数参数inputs的第2个元素", optional=True) = None,
    input_3: I.port("输入1，run函数参数inputs的第3个元素", optional=True) = None,
    run: I.code("run函数", I.code_python, DEFAULT_RUN, specific_type_name="函数", auto_complete_type="python") = None,
    run_now: I.bool("即时执行，如果不勾选，此模块不会即时执行，并将当前行为打包为graph传入到后续模块执行") = True,
    bq_graph: I.bool("bq_graph，用于接收全局传入的graph，用户设置值无效") = True,
) -> [I.port("结果", "result"),]:
    """
    滚动训练模块可以实现训练集和测试集的定期更新轮换。在金融市场中，市场结构是时常变化的，因此模型需要不断训练，这也是滚动训练的出发点。一般而言，是随着时间的推移按固定的时间定期训练模型，比如训练集为2年时间，预测集为1年，模型更新时间为1年。那么由2010-2011年的数据训练出的模型在2012年数据上预测，由2011-2012年训练的模型在2013年数据上预测，依次类推，最后把每次预测的数据拼接起来，进行回测验证。
    """

    inputs = [input_1, input_2, input_3]

    if bq_graph_port is not None:
        bq_graph = bq_graph_port

    if run_now:
        result = run(bq_graph, inputs)
    else:
        result = T.GraphContinue(bq_graph, partial(run, inputs=inputs))

    return Outputs(result=result)


def bigquant_postrun(outputs):
    return outputs
