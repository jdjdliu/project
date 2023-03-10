# -*- coding: utf-8 -*-
# author:guti
from bigdata.common.historydatafielddefs import HistoryDataFieldDefs
from bigdata.common.market import index_constituent_list
from bigdata.common.market import Market


class Transformer:
    def __init__(self, expr_s):
        self.__expr_s = expr_s
        self.__expr = None

    def __call__(self, x):
        if self.__expr_s is None:
            return x
        import numpy as np
        import numexpr as ne
        if self.__expr is None:
            # accelerate with numexpr
            s = self.__expr_s
            if not 'replace' in s and 'clip' not in s and not 'ne.' in s:
                s = s.replace('np.', '')
                s = s.replace('lambda x: ', '')
                self.__expr = lambda x: ne.evaluate(s)
            else:
                self.__expr = eval(s, {'ne': ne, 'np': np}, {})
        return self.__expr(x)

    def __repr__(self):
        return str(self.__expr_s)


class _FEATURETUPE:
    def __init__(self, field, range, expr, desc, category, transform_for_ranker, tables='bar1d', table_name='',
                 base_field=None, type='float32', markets='all'):
        self.field = field
        self.range = range
        self.expr = expr
        self.desc = desc
        self.category = category
        self.tables = tables
        self.markets = self._parse_markets(markets)
        self.type = type
        self.base_field = base_field
        self.table_name = table_name
        # serilize lambda for multiprocessing
        self.__transform_for_ranker = Transformer(transform_for_ranker and 'lambda x: ' + transform_for_ranker)

    def _parse_markets(self, markets):
        if markets == 'all':
            markets = [x.symbol for x in Market.MG_ALL.sub_groups]
        elif isinstance(markets, str):
            markets = markets.split(',')
        elif not isinstance(markets, list):
            markets = [markets]
        return set(x if isinstance(x, str) else x.symbol for x in markets)

    def eval_expr(self, df, i, context=None):
        import pandas as pd, numpy as np
        if self.expr is None:
            return None
        if len(df) == 0:
            return pd.Series([])
        from talib import abstract as ta_abstract
        try:
            return eval(self.expr, ta_abstract.__dict__, {'df': df, 'i': i, 'context': context})
        except Exception as e:
            print('feature expr eval exception: %s, %s, %s' % (e, self.field, df))
            return pd.Series([np.NaN] * len(df))

    def expand_fields(self, with_params=False, low_1=None, up_1=None):
        if not self.range:
            return [(self.field, [])] if with_params else [self.field]

        # TODO: support more 2+D
        s = []
        for i in self.range:
            if low_1 is not None and i < low_1:
                continue
            if up_1 is not None and i >= up_1:
                continue
            if '$i' in self.field:
                name = self.field.replace('$i', str(i))
            else:
                name = '%s_%s' % (self.field, i)
            s.append((name, [i]) if with_params else name)
        return s

    @property
    def transform_for_ranker(self):
        return self.__transform_for_ranker


BAR_FIELDS = ['close', 'open', 'high', 'low', 'volume', 'amount']


def F(field, range, expr, desc, transform_for_ranker, markets=None, base_field=None, type='float32'):
    if base_field is None and not field.startswith('rank_'):
        base_field = field
    if markets:
        return lambda category, default_markets, tables, table_name: _FEATURETUPE(field, range, expr, desc, category,
                                                                                  transform_for_ranker,
                                                                                  tables, table_name, base_field, type,
                                                                                  markets)
    else:
        return lambda category, default_markets, tables, table_name: _FEATURETUPE(field, range, expr, desc, category,
                                                                                  transform_for_ranker,
                                                                                  tables, table_name, base_field, type,
                                                                                  default_markets)


def H(field, expr=None, desc=None, rank=False, transform_for_ranker=None, markets=None, range=[0], base_field=None,
      type='float32'):
    # make feature form history fields
    if field in HistoryDataFieldDefs.FIELD_MAP:
        f = HistoryDataFieldDefs.FIELD_MAP[field]
        if expr is None: expr = 'df.%s' % f.field
        if desc is None: desc = f.desc
        field = f.field
    if base_field is None:
        base_field = field
    feature = F(field, range, expr, desc, transform_for_ranker=transform_for_ranker, markets=markets,
                base_field=base_field, type=type)
    if not rank: return feature
    feature_rank = F('rank_' + field, range, None, desc + '????????????????????????', transform_for_ranker='x*10000', markets=markets,
                     base_field=base_field, type=type)
    return [feature, feature_rank]


def __make(items):
    category = ''
    markets = 'all'
    results = []
    tables = ''
    table_name = ''
    for item in items:
        if isinstance(item, str):
            if item.startswith('category:'):
                category = item[9:].strip()
            elif item.startswith('markets:'):
                markets = item[8:].strip()
            elif item.startswith('tables:'):
                tables = item[7:].strip()
            elif item.startswith('table_name:'):
                table_name = item[11:]
        elif isinstance(item, list):
            results.extend([f(category, markets, tables, table_name) for f in item])
        else:
            results.append(item(category, markets, tables, table_name))
    return results


def _r(start, stop, step=1):
    return list(range(start, stop, step))


## NOTTE??? ??????????????? bigdocs/docs/deps/updatedeps.py

R_0 = [0]
R_1 = _r(0, 20 + 1)
R_2 = _r(0, 120 + 1)
R_3 = _r(0, 20) + _r(20, 90, 10) + _r(90, 360 + 1, 30)

# TRANSFORM_SLOG = ' (np.sign(x)*np.log(np.abs(x)).replace(-np.inf, 0) + 25) * 10000'
TRANSFORM_SLOG = 'ne.evaluate("(log(where(x > 1, x, where(x < -1, -1/x, 1))) + 30) * 10000")'
_FEATURES = __make([
    '',
    F('date', None, 'df.date', '????????????', transform_for_ranker=None),
    F('instrument', None, 'df.instrument', '????????????', transform_for_ranker=None),

    'category:????????????',
    'markets:CN_STOCK_A',
    'tables:bar1d_index,industry,basic_info,stock_status,index_constituent',
    'table_name:features_basic_info',
    F('list_days', R_0, '(df.date - df.list_date).dt.days', '??????????????????????????????????????????', transform_for_ranker=None,
      markets='CN_STOCK_A,US_STOCK,HK_STOCK', base_field='list_date'),
    H('list_board', expr='df.list_board.map({"??????": 1, "???????????????": 2, "?????????": 3})', desc='?????????????????????1?????????????????????2???????????????3',
      transform_for_ranker=None),
    F('company_found_date', R_0, '(df.date - df.company_found_date).dt.days', '??????????????????', transform_for_ranker=None,
      base_field='company_found_date'),
    H('st_status', transform_for_ranker=None),
    H('industry_sw_level1'),
    H('industry_sw_level2'),
    H('industry_sw_level3'),
    *[
        H(x.feature_name) for x in index_constituent_list
    ],

    'category:????????????',
    'markets:CN_STOCK_A',
    'tables:bar1d,stock_status',
    'table_name:features_bar1d',
    F('open', R_1, 'df.open.shift(i)', '??????i????????????????????????????????????0', transform_for_ranker='x*100', base_field='open'),
    F('high', R_1, 'df.high.shift(i)', '??????i????????????????????????', transform_for_ranker='x*100', base_field='high'),
    F('low', R_1, 'df.low.shift(i)', '??????i????????????????????????', transform_for_ranker='x*100', base_field='low'),
    F('volume', R_1, 'df.volume.shift(i)', '??????i????????????????????????', transform_for_ranker='x/10000', base_field='volume'),
    F('adjust_factor', R_1, 'df.adjust_factor.shift(i)', '??????i???????????????????????????', transform_for_ranker='x*10',
      base_field='adjust_factor',
      markets='CN_STOCK_A,CN_FUND,US_STOCK,HK_STOCK'),
    F('deal_number', R_1, 'df.deal_number.shift(i)', '??????i???????????????????????????', transform_for_ranker=None, markets='CN_STOCK_A',
      base_field='deal_number'),
    F('price_limit_status', R_1, 'df.price_limit_status.shift(i)', '??????i??????????????????????????????????????????????????????1???????????????2?????????????????????3???????????????',
      base_field='price_limit_status',
      transform_for_ranker=None, markets='CN_STOCK_A,CN_FUND'),

    F('close', R_2, 'df.close.shift(i)', '??????i????????????????????????', transform_for_ranker='x*100', base_field='close'),
    F('daily_return', R_3, 'df.close.shift(i) / df.close.shift(i + 1)', '??????i????????????????????????=close_i/close_(i+1)',
      base_field='close',
      transform_for_ranker='x*10000'),
    F('return', R_3, 'df.close / df.close.shift(i + 1)', '??????i????????????????????????=close_0/close_(i+1)', base_field='close',
      transform_for_ranker='x*10000'),
    F('rank_return', R_3, None, '??????i????????????????????? (return_i) ?????????=????????????????????????/??????', transform_for_ranker='x*10000'),

    F('amount', R_2, 'df.amount.shift(i)', '??????i????????????????????????', transform_for_ranker='x/10000', base_field='amount'),
    F('rank_amount', R_2, None, '??????i???????????????????????????????????????', transform_for_ranker='x*10000'),
    F('avg_amount', R_3, 'df.amount.rolling(i + 1).mean()', '??????i?????????????????????????????????0????????????', transform_for_ranker='x/10000',
      base_field='amount'),
    F('rank_avg_amount', R_3, None, '??????i????????????????????????????????????????????????', transform_for_ranker='x*10000'),

    'category:???????????????',
    'markets:CN_FUND,CN_STOCK_A,US_STOCK,HK_STOCK',
    'tables:bar1d',
    'table_name:features_turn',
    F('turn', R_1, 'df.turn.shift(i)', '??????i????????????????????????', transform_for_ranker='x*10000', base_field='turn'),
    F('rank_turn', R_1, None, '??????i???????????????????????? (turn_i) ?????????=????????????????????????/??????', transform_for_ranker='x*10000'),
    F('avg_turn', R_3, 'df.turn.rolling(i+1).mean()', '??????i?????????????????????????????????0????????????', transform_for_ranker='x*10000',
      base_field='turn'),
    F('rank_avg_turn', R_3, None, '??????i???????????????????????????????????????=????????????????????????/??????', transform_for_ranker='x*10000'),

    'category:????????????',
    'markets:CN_STOCK_A,US_STOCK,HK_STOCK',
    'tables:market_value,west',
    'table_name:features_mw',
    H('market_cap', rank=True, transform_for_ranker='x/1000000'),
    H('market_cap_float', rank=True, transform_for_ranker='x/1000000'),
    H('pe_ttm', rank=True, transform_for_ranker='x.clip(-1000, 1000) * 10 + x/1000 + 20000'),
    H('pe_lyr', rank=True, transform_for_ranker='x.clip(-1000, 1000) * 10 + x/1000 + 20000'),
    H('pb_lf', rank=True, transform_for_ranker='x.clip(-1000, 1000) * 10 + x/1000 + 20000', markets='CN_STOCK_A'),
    H('pb_mrq', rank=True, transform_for_ranker='x.clip(-1000, 1000) * 10 + x/1000 + 20000',
      markets='US_STOCK,HK_STOCK'),
    H('ps_ttm', rank=True, transform_for_ranker='x.clip(-1000, 1000) * 10 + x/1000 + 20000'),

    H('west_netprofit_ftm', markets='CN_STOCK_A'),
    H('west_eps_ftm', markets='CN_STOCK_A'),
    H('west_avgcps_ftm', markets='CN_STOCK_A'),

    'category:?????????',
    'markets:CN_STOCK_A',
    'tables:net_amount',
    'table_name:features_na',
    F('mf_net_amount', R_1, 'df.mf_net_amount.shift(i)', '??????i?????????????????????????????????= ???????????? - ???????????? (??????????????????????????????????????????)',
      base_field='mf_net_amount',
      transform_for_ranker=TRANSFORM_SLOG),
    F('avg_mf_net_amount', R_1, 'df.mf_net_amount.rolling(i+1).mean()', '??????i????????????????????????????????????', base_field='mf_net_amount',
      transform_for_ranker=TRANSFORM_SLOG),
    F('rank_avg_mf_net_amount', R_1, 'df.mf_net_amount.rolling(i+1).mean()', '??????i??????????????????????????????????????????',
      transform_for_ranker='x*10000'),
    H('mf_net_amount_main'),
    H('mf_net_pct_main'),
    H('mf_net_amount_xl'),
    H('mf_net_pct_xl'),
    H('mf_net_amount_l'),
    H('mf_net_pct_l'),
    H('mf_net_amount_m'),
    H('mf_net_pct_m'),
    H('mf_net_amount_s'),
    H('mf_net_pct_s'),

    'category:????????????',
    'markets:CN_STOCK_A',
    'tables:financial_statement',
    'table_name:features_fs',
    H('fs_publish_date', '(df.date - df.fs_publish_date).dt.days', '???????????????????????????????????????????????????????????????0', transform_for_ranker=None),
    H('fs_quarter_year', 'df.fs_quarter_year.replace(0, float("nan"))', '?????????????????????', transform_for_ranker=None),
    H('fs_quarter_index', 'df.fs_quarter_index.replace(0, float("nan"))', '?????????????????????????????? 1/2/3/4???1?????????????????????????????????',
      transform_for_ranker=None),
    H('fs_net_profit', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_profit_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_profit_yoy', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_profit_qoq', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_deducted_profit', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_deducted_profit_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_roe', rank=True, transform_for_ranker='x * 10 + 10000'),
    H('fs_roe_ttm', rank=True, transform_for_ranker='x * 10 + 10000'),
    H('fs_roa', rank=True, transform_for_ranker='x * 10 + 10000'),
    H('fs_roa_ttm', rank=True, transform_for_ranker='x * 10 + 10000'),
    H('fs_gross_profit_margin', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_gross_profit_margin_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_profit_margin', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_profit_margin_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_operating_revenue', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_operating_revenue_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_operating_revenue_yoy', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_operating_revenue_qoq', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_free_cash_flow', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_cash_flow', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_net_cash_flow_ttm', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_eps', rank=True, transform_for_ranker='x * 100 + 1000'),
    H('fs_eps_yoy', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_bps', rank=True, transform_for_ranker='x * 10 + 10000'),
    H('fs_current_assets', transform_for_ranker='x/1000000'),
    H('fs_non_current_assets', transform_for_ranker='x/1000000'),
    H('fs_current_liabilities', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_non_current_liabilities', transform_for_ranker=TRANSFORM_SLOG),
    H('fs_cash_ratio', rank=True, transform_for_ranker=TRANSFORM_SLOG),
    H('fs_common_equity', transform_for_ranker=TRANSFORM_SLOG),

    H('fs_cash_equivalents'),
    H('fs_account_receivable'),
    H('fs_fixed_assets'),
    H('fs_proj_matl'),
    H('fs_construction_in_process'),
    H('fs_fixed_assets_disp'),

    H('fs_account_payable'),
    H('fs_total_liability'),

    H('fs_paicl_up_capital'),
    H('fs_capital_reserves'),
    H('fs_surplus_reserves'),
    H('fs_undistributed_profit'),
    H('fs_eqy_belongto_parcomsh'),
    H('fs_total_equity'),

    H('fs_gross_revenues'),
    H('fs_total_operating_costs'),
    H('fs_selling_expenses'),
    H('fs_financial_expenses'),
    H('fs_general_expenses'),
    H('fs_operating_profit'),
    H('fs_total_profit'),
    H('fs_income_tax'),
    H('fs_net_income'),

    'category:????????????',
    'markets:CN_STOCK_A',
    'tables:financial_statement',
    'table_name:features_gd',
    H('sh_holder_avg_pct', rank=True, transform_for_ranker='x*10000'),
    H('sh_holder_avg_pct_3m_chng', rank=True, transform_for_ranker='x*100 + 1000'),
    H('sh_holder_avg_pct_6m_chng', rank=True, transform_for_ranker='x*100 + 1000'),
    H('sh_holder_num', rank=True, transform_for_ranker='x'),

    'category:??????????????????',
    'markets:all',
    'tables:bar1d',
    'table_name:features_js',
    F('ta_sma_$i_0', [5, 10, 20, 30, 60], 'MA(context.ta_input_data, timeperiod=i)', '????????????i????????????????????????',
      transform_for_ranker='x*10', base_field=BAR_FIELDS),
    F('ta_ema_$i_0', [5, 10, 20, 30, 60], 'EMA(context.ta_input_data, timeperiod=i)', '????????????i????????????????????????',
      transform_for_ranker='x*10', base_field=BAR_FIELDS),
    F('ta_wma_$i_0', [5, 10, 20, 30, 60], 'WMA(context.ta_input_data, timeperiod=i)', '????????????i????????????????????????',
      transform_for_ranker='x*10', base_field=BAR_FIELDS),
    F('ta_ad', [0], 'AD(context.ta_input_data)', '??????????????????', transform_for_ranker=TRANSFORM_SLOG, base_field=BAR_FIELDS),
    F('ta_aroon_down_$i_0', [14, 28], 'context.ta("AROON", i)[0]', '????????????aroondown???timeperiod=i', base_field=BAR_FIELDS,
      transform_for_ranker='x*100'),
    F('ta_aroon_up_$i_0', [14, 28], 'context.ta("AROON", i)[1]', '????????????aroonup???timeperiod=i', base_field=BAR_FIELDS,
      transform_for_ranker='x*100'),
    F('ta_aroonosc_$i_0', [14, 28], 'AROONOSC(context.ta_input_data, timeperiod=i)', 'AROONOSC?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_atr_$i_0', [14, 28], 'ATR(context.ta_input_data, timeperiod=i)', 'ATR?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_bbands_upperband_$i_0', [14, 28], 'context.ta("BBANDS", i)[0]', 'BBANDS?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_bbands_middleband_$i_0', [14, 28], 'context.ta("BBANDS", i)[1]', 'BBANDS?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_bbands_lowerband_$i_0', [14, 28], 'context.ta("BBANDS", i)[2]', 'BBANDS?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_adx_$i_0', [14, 28], 'ADX(context.ta_input_data, timeperiod=i)', 'ADX?????????timeperiod=i',
      transform_for_ranker='x*100', base_field=BAR_FIELDS),
    F('ta_cci_$i_0', [14, 28], 'CCI(context.ta_input_data, timeperiod=i)', 'CCI?????????timeperiod=i',
      transform_for_ranker='(x+1000)*100', base_field=BAR_FIELDS),
    F('ta_macd_macd_12_26_9', [0], 'context.ta("MACD")[0]', 'MACD', transform_for_ranker='x*100 + 10000000',
      base_field=BAR_FIELDS),
    F('ta_macd_macdsignal_12_26_9', [0], 'context.ta("MACD")[1]', 'MACD', transform_for_ranker='x*100 + 10000000',
      base_field=BAR_FIELDS),
    F('ta_macd_macdhist_12_26_9', [0], 'context.ta("MACD")[2]', 'MACD', transform_for_ranker='x*100 + 10000000',
      base_field=BAR_FIELDS),
    F('ta_obv', [0], 'context.ta("OBV")', 'OBV??????', transform_for_ranker='np.log(x)*100', base_field=BAR_FIELDS),
    F('ta_stoch_slowk_5_3_0_3_0', [0], 'context.ta("STOCH")[0]', 'STOCH (KDJ) ??????K???', transform_for_ranker='x * 100',
      base_field=BAR_FIELDS),
    F('ta_stoch_slowd_5_3_0_3_0', [0], 'context.ta("STOCH")[1]', 'STOCH (KDJ) ??????D???', transform_for_ranker='x * 100',
      base_field=BAR_FIELDS),
    F('ta_mfi_$i_0', [14, 28], 'MFI(context.ta_input_data, timeperiod=i)', 'MFI?????????timeperiod=i', base_field=BAR_FIELDS,
      transform_for_ranker='x*100'),
    F('ta_rsi_$i_0', [14, 28], 'RSI(context.ta_input_data, timeperiod=i)', 'RSI?????????timeperiod=i', base_field=BAR_FIELDS,
      transform_for_ranker='x*100'),
    F('ta_trix_$i_0', [14, 28], 'TRIX(context.ta_input_data, timeperiod=i)', 'TRIX?????????timeperiod=i',
      base_field=BAR_FIELDS,
      transform_for_ranker='x*100+10000'),
    F('ta_sar', [0], 'SAR(context.ta_input_data)', 'SAR??????', transform_for_ranker='x*10', base_field=BAR_FIELDS),
    F('ta_mom_$i_0', [10, 20, 30, 60], 'MOM(context.ta_input_data, timeperiod=i)', 'MOM?????????timperiod=i',
      base_field=BAR_FIELDS,
      transform_for_ranker=TRANSFORM_SLOG),
    F('ta_willr_$i_0', [14, 28], 'WILLR(context.ta_input_data, timeperiod=i)', 'WILLR?????????timeperiod=i',
      base_field=BAR_FIELDS,
      transform_for_ranker='x * 100 + 200'),

    'category:?????????',
    'markets:all',
    'tables:bar1d',
    'table_name:features_volatility',
    H('swing_volatility_$i_0', range=[5, 10, 30, 60, 120, 240],
      expr='((df.high - df.low)/df.close).rolling(center=False, window=i).std()', desc='??????????????????timeperiod=i', rank=True,
      transform_for_ranker='x * 1000', base_field=BAR_FIELDS),
    H('volatility_$i_0', range=[5, 10, 30, 60, 120, 240],
      expr='(df.close/df.close.shift(1)-1).rolling(center=False, window=i).std()', desc='????????????timeperiod=i', rank=True,
      transform_for_ranker='x * 1000', base_field=BAR_FIELDS),

    'category:BETA???',
    'markets:CN_STOCK_A',
    'tables:bar1d_index,industry,bar1d',
    'table_name:features_beta',
    H('beta_sse50_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000016.SHA" ,timeperiod=i)',
      desc='BETA???(??????50)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_csi300_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000300.SHA" ,timeperiod=i)',
      desc='BETA???(??????300)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_csi500_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000905.SHA" ,timeperiod=i)',
      desc='BETA???(??????500)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_csi800_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000906.SHA" ,timeperiod=i)',
      desc='BETA???(??????800)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_sse180_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000010.SHA" ,timeperiod=i)',
      desc='BETA???(??????180)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_csi100_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000903.SHA" ,timeperiod=i)',
      desc='BETA???(??????100)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_szzs_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("000001.SHA" ,timeperiod=i)',
      desc='BETA???(????????????)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_gem_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("399006.SZA" ,timeperiod=i)',
      desc='BETA???(?????????)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
    H('beta_industry_$i_0', range=[5, 10, 30, 60, 90, 120, 180], expr='context.beta("INDUSTRY" ,timeperiod=i)',
      desc='BETA???(????????????)???timeperiod=i', rank=True, transform_for_ranker='x * 100', base_field=BAR_FIELDS),
])


class FeatureDefs:
    FEATURE_LIST = _FEATURES

    FEATURE_MAP = {f.field: f for f in FEATURE_LIST}

    # ?????????????????????????????????????????????????????????1 ?????? shift???????????????????????????FEATURE??????expr???????????????
    # FEATURE_DAYS_LOOK_BACK = int((max(max(f.range) if f.range else 0 for f in FEATURE_LIST if not f.field.startswith('rank_')) + 1)*1.5)
    max_range = 0
    for f in FEATURE_LIST:
        if not f.field.startswith('rank_'):
            if f.range and max(f.range) > max_range:
                max_range = max(f.range)
    FEATURE_DAYS_LOOK_BACK = int((max_range + 1) * 1.5)


if __name__ == '__main__':
    # test code
    print(FeatureDefs.FEATURE_DAYS_LOOK_BACK)
    import pandas as pd
    import datetime
    import numpy as np

    arr = pd.Series(np.random.rand(1000000) * 1000)
    s = datetime.datetime.now()
    for f in FeatureDefs.FEATURE_LIST:
        f.transform_for_ranker(arr)

    print((datetime.datetime.now() - s).total_seconds())
