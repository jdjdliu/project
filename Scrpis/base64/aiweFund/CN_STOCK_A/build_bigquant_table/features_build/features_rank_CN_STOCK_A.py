import os
import sys
import pandas as pd


BASE_DIR = os.path.dirname((os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

print(sys.path)
from common_features.utils import truncate, args, log
from common_features.features import FeatureDefs
from bigdatasource.api import DataSource, UpdateDataSource
from bigdata.api.datareader import D

# from bigdatabuilder.settings import DAY_FORMAT
DAY_FORMAT = "%Y-%m-%d"


class Build(object):

    start_date = args.start_date
    end_date = args.end_date
    markets = args.markets or ['CN_STOCK_A']

    # 原始数据表
    original_data_tables = ['features_CN_STOCK_A_G300', 'features_CN_STOCK_A_G301', 'features_CN_STOCK_A_G302']

    def __init__(self):
        self.log = log
        self.__main_feature_list = [f for f in FeatureDefs.FEATURE_LIST if not f.field.startswith('rank_')]
        self.__rank_feature_list = [f for f in FeatureDefs.FEATURE_LIST if f.field.startswith('rank_')]
        self.features_fields_map = {
            'features_CN_STOCK_A_G310': ['date', 'instrument', 'rank_amount_0', 'rank_amount_1', 'rank_amount_10',
                                         'rank_amount_11',
                                         'rank_amount_12', 'rank_amount_13', 'rank_amount_14', 'rank_amount_15',
                                         'rank_amount_16',
                                         'rank_amount_17', 'rank_amount_18', 'rank_amount_19', 'rank_amount_2',
                                         'rank_amount_20',
                                         'rank_amount_3', 'rank_amount_4', 'rank_amount_5', 'rank_amount_6',
                                         'rank_amount_7', 'rank_amount_8',
                                         'rank_amount_9', 'rank_avg_amount_0', 'rank_avg_amount_1',
                                         'rank_avg_amount_10', 'rank_avg_amount_11',
                                         'rank_avg_amount_12', 'rank_avg_amount_13', 'rank_avg_amount_14',
                                         'rank_avg_amount_15',
                                         'rank_avg_amount_16', 'rank_avg_amount_17', 'rank_avg_amount_18',
                                         'rank_avg_amount_19',
                                         'rank_avg_amount_2', 'rank_avg_amount_20', 'rank_avg_amount_3',
                                         'rank_avg_amount_4',
                                         'rank_avg_amount_5', 'rank_avg_amount_6', 'rank_avg_amount_7',
                                         'rank_avg_amount_8',
                                         'rank_avg_amount_9', 'rank_avg_mf_net_amount_0', 'rank_avg_mf_net_amount_1',
                                         'rank_avg_mf_net_amount_10', 'rank_avg_mf_net_amount_11',
                                         'rank_avg_mf_net_amount_12',
                                         'rank_avg_mf_net_amount_13', 'rank_avg_mf_net_amount_14',
                                         'rank_avg_mf_net_amount_15',
                                         'rank_avg_mf_net_amount_16', 'rank_avg_mf_net_amount_17',
                                         'rank_avg_mf_net_amount_18',
                                         'rank_avg_mf_net_amount_19', 'rank_avg_mf_net_amount_2',
                                         'rank_avg_mf_net_amount_20',
                                         'rank_avg_mf_net_amount_3', 'rank_avg_mf_net_amount_4',
                                         'rank_avg_mf_net_amount_5',
                                         'rank_avg_mf_net_amount_6', 'rank_avg_mf_net_amount_7',
                                         'rank_avg_mf_net_amount_8',
                                         'rank_avg_mf_net_amount_9', 'rank_avg_turn_0', 'rank_avg_turn_1',
                                         'rank_avg_turn_10',
                                         'rank_avg_turn_11', 'rank_avg_turn_12', 'rank_avg_turn_13', 'rank_avg_turn_14',
                                         'rank_avg_turn_15',
                                         'rank_avg_turn_16', 'rank_avg_turn_17', 'rank_avg_turn_18', 'rank_avg_turn_19',
                                         'rank_avg_turn_2',
                                         'rank_avg_turn_20', 'rank_avg_turn_3', 'rank_avg_turn_4', 'rank_avg_turn_5',
                                         'rank_avg_turn_6',
                                         'rank_avg_turn_7', 'rank_avg_turn_8', 'rank_avg_turn_9', 'rank_fs_bps_0',
                                         'rank_fs_cash_ratio_0',
                                         'rank_fs_eps_0', 'rank_fs_eps_yoy_0', 'rank_fs_net_profit_qoq_0',
                                         'rank_fs_net_profit_yoy_0',
                                         'rank_fs_operating_revenue_qoq_0', 'rank_fs_operating_revenue_yoy_0',
                                         'rank_fs_roa_0',
                                         'rank_fs_roa_ttm_0', 'rank_fs_roe_0', 'rank_fs_roe_ttm_0', 'rank_market_cap_0',
                                         'rank_market_cap_float_0', 'rank_pb_lf_0', 'rank_pe_lyr_0', 'rank_pe_ttm_0',
                                         'rank_ps_ttm_0',
                                         'rank_return_0', 'rank_return_1', 'rank_return_10', 'rank_return_11',
                                         'rank_return_12',
                                         'rank_return_13', 'rank_return_14', 'rank_return_15', 'rank_return_16',
                                         'rank_return_17',
                                         'rank_return_18', 'rank_return_19', 'rank_return_2', 'rank_return_20',
                                         'rank_return_3',
                                         'rank_return_4', 'rank_return_5', 'rank_return_6', 'rank_return_7',
                                         'rank_return_8', 'rank_return_9',
                                         'rank_sh_holder_avg_pct_0', 'rank_sh_holder_avg_pct_3m_chng_0',
                                         'rank_sh_holder_avg_pct_6m_chng_0',
                                         'rank_sh_holder_num_0', 'rank_turn_0', 'rank_turn_1', 'rank_turn_10',
                                         'rank_turn_11', 'rank_turn_12',
                                         'rank_turn_13', 'rank_turn_14', 'rank_turn_15', 'rank_turn_16', 'rank_turn_17',
                                         'rank_turn_18',
                                         'rank_turn_19', 'rank_turn_2', 'rank_turn_20', 'rank_turn_3', 'rank_turn_4',
                                         'rank_turn_5',
                                         'rank_turn_6', 'rank_turn_7', 'rank_turn_8', 'rank_turn_9'],
            'features_CN_STOCK_A_G311': ['date', 'instrument', 'rank_amount_100', 'rank_amount_101', 'rank_amount_102',
                                         'rank_amount_103',
                                         'rank_amount_104', 'rank_amount_105', 'rank_amount_106', 'rank_amount_107',
                                         'rank_amount_108',
                                         'rank_amount_109', 'rank_amount_110', 'rank_amount_111', 'rank_amount_112',
                                         'rank_amount_113',
                                         'rank_amount_114', 'rank_amount_115', 'rank_amount_116', 'rank_amount_117',
                                         'rank_amount_118',
                                         'rank_amount_119', 'rank_amount_120', 'rank_amount_21', 'rank_amount_22',
                                         'rank_amount_23',
                                         'rank_amount_24', 'rank_amount_25', 'rank_amount_26', 'rank_amount_27',
                                         'rank_amount_28',
                                         'rank_amount_29', 'rank_amount_30', 'rank_amount_31', 'rank_amount_32',
                                         'rank_amount_33',
                                         'rank_amount_34', 'rank_amount_35', 'rank_amount_36', 'rank_amount_37',
                                         'rank_amount_38',
                                         'rank_amount_39', 'rank_amount_40', 'rank_amount_41', 'rank_amount_42',
                                         'rank_amount_43',
                                         'rank_amount_44', 'rank_amount_45', 'rank_amount_46', 'rank_amount_47',
                                         'rank_amount_48',
                                         'rank_amount_49', 'rank_amount_50', 'rank_amount_51', 'rank_amount_52',
                                         'rank_amount_53',
                                         'rank_amount_54', 'rank_amount_55', 'rank_amount_56', 'rank_amount_57',
                                         'rank_amount_58',
                                         'rank_amount_59', 'rank_amount_60', 'rank_amount_61', 'rank_amount_62',
                                         'rank_amount_63',
                                         'rank_amount_64', 'rank_amount_65', 'rank_amount_66', 'rank_amount_67',
                                         'rank_amount_68',
                                         'rank_amount_69', 'rank_amount_70', 'rank_amount_71', 'rank_amount_72',
                                         'rank_amount_73',
                                         'rank_amount_74', 'rank_amount_75', 'rank_amount_76', 'rank_amount_77',
                                         'rank_amount_78',
                                         'rank_amount_79', 'rank_amount_80', 'rank_amount_81', 'rank_amount_82',
                                         'rank_amount_83',
                                         'rank_amount_84', 'rank_amount_85', 'rank_amount_86', 'rank_amount_87',
                                         'rank_amount_88',
                                         'rank_amount_89', 'rank_amount_90', 'rank_amount_91', 'rank_amount_92',
                                         'rank_amount_93',
                                         'rank_amount_94', 'rank_amount_95', 'rank_amount_96', 'rank_amount_97',
                                         'rank_amount_98',
                                         'rank_amount_99', 'rank_avg_amount_120', 'rank_avg_amount_150',
                                         'rank_avg_amount_180',
                                         'rank_avg_amount_210', 'rank_avg_amount_240', 'rank_avg_amount_270',
                                         'rank_avg_amount_30',
                                         'rank_avg_amount_300', 'rank_avg_amount_330', 'rank_avg_amount_360',
                                         'rank_avg_amount_40',
                                         'rank_avg_amount_50', 'rank_avg_amount_60', 'rank_avg_amount_70',
                                         'rank_avg_amount_80',
                                         'rank_avg_amount_90', 'rank_avg_turn_120', 'rank_avg_turn_150',
                                         'rank_avg_turn_180',
                                         'rank_avg_turn_210', 'rank_avg_turn_240', 'rank_avg_turn_270',
                                         'rank_avg_turn_30', 'rank_avg_turn_300',
                                         'rank_avg_turn_330', 'rank_avg_turn_360', 'rank_avg_turn_40',
                                         'rank_avg_turn_50', 'rank_avg_turn_60',
                                         'rank_avg_turn_70', 'rank_avg_turn_80', 'rank_avg_turn_90',
                                         'rank_beta_csi100_10_0',
                                         'rank_beta_csi100_120_0', 'rank_beta_csi100_180_0', 'rank_beta_csi100_30_0',
                                         'rank_beta_csi100_5_0',
                                         'rank_beta_csi100_60_0', 'rank_beta_csi100_90_0', 'rank_beta_csi300_10_0',
                                         'rank_beta_csi300_120_0',
                                         'rank_beta_csi300_180_0', 'rank_beta_csi300_30_0', 'rank_beta_csi300_5_0',
                                         'rank_beta_csi300_60_0',
                                         'rank_beta_csi300_90_0', 'rank_beta_csi500_10_0', 'rank_beta_csi500_120_0',
                                         'rank_beta_csi500_180_0',
                                         'rank_beta_csi500_30_0', 'rank_beta_csi500_5_0', 'rank_beta_csi500_60_0',
                                         'rank_beta_csi500_90_0',
                                         'rank_beta_csi800_10_0', 'rank_beta_csi800_120_0', 'rank_beta_csi800_180_0',
                                         'rank_beta_csi800_30_0',
                                         'rank_beta_csi800_5_0', 'rank_beta_csi800_60_0', 'rank_beta_csi800_90_0',
                                         'rank_beta_gem_10_0',
                                         'rank_beta_gem_120_0', 'rank_beta_gem_180_0', 'rank_beta_gem_30_0',
                                         'rank_beta_gem_5_0',
                                         'rank_beta_gem_60_0', 'rank_beta_gem_90_0', 'rank_beta_industry_10_0',
                                         'rank_beta_industry_120_0',
                                         'rank_beta_industry_180_0', 'rank_beta_industry_30_0',
                                         'rank_beta_industry_5_0',
                                         'rank_beta_industry_60_0', 'rank_beta_industry_90_0',
                                         'rank_beta_industry1_10_0', 'rank_beta_industry1_120_0',
                                         'rank_beta_industry1_180_0', 'rank_beta_industry1_30_0',
                                         'rank_beta_industry1_60_0', 'rank_beta_industry1_90_0',
                                         'rank_beta_industry1_5_0', 'rank_beta_sse180_10_0',
                                         'rank_beta_sse180_120_0', 'rank_beta_sse180_180_0', 'rank_beta_sse180_30_0',
                                         'rank_beta_sse180_5_0',
                                         'rank_beta_sse180_60_0', 'rank_beta_sse180_90_0', 'rank_beta_sse50_10_0',
                                         'rank_beta_sse50_120_0',
                                         'rank_beta_sse50_180_0', 'rank_beta_sse50_30_0', 'rank_beta_sse50_5_0',
                                         'rank_beta_sse50_60_0',
                                         'rank_beta_sse50_90_0', 'rank_beta_szzs_10_0', 'rank_beta_szzs_120_0',
                                         'rank_beta_szzs_180_0',
                                         'rank_beta_szzs_30_0', 'rank_beta_szzs_5_0', 'rank_beta_szzs_60_0',
                                         'rank_beta_szzs_90_0',
                                         'rank_return_120', 'rank_return_150', 'rank_return_180', 'rank_return_210',
                                         'rank_return_240',
                                         'rank_return_270', 'rank_return_30', 'rank_return_300', 'rank_return_330',
                                         'rank_return_360',
                                         'rank_return_40', 'rank_return_50', 'rank_return_60', 'rank_return_70',
                                         'rank_return_80',
                                         'rank_return_90', 'rank_swing_volatility_10_0', 'rank_swing_volatility_120_0',
                                         'rank_swing_volatility_240_0', 'rank_swing_volatility_30_0',
                                         'rank_swing_volatility_5_0',
                                         'rank_swing_volatility_60_0', 'rank_volatility_10_0', 'rank_volatility_120_0',
                                         'rank_volatility_240_0',
                                         'rank_volatility_30_0', 'rank_volatility_5_0', 'rank_volatility_60_0']}

    def start(self):
        import datetime
        from bigshared.common.utils import is_trading_day
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        if not is_trading_day():
            print("{} 不是交易日, 跳过数据构建".format(today_str))
            return
        if len(self.markets) > 1:
            self.log.error('行情数据不能同时构建多个市场!')
            raise RuntimeError('行情数据不能同时构建多个市场!')

        # 股票基本数据，在某段时间段上市的股票数据
        market = self.markets[0]

        self.log.info("开始计算rank因子 {} {} {}".format(market, self.start_date, self.end_date))
        instruments = D.instruments(start_date=self.start_date, end_date=self.end_date, market=market)

        field_groups = {}
        for f in self.__rank_feature_list:
            field_groups.setdefault(f.category, [])
            field_groups[f.category].append(f)

        df = pd.DataFrame()
        for group_id, feature_list in field_groups.items():
            self.log.info('compute group id %s..' % group_id)
            base_fields = set()
            for f in feature_list:
                for feature_name, other in f.expand_fields(with_params=True):
                    base_fields.add(feature_name.replace('rank_', ''))
            # 组合数据来源
            all_df = pd.DataFrame()
            for table in self.original_data_tables:
                df_tem = DataSource(table).read(
                    instruments, start_date=self.start_date, end_date=self.end_date)
                if all_df.empty:
                    all_df = df_tem.copy()
                else:
                    all_df = pd.merge(all_df, df_tem, on=['instrument', 'date'], how='outer')
            grouped = all_df.groupby('date')
            ret_df = pd.DataFrame(all_df[['date', 'instrument']])
            for f in feature_list:
                for feature_name, other in f.expand_fields(with_params=True):
                    if f.base_field not in list(all_df.columns) \
                            and feature_name.replace('rank_', '') not in list(all_df.columns):
                        self.log.warning("{} not in df columns, {}".format(feature_name, f.base_field))
                        continue
                    self.log.info('计算因子 {}'.format(feature_name))
                    ret_df[feature_name] = grouped[feature_name.replace('rank_', '')].rank(pct=True)
            ret_df = truncate(ret_df, 'date', self.start_date, self.end_date)
            if df.empty:
                df = ret_df.copy()
            else:
                df = pd.merge(df, ret_df, on=['instrument', 'date'], how='outer')
        self.log.info('计算因子结束...')

        for table_name in self.features_fields_map.keys():
            fields = [i.strip().replace('\n', '') for i in self.features_fields_map[table_name]]
            UpdateDataSource().update(alias=table_name, df=df[fields])


if __name__ == '__main__':
    Build().start()
