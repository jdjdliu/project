import json
# from features_CN_STOCK_A import schema
fields_lst = [
                'instrument', 'date',
                # 量价因子
                'adjust_factor_0', 'amount_0', 'avg_amount_0', 'close_0', 'daily_return_0', 'deal_number_0',
                'high_0', 'low_0', 'open_0', 'price_limit_status_0', 'rank_amount_0', 'rank_avg_amount_0',
                'rank_return_0', 'return_0', 'volume_0',

                # 换手率因子
                'avg_turn_0', 'rank_avg_turn_0', 'turn_0', 'rank_turn_0',

                # 基本信息因子
                'company_found_date_0', 'in_csi100_0', 'in_csi300_0', 'in_csi500_0', 'in_csi800_0',
                'in_sse180_0', 'in_sse50_0', 'in_szse100_0', 'industry_sw_level1_0', 'industry_sw_level2_0',
                'industry_sw_level3_0', 'list_board_0', 'list_days_0', 'st_status_0',

                # # 资金流因子：缺表
                'avg_mf_net_amount_0', 'mf_net_amount_0', 'mf_net_amount_l_0', 'mf_net_amount_m_0',
                'mf_net_amount_main_0', 'mf_net_amount_s_0', 'mf_net_amount_xl_0', 'mf_net_pct_l_0',
                'mf_net_pct_m_0', 'mf_net_pct_main_0', 'mf_net_pct_s_0', 'mf_net_pct_xl_0',
                'rank_avg_mf_net_amount_0', 'rank_avg_mf_net_amount_0',

                # # 股东因子：缺表
                'rank_sh_holder_avg_pct_0', 'rank_sh_holder_avg_pct_3m_chng_0', 'rank_sh_holder_avg_pct_6m_chng_0',
                'rank_sh_holder_num_0', 'sh_holder_avg_pct_0', 'sh_holder_avg_pct_3m_chng_0',
                'sh_holder_avg_pct_6m_chng_0', 'sh_holder_num_0',

                # 估值因子
                'market_cap_0', 'market_cap_float_0', 'pb_lf_0', 'pe_lyr_0', 'pe_ttm_0', 'ps_ttm_0', 'rank_market_cap_0',
                'rank_market_cap_float_0', 'rank_pb_lf_0', 'rank_pe_lyr_0', 'rank_pe_ttm_0', 'rank_ps_ttm_0',
                # 针对估值因子下面三列：需要west_CN_STOCK_A，先注释掉不计算，features.py里面也注释，后续有数据了要放开
                # !!!!!!
                'west_avgcps_ftm_0', 'west_eps_ftm_0', 'west_netprofit_ftm_0',

                # 技术分析因子
                'ta_ad_0', 'ta_adx_14_0', 'ta_adx_28_0', 'ta_aroon_down_14_0', 'ta_aroon_down_28_0', 'ta_aroon_up_14_0',
                'ta_aroon_up_28_0', 'ta_aroonosc_14_0', 'ta_aroonosc_28_0', 'ta_atr_14_0', 'ta_atr_28_0',
                'ta_bbands_lowerband_14_0', 'ta_bbands_lowerband_28_0', 'ta_bbands_middleband_14_0',
                'ta_bbands_middleband_28_0', 'ta_bbands_upperband_14_0', 'ta_bbands_upperband_28_0', 'ta_cci_14_0',
                'ta_cci_28_0', 'ta_ema_5_0', 'ta_ema_10_0', 'ta_ema_20_0', 'ta_ema_30_0', 'ta_ema_60_0',
                'ta_macd_macd_12_26_9_0', 'ta_macd_macdhist_12_26_9_0', 'ta_macd_macdsignal_12_26_9_0',
                'ta_mfi_14_0', 'ta_mfi_28_0', 'ta_mom_10_0', 'ta_mom_20_0', 'ta_mom_30_0', 'ta_mom_60_0',
                'ta_obv_0', 'ta_rsi_14_0', 'ta_rsi_28_0', 'ta_sar_0', 'ta_sma_5_0', 'ta_sma_10_0', 'ta_sma_20_0',
                'ta_sma_30_0', 'ta_sma_60_0', 'ta_stoch_slowd_5_3_0_3_0_0', 'ta_stoch_slowk_5_3_0_3_0_0',
                'ta_trix_14_0', 'ta_trix_28_0', 'ta_willr_14_0', 'ta_willr_28_0', 'ta_wma_5_0', 'ta_wma_10_0',
                'ta_wma_20_0', 'ta_wma_30_0', 'ta_wma_60_0',

                # beta值因子
                'beta_csi100_10_0', 'beta_csi100_120_0','beta_csi100_180_0', 'beta_csi100_30_0',
                'beta_csi100_5_0', 'beta_csi100_60_0', 'beta_csi100_90_0',

                'beta_csi300_10_0', 'beta_csi300_120_0', 'beta_csi300_180_0', 'beta_csi300_30_0',
                'beta_csi300_5_0', 'beta_csi300_60_0', 'beta_csi300_90_0',

                'beta_csi500_10_0', 'beta_csi500_120_0', 'beta_csi500_180_0', 'beta_csi500_30_0',
                'beta_csi500_5_0', 'beta_csi500_60_0', 'beta_csi500_90_0',

                'beta_csi800_10_0', 'beta_csi800_120_0', 'beta_csi800_180_0', 'beta_csi800_30_0',
                'beta_csi800_5_0', 'beta_csi800_60_0', 'beta_csi800_90_0',

                'beta_gem_10_0', 'beta_gem_120_0', 'beta_gem_180_0', 'beta_gem_30_0',
                'beta_gem_5_0', 'beta_gem_60_0', 'beta_gem_90_0',

                'beta_industry_10_0', 'beta_industry_120_0', 'beta_industry_180_0', 'beta_industry_30_0',
                'beta_industry_5_0', 'beta_industry_60_0', 'beta_industry_90_0',

                #     'beta_industry1_10_0', 'beta_industry1_120_0', 'beta_industry1_180_0',
                #     'beta_industry1_30_0', 'beta_industry1_5_0', 'beta_industry1_60_0',
                #     'beta_industry1_90_0',

                'beta_sse180_10_0', 'beta_sse180_120_0', 'beta_sse180_180_0','beta_sse180_30_0',
                'beta_sse180_5_0', 'beta_sse180_60_0', 'beta_sse180_90_0',

                'beta_sse50_10_0', 'beta_sse50_120_0', 'beta_sse50_180_0', 'beta_sse50_30_0',
                'beta_sse50_5_0', 'beta_sse50_60_0', 'beta_sse50_90_0',

                'beta_szzs_120_0', 'beta_szzs_180_0', 'beta_szzs_30_0', 'beta_szzs_5_0',
                'beta_szzs_60_0', 'beta_szzs_90_0', 'beta_szzs_10_0',

                'rank_beta_csi100_10_0',
                'rank_beta_csi100_120_0', 'rank_beta_csi100_180_0', 'rank_beta_csi100_30_0',
                'rank_beta_csi100_5_0', 'rank_beta_csi100_60_0', 'rank_beta_csi100_90_0',

                'rank_beta_csi300_10_0', 'rank_beta_csi300_120_0',
                'rank_beta_csi300_180_0', 'rank_beta_csi300_30_0', 'rank_beta_csi300_5_0',
                'rank_beta_csi300_60_0', 'rank_beta_csi300_90_0',

                'rank_beta_csi500_10_0', 'rank_beta_csi500_120_0',
                'rank_beta_csi500_180_0',
                'rank_beta_csi500_30_0', 'rank_beta_csi500_5_0', 'rank_beta_csi500_60_0',
                'rank_beta_csi500_90_0',

                'rank_beta_csi800_10_0', 'rank_beta_csi800_120_0', 'rank_beta_csi800_180_0',
                'rank_beta_csi800_30_0',
                'rank_beta_csi800_5_0', 'rank_beta_csi800_60_0', 'rank_beta_csi800_90_0',
                'rank_beta_gem_10_0',
                'rank_beta_gem_120_0', 'rank_beta_gem_180_0', 'rank_beta_gem_30_0',
                'rank_beta_gem_5_0',
                'rank_beta_gem_60_0', 'rank_beta_gem_90_0',
                'rank_beta_industry_10_0',
                'rank_beta_industry_120_0',
                'rank_beta_industry_180_0', 'rank_beta_industry_30_0',
                'rank_beta_industry_5_0',
                'rank_beta_industry_60_0', 'rank_beta_industry_90_0',

                # 'rank_beta_industry1_10_0', 'rank_beta_industry1_120_0',
                # 'rank_beta_industry1_180_0', 'rank_beta_industry1_30_0',
                # 'rank_beta_industry1_60_0', 'rank_beta_industry1_90_0',
                # 'rank_beta_industry1_5_0',
                'rank_beta_sse180_10_0',
                'rank_beta_sse180_120_0', 'rank_beta_sse180_180_0', 'rank_beta_sse180_30_0',
                'rank_beta_sse180_5_0',
                'rank_beta_sse180_60_0', 'rank_beta_sse180_90_0', 'rank_beta_sse50_10_0',
                'rank_beta_sse50_120_0',
                'rank_beta_sse50_180_0', 'rank_beta_sse50_30_0', 'rank_beta_sse50_5_0',
                'rank_beta_sse50_60_0',
                'rank_beta_sse50_90_0',

                'rank_beta_szzs_10_0', 'rank_beta_szzs_120_0',
                'rank_beta_szzs_180_0',
                'rank_beta_szzs_30_0', 'rank_beta_szzs_5_0', 'rank_beta_szzs_60_0',
                'rank_beta_szzs_90_0',

                # 波动率因子
                'swing_volatility_10_0', 'swing_volatility_120_0', 'swing_volatility_240_0',
                'swing_volatility_30_0',
                'swing_volatility_5_0', 'swing_volatility_60_0',

                'volatility_10_0', 'volatility_120_0', 'volatility_240_0',
                'volatility_30_0',
                'volatility_5_0', 'volatility_60_0',

                'rank_swing_volatility_10_0', 'rank_swing_volatility_120_0',
                'rank_swing_volatility_240_0', 'rank_swing_volatility_30_0',
                'rank_swing_volatility_5_0',
                'rank_swing_volatility_60_0',

                'rank_volatility_10_0', 'rank_volatility_120_0', 'rank_volatility_240_0',
                'rank_volatility_30_0', 'rank_volatility_5_0', 'rank_volatility_60_0',

                # # 财务相关-盈利因子
                'fs_quarter_year_0', 'fs_quarter_index_0', 'fs_publish_date_0',
                # 'roa2', 'roa', 'op_gr', 'pro_tog', 'gc_gr', 'ebit_gr', 'gro_pro_mar', 'net_pro_mar', 'cog_sal',
                'roa2_0', 'roa_0', 'op_gr_0', 'pro_tog_0', 'gc_gr_0', 'ebit_gr_0', 'gro_pro_mar_0', 'net_pro_mar_0',
                 'cog_sal_0',

                # # 财务相关-成长因子
                # 'yoy_eps_bas', 'yoy_eps_dil', 'yoy_ocf', 'yoy_op', 'yoy_ebt_tot', 'yoy_net_pro', 'yoy_tr', 'yoy_or',
                # 'yoy_bps', 'yoy_ass', 'yoy_equ_sha',
                'yoy_eps_bas_0', 'yoy_eps_dil_0', 'yoy_ocf_0', 'yoy_op_0', 'yoy_ebt_tot_0', 'yoy_net_pro_0', 'yoy_tr_0',
                'yoy_or_0', 'yoy_bps_0', 'yoy_ass_0', 'yoy_equ_sha_0',

                # # 财务相关-质量因子
                # 'acc_lia_pro', 'ass_equ', 'lia_tot_equ', 'ebit_int', 'cur_rat', 'qui_rat', 'cas_rat', 'cat_ass',
                # 'ocf_lia_cur', 'equ_deb', 'ocf_deb',
                'acc_lia_pro_0', 'ass_equ_0', 'lia_tot_equ_0', 'ebit_int_0', 'cur_rat_0', 'qui_rat_0', 'cas_rat_0',
                'cat_ass_0', 'ocf_lia_cur_0', 'equ_deb_0', 'ocf_deb_0',

                # # 财务相关-营运因子
                # 'inv_rn', 'art_rn', 'art_day', 'fat_rn', 'cat_rn', 'ass_rn', 'fcff', 'wor_cap', 'net_wor_cap',
                'inv_rn_0', 'art_rn_0', 'art_day_0', 'fat_rn_0', 'cat_rn_0', 'ass_rn_0', 'fcff_0', 'wor_cap_0',
                'net_wor_cap_0',

                # # 财务相关-每股因子
                # 'eps_bas', 'eps_dlt', 'bps', 'ocfps', 'gr_ps', 'orps', 'sur_cap', 'sur_res', 'und_ps', 'ret_ps',
                # 'cfps', 'ebit_ps', 'fcff_ps', 'fcfe_ps',
                'eps_bas_0', 'eps_dlt_0', 'bps_0', 'ocfps_0', 'gr_ps_0', 'orps_0', 'sur_cap_0', 'sur_res_0', 'und_ps_0',
                'ret_ps_0', 'cfps_0', 'ebit_ps_0', 'fcff_ps_0', 'fcfe_ps_0',
            ]
features_map = {}
for x in fields_lst:
    features_map[x] = 'features_CN_STOCK_A'
res_json = json.dumps(features_map)
print(len(features_map))
with open('features_map.json', mode='w') as f:
    f.write(res_json)