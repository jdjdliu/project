import os
import subprocess
import datetime
import pandas as pd
from sdk.datasource import DataSource

START_DATE = (datetime.date.today() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
END_DATE = datetime.date.today().strftime('%Y-%m-%d')
IS_WRITE = False
BASE_SCRIPT_PATH = "/var/app/data/bigquant/datasource/data_build/aiweFund/"
LOG_PATH = "/var/app/data/bigquant/datasource/data_build/update_logs/"


def start_run():
    trading_df = DataSource('all_trading_days').read(start_date=END_DATE, end_date=END_DATE)
    if isinstance(trading_df, pd.DataFrame):
        trading_df = trading_df[trading_df.country_code == 'CN']
        if not trading_df.empty:
            return True


def write_finish_log(record_file, date=END_DATE):
    with open(record_file, mode='a') as f:
        f.write(f"{date}\n")


def read_max_record_date(record_file):
    s_date = START_DATE
    if not os.path.exists(record_file):
        return s_date
    with open(record_file, mode='r') as f:
        datas = f.readlines()
    last_date = datas[-1].replace("\n", '')
    if last_date < s_date:
        return last_date
    return s_date


def run_command_str(source_cmd_str):
    # GLOBAL_TABLE/fetch_source.py --> GLOBAL_TABLE-fetch_source
    run_name = source_cmd_str.split(" ")[0][:-3].replace("/", "-")
    record_path = LOG_PATH + 'scripts_logs/'
    if not os.path.exists(record_path):
        os.mkdir(record_path)
    record_file = record_path + run_name + '.txt'
    s_date = read_max_record_date(record_file)
    cmd_str = 'python3 ' + BASE_SCRIPT_PATH + source_cmd_str + f" --start_date='{s_date}' --end_date='{END_DATE}'"

    print(f"::start_run command: {cmd_str}")
    res = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out = res.stdout.read().decode('utf-8')
    print(std_out)
    err_out = res.stderr.read().decode('utf-8').strip()
    if err_out:
        print(":: have error info -------------->")
        print(err_out)
        print(f":: finish_run(on error) command: {cmd_str}")
        return False, run_name
    else:
        write_finish_log(record_file, date=END_DATE)
        print(f"::finish_run command: {cmd_str}")
        return True, run_name


def run_scripts():

    base_build = [
        'GLOBAL_TABLE/fetch_source.py --is_history=False --alias="REF_CALENDAR,REF_REGION,PTY_BASICINFO,PTY_INDUSTRY,PTY_FIN_INCOME,PTY_FIN_DRVIND,PTY_FIN_CASHFLOW,PTY_FIN_BALANCE,PTY_FIN_INDICATOR"',
        # 'GLOBAL_TABLE/fetch_source.py --is_history=False --alias="REF_CALENDAR,REF_REGION,PTY_BASICINFO,PTY_INDUSTRY"',
        'GLOBAL_TABLE/all_trading_days.py'
    ]
    success_lst = []
    # if start_run():
    if True:  # run every day change in 2022-07-21 16:35
        print(f">>>>> start running")
        # 1.先跑基础表，这些失败了后面的不用跑了
        for source_cmd_str in base_build:
            res, run_name = run_command_str(source_cmd_str)
            if not res:
                break
            success_lst.append(run_name)
        if "GLOBAL_TABLE-all_trading_days" not in success_lst:
            print(">>>>>>>>>>>.!! end run all shell")
            return

        # 2.跑基金相关的数据
        fund_build = [
             'CN_FUND/fetch_fund_data.py --is_history=False ', #  --alias="" TABLES_MAPS的全部表
             'CN_FUND/build_bigquant_table/basic_info_CN_FUND.py',
             'CN_FUND/build_bigquant_table/bar1d_CN_FUND.py',
             'CN_FUND/build_bigquant_table/adjust_factor_CN_FUND.py',
             'CN_FUND/build_bigquant_table/dividend_send_CN_FUND.py',
             'CN_FUND/build_bigquant_table/instruments_CN_FUND.py',
             'CN_FUND/build_bigquant_table/basic_info_CN_MUTFUND.py',
             'CN_FUND/build_bigquant_table/history_divm_CN_MUTFUND.py',
             'CN_FUND/build_bigquant_table/history_nav_CN_MUTFUND.py',
             'CN_FUND/build_bigquant_table/instruments_CN_MUTFUND.py',
        ]
        for source_cmd_str in fund_build:
            res, run_name = run_command_str(source_cmd_str)
            if not res:
                break
            success_lst.append(run_name)

        # 3.跑股票相关的数据
        stock_build = [
            'CN_STOCK_A/fetch_stock_data.py --is_history=False --alias="STK_BASICINFO,STK_SPCTREAT,STK_INFOCHG,STK_FREECRCSHR,STK_SCMEMBERS,STK_IPO,STK_EXRIGHT,STK_DIVIDEND,STK_SUSPENSION,STK_BLOCKTRADE"',
            'CN_STOCK_A/build_bigquant_table/basic_info_CN_STOCK_A.py',
            'CN_STOCK_A/fetch_stock_data_2.py --is_history=True --alias="STK_EODPRICE,STK_VALUATION,STK_TA_VOLUME"',
            'CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A_fill.py',
            'CN_STOCK_A/build_bigquant_table/instruments_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/stock_status_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/industry_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/market_value_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/dividend_send_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/financial_statement_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/financial_statement_ff_CN_STOCK_A.py',
        ]

        for source_cmd_str in stock_build:
            res, run_name = run_command_str(source_cmd_str)
            if not res:
                break
            success_lst.append(run_name)

        # 4.跑指数相关的数据
        index_build = [
            'CN_INDEX/fetch_source_index.py --alias="IDX_BASICINFO"',
            'CN_STOCK_A/build_bigquant_table/basic_info_index_CN_STOCK_A.py',
            'CN_INDEX/fetch_source_index_2.py --alias="IDX_EODVALUE,IDX_COMPONENTS,IDX_WT_STK"',
            # 'CN_INDEX/fetch_source_index_2.py --alias="IDX_EODVALUE"',
            'CN_STOCK_A/build_bigquant_table/bar1d_index_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/index_constituent_CN_STOCK_A.py',
            'CN_STOCK_A/build_bigquant_table/index_element_weight.py',
        ]
        for source_cmd_str in index_build:
            res, run_name = run_command_str(source_cmd_str)
            if not res:
                break
            success_lst.append(run_name)
        if ('CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill' in success_lst) and\
                ('CN_STOCK_A-build_bigquant_table-bar1d_index_CN_STOCK_A' in success_lst):
            res, run_name = run_command_str('CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A_all.py')
            if res:
                success_lst.append(run_name)

        features_table_lst = [
            # 量价因子 bar1d,stock_status
            'CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill',
            'CN_STOCK_A-build_bigquant_table-stock_status_CN_STOCK_A',

            # 换手率因子 bar1d
            'CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill',

            # 基本信息 bar1d_index,industry,basic_info,stock_status,index_constituent
            'CN_STOCK_A-build_bigquant_table-bar1d_index_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-industry_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-basic_info_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-stock_status_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-index_constituent_CN_STOCK_A',

            # 估值因子 market_value
            'CN_STOCK_A-build_bigquant_table-market_value_CN_STOCK_A',

            # 技术分析因子 bar1d
            'CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill',

            # BETA值bar1d_index,industry,bar1d
            'CN_STOCK_A-build_bigquant_table-bar1d_index_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-industry_CN_STOCK_A',
            'CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill',

            # 波动率 bar1d
            'CN_STOCK_A-build_bigquant_table-bar1d_CN_STOCK_A_fill',

            # 财务因子financial_statement
            'CN_STOCK_A-build_bigquant_table-financial_statement_ff_CN_STOCK_A',
        ]

        if set(features_table_lst) <= set(success_lst):
            source_cmd_str = 'CN_STOCK_A/build_bigquant_table/features_build/features_CN_STOCK_A.py'
            res, run_name = run_command_str(source_cmd_str)

        future_build = [
                'CN_FUTURE/fetch_future_data.py',
                'CN_FUTURE/build_bigquant_table/basic_info_CN_FUTURE.py',
                'CN_FUTURE/build_bigquant_table/dominant_CN_FUTURE.py',
                'CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE.py',
                'CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE_ffill.py',
                'CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE_0000.py',
                'CN_FUTURE/build_bigquant_table/instrument_CN_FUTURE.py',

        ]
        for future_cmd_str in future_build:
                res, run_name = run_command_str(future_cmd_str)
                if not res:
                        break
                success_lst.append(run_name)

    else:
        print("Not trading day, don't run")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    run_scripts()

