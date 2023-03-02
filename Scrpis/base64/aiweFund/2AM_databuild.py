import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

DAG_ID = "2AM_databuild"
start_date = None
end_date = None
is_history = False

# ====for debug or rebuild history data===============
# start_date = '2021-01-01'
# end_date = '2021-06-30'
# is_history = True
# ====================================================

default_image = os.getenv("AIFLOW_IMAGE_LATEST")
#image = "csapdev.registry.cmbchina.cn/lx58/aiwequantplatformaiflow2:pl226595-30"
pod_template_file = "/opt/airflow/dags/pod_templates/data.yaml"

namespace = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE", "ap-lx58-aiwe-quantize")

# 并发数
concurrency = 5

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval='1 2 * * *',
    default_args={
        "owner": "aiflow", 
        "retries": 2
    },
    start_date=days_ago(30),  # type: ignore
    catchup=False,
    tags=["data"],
    concurrency=concurrency,
)

base_path = '/opt/airflow/dags/scripts/datascripts/aiweFund/'


def gen_task(script):
    bash_command = "python3 {}".format(script)
    if start_date:
        bash_command = "{} --start_date {}".format(bash_command, start_date)
    if end_date:
        bash_command = "{} --end_date {}".format(bash_command, end_date)
    if is_history and ("--alias" in script):  # 只有fetch_xxx.py才会有is_history参数
        bash_command = "{} --is_history {}".format(bash_command, is_history)
    return bash_command

# build_command = "python3 GLOBAL_TABLE/all_trading_days.py"


def get_task_pod(task_name, build_command):
    return KubernetesPodOperator(
            task_id=task_name,
            namespace=namespace,
            image=default_image,
            cmds=["bash", "-cx"],
            arguments=[build_command],
            pod_template_file=pod_template_file,
            in_cluster=True,
            is_delete_operator_pod=True,
            get_logs=True,
            execution_timeout=timedelta(seconds=60 * 60),
            dag=dag,
            name=task_name.replace('_', '-'),
    )


bash_operator_tasks = {

    # 1.全局通用的数据
    # 'fetch_source_global': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --is_history=False --alias="REF_CALENDAR,'
    #                                 f'REF_REGION,PTY_BASICINFO,PTY_INDUSTRY,PTY_FIN_INCOME,PTY_FIN_DRVIND,'
                                    # f'PTY_FIN_CASHFLOW,PTY_FIN_BALANCE,PTY_FIN_INDICATOR"'),
    'global_REF_CALENDAR': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="REF_CALENDAR"'),
    'global_REF_REGION': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="REF_REGION"'),
    'global_PTY_BASICINFO': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_BASICINFO"'),
    'global_PTY_INDUSTRY': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_INDUSTRY"'),
    'global_PTY_FIN_INCOME': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_FIN_INCOME"'),
    'global_PTY_FIN_DRVIND': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_FIN_DRVIND"'),
    'global_PTY_FIN_CASHFLOW': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_FIN_CASHFLOW"'),
    'global_PTY_FIN_BALANCE': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_FIN_BALANCE"'),
    'global_PTY_FIN_INDICATOR': gen_task(f'{base_path}GLOBAL_TABLE/fetch_source.py --alias="PTY_FIN_INDICATOR"'),

    'all_trading_days': gen_task(f'{base_path}GLOBAL_TABLE/all_trading_days.py'),

    # 2.基金相关的数据
    # 'fetch_source_fund': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py'),

    'fund_FUND_BASICINFO': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_BASICINFO"'),
    'fund_FUND_CLASSIFICATION': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_CLASSIFICATION"'),
    'fund_FUND_OPERPERIOD': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_OPERPERIOD"'),
    'fund_FUND_PFMBCHM': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_PFMBCHM"'),
    'fund_FUND_FEE': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_FEE"'),
    'fund_FUND_NETVALUE': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_NETVALUE"'),
    'fund_FUND_NETVALUE_MMF': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_NETVALUE_MMF"'),
    'fund_FUND_EODPRICE': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_EODPRICE"'),
    'fund_FUND_SHARES': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_SHARES"'),
    'fund_FUND_FIN_INCOME': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_FIN_INCOME"'),
    'fund_FUND_FIN_BALANCE': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_FIN_BALANCE"'),
    'fund_FUND_FIN_INDICATOR': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_FIN_INDICATOR"'),
    'fund_FUND_SHRCNVSPLIT': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_SHRCNVSPLIT"'),
    'fund_FUND_DIVIDEND': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_DIVIDEND"'),
    'fund_FUND_MANAGER': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_MANAGER"'),
    'fund_FUND_INVPOSLMT': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_INVPOSLMT"'),
    'fund_FUND_HOLDERSTRUC': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_HOLDERSTRUC"'),
    'fund_FUND_PFL_SECLIST_QDII': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_PFL_SECLIST_QDII"'),
    'fund_FUND_PFL_SECLIST': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_PFL_SECLIST"'),
    'fund_FUND_PFL_INDUSTRY': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_PFL_INDUSTRY"'),
    'fund_FUND_PFL_ASSET': gen_task(f'{base_path}CN_FUND/fetch_fund_data.py --alias="FUND_PFL_ASSET"'),


    'basic_info_CN_FUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/basic_info_CN_FUND.py'),
    'bar1d_CN_FUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/bar1d_CN_FUND.py'),
    'adjust_factor_CN_FUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/adjust_factor_CN_FUND.py'),
    'dividend_send_CN_FUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/dividend_send_CN_FUND.py'),
    'instruments_CN_FUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/instruments_CN_FUND.py'),
    'basic_info_CN_MUTFUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/basic_info_CN_MUTFUND.py'),
    'history_divm_CN_MUTFUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/history_divm_CN_MUTFUND.py'),
    'history_nav_CN_MUTFUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/history_nav_CN_MUTFUND.py'),
    'instruments_CN_MUTFUND': gen_task(f'{base_path}CN_FUND/build_bigquant_table/instruments_CN_MUTFUND.py'),

    # 3.股票相关的数据
    # 'fetch_source_stock': gen_task(f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=False --alias="STK_BASICINFO,'
    #                                f'STK_SPCTREAT,STK_INFOCHG,STK_FREECRCSHR,STK_SCMEMBERS,STK_IPO,STK_EXRIGHT,'
    #                                f'STK_DIVIDEND,STK_SUSPENSION,STK_BLOCKTRADE"'),
    'basic_info_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/basic_info_CN_STOCK_A.py'),

    # TODO --is_history params to False?
    'stock_STK_BASICINFO': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_BASICINFO"'),
    'stock_STK_SPCTREAT': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_SPCTREAT"'),
    'stock_STK_INFOCHG': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_INFOCHG"'),
    'stock_STK_FREECRCSHR': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_FREECRCSHR"'),
    'stock_STK_SCMEMBERS': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_SCMEMBERS"'),
    'stock_STK_IPO': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_IPO"'),
    'stock_STK_EXRIGHT': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_EXRIGHT"'),
    'stock_STK_DIVIDEND': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_DIVIDEND"'),
    'stock_STK_SUSPENSION': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_SUSPENSION"'),
    'stock_STK_BLOCKTRADE': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_BLOCKTRADE"'),
    'stock_STK_EODPRICE': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_EODPRICE"'),
    'stock_STK_VALUATION': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_VALUATION"'),
    'stock_STK_TA_VOLUME': gen_task(
        f'{base_path}CN_STOCK_A/fetch_stock_data.py --is_history=True --alias="STK_TA_VOLUME"'),

    'bar1d_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A.py'),
    'bar1d_CN_STOCK_A_fill': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A_fill.py'),
    'instruments_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/instruments_CN_STOCK_A.py'),
    'stock_status_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/stock_status_CN_STOCK_A.py'),
    'industry_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/industry_CN_STOCK_A.py'),
    'market_value_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/market_value_CN_STOCK_A.py'),
    'dividend_send_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/dividend_send_CN_STOCK_A.py'),
    'financial_statement_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/financial_statement_CN_STOCK_A.py'),
    'financial_statement_ff_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/financial_statement_ff_CN_STOCK_A.py'),

    # 指数相关的数据
    # 'fetch_source_index': gen_task(f'{base_path}CN_INDEX/fetch_source_index.py --alias="IDX_BASICINFO"'),
    'index_IDX_BASICINFO': gen_task(f'{base_path}CN_INDEX/fetch_source_index.py --alias="IDX_BASICINFO"'),
    'index_IDX_EODVALUE': gen_task(f'{base_path}CN_INDEX/fetch_source_index.py --alias="IDX_EODVALUE"'),
    'index_IDX_COMPONENTS': gen_task(f'{base_path}CN_INDEX/fetch_source_index.py --alias="IDX_COMPONENTS"'),
    'index_IDX_WT_STK': gen_task(f'{base_path}CN_INDEX/fetch_source_index.py --alias="IDX_WT_STK"'),
    'basic_info_index_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/basic_info_index_CN_STOCK_A.py'),
    # 'fetch_source_index_eod': gen_task(f'{base_path}CN_INDEX/fetch_source_index_2.py --alias="IDX_EODVALUE,IDX_COMPONENTS,IDX_WT_STK"'),
    'bar1d_index_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/bar1d_index_CN_STOCK_A.py'),
    'index_constituent_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/index_constituent_CN_STOCK_A.py'),
    'index_element_weight': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/index_element_weight.py'),
    'bar1d_CN_STOCK_A_all': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/bar1d_CN_STOCK_A_all.py'),
    'features_CN_STOCK_A': gen_task(f'{base_path}CN_STOCK_A/build_bigquant_table/features_build/features_CN_STOCK_A.py'),

    # 期貨相关
    # 'fetch_source_future': gen_task(f'{base_path}CN_FUTURE/fetch_future_data.py'),
    'future_FTR_BASICINFO': gen_task(f'{base_path}CN_FUTURE/fetch_future_data.py --alias="FTR_BASICINFO"'),
    'future_FTR_EODPRICE': gen_task(f'{base_path}CN_FUTURE/fetch_future_data.py --alias="FTR_EODPRICE"'),
    'future_FTR_CNVFACTOR_TF': gen_task(f'{base_path}CN_FUTURE/fetch_future_data.py --alias="FTR_CNVFACTOR_TF"'),

    'basic_info_CN_FUTURE': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/basic_info_CN_FUTURE.py'),
    'dominant_CN_FUTURE': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/dominant_CN_FUTURE.py'),
    'bar1d_CN_FUTURE': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE.py'),
    'bar1d_CN_FUTURE_ffill': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE_ffill.py'),
    'bar1d_CN_FUTURE_0000': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/bar1d_CN_FUTURE_0000.py'),
    'instrument_CN_FUTURE': gen_task(f'{base_path}CN_FUTURE/build_bigquant_table/instrument_CN_FUTURE.py'),
}

task_instance = {}
for task_id, bash_command in bash_operator_tasks.items():
    # task_instance[task_id] = BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)
    task_instance[task_id] = get_task_pod(task_name=task_id, build_command=bash_command)


# =======================
# key 当前任务， value 下一个/多个任务
# 尽量将每个表创建一个task,便于后续的维护，虽然fetch_xxx.py系列可以指定一次跑多个表
# 这里有些任务没有前后依赖关系，不过避免同时运行多个占用资源以及dag的易读性，还是进行串行（资源占用可以通过调整并发数控制）
task_downstream_map = {
    # task1 -> task2  task1 -> task3
    # "fetch_source_global": ["all_trading_days"],
    "global_REF_CALENDAR": ["all_trading_days"],
    "global_REF_REGION": ["all_trading_days"],
    "global_PTY_BASICINFO": ["all_trading_days"],
    "global_PTY_INDUSTRY": ["all_trading_days"],
    "global_PTY_FIN_INCOME": ["all_trading_days"],
    "global_PTY_FIN_DRVIND": ["all_trading_days"],
    "global_PTY_FIN_CASHFLOW": ["all_trading_days"],
    "global_PTY_FIN_BALANCE": ["all_trading_days"],
    "global_PTY_FIN_INDICATOR": ["all_trading_days"],
    # "": ["all_trading_days"],

    "all_trading_days": [
        'fund_FUND_BASICINFO', 'fund_FUND_CLASSIFICATION', 'fund_FUND_OPERPERIOD', 'fund_FUND_PFMBCHM', 'fund_FUND_FEE',
        'fund_FUND_NETVALUE', 'fund_FUND_NETVALUE_MMF', 'fund_FUND_EODPRICE', 'fund_FUND_SHARES', 'fund_FUND_FIN_INCOME',
        'fund_FUND_FIN_BALANCE', 'fund_FUND_FIN_INDICATOR', 'fund_FUND_SHRCNVSPLIT', 'fund_FUND_DIVIDEND',
        'fund_FUND_MANAGER', 'fund_FUND_INVPOSLMT', 'fund_FUND_HOLDERSTRUC', 'fund_FUND_PFL_SECLIST_QDII',
        'fund_FUND_PFL_SECLIST', 'fund_FUND_PFL_INDUSTRY', 'fund_FUND_PFL_ASSET',

        'stock_STK_BASICINFO', 'stock_STK_SPCTREAT', 'stock_STK_INFOCHG', 'stock_STK_FREECRCSHR', 'stock_STK_SCMEMBERS',
        'stock_STK_IPO', 'stock_STK_EXRIGHT', 'stock_STK_DIVIDEND', 'stock_STK_SUSPENSION', 'stock_STK_BLOCKTRADE',

        "index_IDX_BASICINFO",

        'future_FTR_BASICINFO', 'future_FTR_EODPRICE', 'future_FTR_CNVFACTOR_TF',
    ],
    # fund
    # "fetch_source_fund": ["basic_info_CN_FUND"],
    "fund_FUND_BASICINFO": ["basic_info_CN_FUND"],
    "fund_FUND_CLASSIFICATION": ["basic_info_CN_FUND"],
    "fund_FUND_OPERPERIOD": ["basic_info_CN_FUND"],
    "fund_FUND_PFMBCHM": ["basic_info_CN_FUND"],
    "fund_FUND_FEE": ["basic_info_CN_FUND"],
    "fund_FUND_NETVALUE": ["basic_info_CN_FUND"],
    "fund_FUND_NETVALUE_MMF": ["basic_info_CN_FUND"],
    "fund_FUND_EODPRICE": ["basic_info_CN_FUND"],
    "fund_FUND_SHARES": ["basic_info_CN_FUND"],
    "fund_FUND_FIN_INCOME": ["basic_info_CN_FUND"],
    "fund_FUND_FIN_BALANCE": ["basic_info_CN_FUND"],
    "fund_FUND_FIN_INDICATOR": ["basic_info_CN_FUND"],
    "fund_FUND_SHRCNVSPLIT": ["basic_info_CN_FUND"],
    "fund_FUND_DIVIDEND": ["basic_info_CN_FUND"],
    "fund_FUND_MANAGER": ["basic_info_CN_FUND"],
    "fund_FUND_INVPOSLMT": ["basic_info_CN_FUND"],
    "fund_FUND_HOLDERSTRUC": ["basic_info_CN_FUND"],
    "fund_FUND_PFL_SECLIST_QDII": ["basic_info_CN_FUND"],
    "fund_FUND_PFL_SECLIST": ["basic_info_CN_FUND"],
    "fund_FUND_PFL_INDUSTRY": ["basic_info_CN_FUND"],
    "fund_FUND_PFL_ASSET": ["basic_info_CN_FUND"],

    'basic_info_CN_FUND': ['basic_info_CN_MUTFUND', 'bar1d_CN_FUND'],
    'bar1d_CN_FUND': ['adjust_factor_CN_FUND'],
    'adjust_factor_CN_FUND': ['dividend_send_CN_FUND'],
    'dividend_send_CN_FUND': ['instruments_CN_FUND'],
    'basic_info_CN_MUTFUND': ['history_divm_CN_MUTFUND'],
    'history_divm_CN_MUTFUND': ['history_nav_CN_MUTFUND'],
    'history_nav_CN_MUTFUND': ['instruments_CN_MUTFUND'],

    # stock
    # "fetch_source_stock": ["basic_info_CN_STOCK_A"],
    "stock_STK_BASICINFO": ["basic_info_CN_STOCK_A"],
    "stock_STK_SPCTREAT": ["basic_info_CN_STOCK_A"],
    "stock_STK_INFOCHG": ["basic_info_CN_STOCK_A"],
    "stock_STK_FREECRCSHR": ["basic_info_CN_STOCK_A"],
    "stock_STK_SCMEMBERS": ["basic_info_CN_STOCK_A"],
    "stock_STK_IPO": ["basic_info_CN_STOCK_A"],
    "stock_STK_EXRIGHT": ["basic_info_CN_STOCK_A"],
    "stock_STK_DIVIDEND": ["basic_info_CN_STOCK_A"],
    "stock_STK_SUSPENSION": ["basic_info_CN_STOCK_A"],
    "stock_STK_BLOCKTRADE": ["basic_info_CN_STOCK_A"],

    'basic_info_CN_STOCK_A': ['stock_STK_EODPRICE', 'stock_STK_VALUATION', 'stock_STK_TA_VOLUME'],
    'stock_STK_EODPRICE': ['bar1d_CN_STOCK_A'],
    'stock_STK_VALUATION': ['bar1d_CN_STOCK_A'],
    'stock_STK_TA_VOLUME': ['bar1d_CN_STOCK_A'],

    'bar1d_CN_STOCK_A': ['bar1d_CN_STOCK_A_fill'],
    'bar1d_CN_STOCK_A_fill': ['instruments_CN_STOCK_A'],
    'instruments_CN_STOCK_A': ['stock_status_CN_STOCK_A'],
    'stock_status_CN_STOCK_A': ['industry_CN_STOCK_A'],
    'industry_CN_STOCK_A': ['market_value_CN_STOCK_A'],
    'market_value_CN_STOCK_A': ['dividend_send_CN_STOCK_A'],
    'dividend_send_CN_STOCK_A': ['financial_statement_CN_STOCK_A'],
    'financial_statement_CN_STOCK_A': ['financial_statement_ff_CN_STOCK_A'],
    'financial_statement_ff_CN_STOCK_A': ['bar1d_CN_STOCK_A_all'],
    # '': [''],

    # index
    # 'fetch_source_index': ['basic_info_index_CN_STOCK_A'],
    "index_IDX_BASICINFO": ["basic_info_index_CN_STOCK_A"],
    'basic_info_index_CN_STOCK_A': ['index_IDX_EODVALUE', 'index_IDX_COMPONENTS', 'index_IDX_WT_STK'],
    # 'fetch_source_index_eod': ['bar1d_index_CN_STOCK_A'],
    "index_IDX_EODVALUE": ["bar1d_index_CN_STOCK_A"],
    "index_IDX_COMPONENTS": ["bar1d_index_CN_STOCK_A"],
    "index_IDX_WT_STK": ["bar1d_index_CN_STOCK_A"],

    'bar1d_index_CN_STOCK_A': ['index_constituent_CN_STOCK_A'],
    'index_constituent_CN_STOCK_A': ['index_element_weight'],
    'index_element_weight': ['bar1d_CN_STOCK_A_all'],
    'bar1d_CN_STOCK_A_all': ['features_CN_STOCK_A'],

    # future
    # "fetch_source_future": ["basic_info_CN_FUTURE"],
    "future_FTR_BASICINFO": ["basic_info_CN_FUTURE"],
    "future_FTR_EODPRICE": ["basic_info_CN_FUTURE"],
    "future_FTR_CNVFACTOR_TF": ["basic_info_CN_FUTURE"],

    "basic_info_CN_FUTURE": ["dominant_CN_FUTURE"],
    "dominant_CN_FUTURE": ["bar1d_CN_FUTURE"],
    "bar1d_CN_FUTURE": ["bar1d_CN_FUTURE_ffill"],
    "bar1d_CN_FUTURE_ffill": ["bar1d_CN_FUTURE_0000"],
    "bar1d_CN_FUTURE_0000": ["instrument_CN_FUTURE"],

}

for task_id, downstream_tasks in task_downstream_map.items():
    task = task_instance.get(task_id)
    for sub_task_id in downstream_tasks:
        downstream_task = task_instance.get(sub_task_id)
        task.set_downstream(downstream_task)