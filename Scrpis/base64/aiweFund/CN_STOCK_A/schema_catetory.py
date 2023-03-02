"""
用于管理股票的目录分类以及排序信息，既 sechema中的category 和 rank字段
以免每次修改整个分类的信息都要去各个脚本中修改
"""

# 原表:原表的schema信息暂时放在一个文件CN_STOCK_A/schema/__init__.py中,这里不再统一管理
SOURCE_TABLE = {}

# bigquant表
# A股
# 1.基本数据
basic_info_CN_STOCK_A = ('A股数据/基本信息', 1001001, "A股基本信息")
instruments_CN_STOCK_A = ('A股数据/基本信息', 1001002, "每日A股列表")  # need: basic_info_CN_STOCK_A, bar1d_CN_STOCK_A
basic_info_IndustrySw = ('A股数据/基本信息', 1001003, "申万行业分类基本信息")  # 线上古涛说有更新是手动导入，一般很少变动；公司电脑财通有半完成的脚本参考，依赖的wind表还没有AshareSWindustriesClass+AshareIndustriesCode ('A股数据/基本信息', 1001003, "申万行业分类基本信息")
industry_CN_STOCK_A = ('A股数据/基本信息', 1001004, "A股行业概念")


# 2.行情数据
bar1d_CN_STOCK_A = ('A股数据/行情数据', 1002001, "A股日线行情")
# limit_price_CN_STOCK_A = ... # 依赖的STK_EOFPRICE表缺少涨跌停字段('A股数据/行情数据', 1002002, "A股涨跌停价格")
market_value_CN_STOCK_A = ('A股数据/行情数据', 1002003, "A股估值分析")
stock_status_CN_STOCK_A = ('A股数据/行情数据', 1002004, "A股股票状态")
# net_amount_CN_STOCK_A = ...  # ('A股数据/行情数据', 1002004, "A股资金流") 数据表没有，已提给李雄老师AShareMoneyflow
dividend_send_CN_STOCK_A = ('A股数据/行情数据', 1002004, "A股分红数据")
# rightsissue_CN_STOCK_A = 。。。('A股数据/行情数据', 1002004, "A股配股数据")  # 字段不全，缺少配股公告日、方案进度

# 3.基本面数据
# west_CN_STOCK_A = ('A股数据/基本面数据', 1003001, "A股一致预期")   #数据表没有，已提给李雄老师AShareConsensusRollingData
financial_statement_CN_STOCK_A = ('A股数据/基本面数据', 1003002, "A股财报数据")
financial_statement_ff_CN_STOCK_A = ('A股数据/基本面数据', 1003003, "A股财报数据（向前填充）")


# 指数数据
# 1.基本信息
basic_info_index_CN_STOCK_A = ('指数数据/基本信息', 1001001, "A股指数基本信息")
index_constituent_CN_STOCK_A = ('指数数据/基本信息', 1001002, "A股指数成分")   # 这个加工逻辑有点复杂
index_element_weight = ('指数数据/基本信息', 1001003, "A股指数成分权重")

# 2.行情数据
bar1d_index_CN_STOCK_A = ('指数数据/行情数据', 1002001, "A股指数日线行情")  # 缺少复权因子、换手率，暂时先设置为1和None


bar1d_CN_STOCK_A_all = ('A股数据/行情数据', 1002005, "A股日线行情(含指数)")


# 模拟交易需要的表：
# 1basic_info_CN_STOCK_A
# 1basic_info_index_CN_STOCK_A
# 1industry_CN_STOCK_A
# 1bar1d_CN_STOCK_A
# 1bar1d_index_CN_STOCK_A
# 1bar1d_CN_STOCK_A_all
# 1instruments_CN_STOCK_A
# limit_price_XN_STOCK_A
# 1stock_status_CN_STOCK_A

# 原始表导入共19张
# 股票13张
# 指数4张
# 公司主体表2张；

# 构建宽邦表13张
# 还有5张没有构建--缺失数据字段
