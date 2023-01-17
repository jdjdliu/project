from enum import Enum


class AlphaType(str, Enum):
    """
    因子类型

    ALPHA: 因子
    INDEX: 指数
    """

    ALPHA = "ALPHA"
    INDEX = "INDEX"


class ProductType(str, Enum):
    """
    资产类型

    STOCK: 股票
    FUND: 基金
    FUTURE: 期货
    """

    STOCK = "STOCK"
    FUND = "FUND"
    FUTURE = "FUTURE"


class PerformanceSource(str, Enum):
    """
    绩效来源

    BACKTEST: 回测
    ALPHA: 因子
    """

    BACKTEST = "BACKTEST"
    ALPHA = "ALPHA"


class MemberRole(str, Enum):
    """
    成员角色

    OWNER: 创建人
    MANAGER: 管理人
    RESEARCHER: 研究员
    """

    OWNER = "OWNER"
    MANAGER = "MANAGER"
    RESEARCHER = "RESEARCHER"


class AuditStatus(str, Enum):
    """
    审核状态

    PENDING: 待审核
    APPROVED: 审核通过
    REJECTED: 审核拒绝
    """

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class RebalancePrice(str, Enum):
    """
    收益价格
    """

    CLOSE_0 = "close_0"
    OPEN_0 = "open_0"
    VWAP = "vwap"


class StockPool(str, Enum):
    """
    股票池

    ALL: 全市场
    IN_CSI300_0: 沪深300
    IN_CSI500_0: 中证500
    IN_CSI800_0: 中证800
    """

    ALL = "全市场"
    IN_CSI300_0 = "沪深300"
    IN_CSI500_0 = "中证500"
    IN_CSI800_0 = "中证800"


class ReturnsCalculationMethod(str, Enum):
    """
    收益计算方式

    CUM_PROD: 累乘
    CUM_SUM: 累加
    """

    CUM_PROD = "累乘"
    CUM_SUM = "累加"


class Benchmark(str, Enum):
    """
    收益率基准, 选中无则计算绝对收益, 选中其他基准则计算对应基准的相对收益(分组收益计算)

    NONE: 无
    HIX_000300: 沪深300
    HIX_000905: 中证500
    HIX_000906: 中证800
    """

    NONE = "无"
    HIX_000300 = "沪深300"
    HIX_000905 = "中证500"
    HIX_000906 = "中证800"
    IN_CSI1000_0 = "中证1000"
    GEM = "创业板"


class Neutralization(str, Enum):
    """
    中性化风险因子
    利用回归得到一个与风险因子线性无关的因子, 用残差作为中性化后的新因子

    INDUSTRY: 行业
    SIZE: 市值
    """

    INDUSTRY = "行业"
    SIZE = "市值"


class Metric(str, Enum):
    """
    指标

    QUANTILE_RETURNS: 因子表现概览
    BASIC_DESCRIPTION: 因子分布
    INDUSTRY: 因子行业分布
    MARKET_CAP: 因子市值分布
    IC: IC分析
    REBALANCE_OVERLAP: 买入信号重合分析
    PB_RATIO: 因子估值分析
    TURNOVER: 因子拥挤度分析
    STOCKS: 因子值最大/最小股票
    FACTOR_VALUE: 表达式因子值
    FACTOR_PAIRWISE_CORRELATION_MERGED: 多因子相关性分析
    """

    QUANTILE_RETURNS = "因子表现概览"
    BASIC_DESCRIPTION = "因子分布"
    INDUSTRY = "因子行业分布"
    MARKET_CAP = "因子市值分布"
    IC = "IC分析"
    REBALANCE_OVERLAP = "买入信号重合分析"
    PB_RATIO = "因子估值分析"
    TURNOVER = "因子拥挤度分析"
    STOCKS = "因子值最大/最小股票"
    FACTOR_VALUE = "表达式因子值"
    FACTOR_PAIRWISE_CORRELATION_MERGED = "多因子相关性分析"


class UserDataMergeMethod(str, Enum):
    """
    用户数据合并方式
    """

    LEFT = "left"
    INNER = "inner"


class FilterOperator(str, Enum):
    """
    过滤条件运算符
    """

    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"


class TaskType(str, Enum):
    """
    任务类型
    """

    Alpha = "因子计算"


class AlphaBuildType(str, Enum):
    """
    因子构建类型
        WIZARD: 向导式
        CODED: 代码式
    """

    WIZARD = "WIZARD"
    CODED = "CODED"


class WeightMethod(str, Enum):
    """
    加权类型
    """

    WEIGHTED_MEAN = "等权重"
    WEIGHTED_MARKER_CAP = "市值加权"


class Sort(str, Enum):
    """
    排序方式
    """

    ASC = "升序"
    DESC = "降序"
