from enum import Enum


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


class StrategyBuildType(str, Enum):
    """
    策略构建类型
        WIZARD: 向导式
        CODED: 代码式
    """

    WIZARD = "WIZARD"
    CODED = "CODED"


class ReferenceType(str, Enum):
    """
    参考基准:
        上证50
        沪深300
        中证500
        中证800
        中证1000
        创业板
    """

    SSE_50 = "上证50"
    IN_CSI300_0 = "沪深300"
    IN_CSI500_0 = "中证500"
    IN_CSI800_0 = "中证800"
    IN_CSI1000_0 = "中证1000"
    GEM = "创业板"


class StockPool(str, Enum):
    """
    股票池

    ALL: 全市场
    IN_CSI300_0: 沪深300
    IN_CSI500_0: 中证500
    IN_CSI800_0: 中证800
    """

    ALL = "全市场"
    SSE_50 = "上证50"
    IN_CSI300_0 = "沪深300"
    IN_CSI500_0 = "中证500"
    IN_CSI800_0 = "中证800"


class MarketType(str, Enum):
    """
    上市板
    """

    ALL = "全市场"
    SSE = "上证主板"
    SZSE = "深证主板"
    GEM = "创业板"
    STAR = "科创版"
    BSE = "北交所"


class IndustryType(str, Enum):
    """
    行业
    """

    ALL = "全行业"
    AGRICULTURE = "农林牧渔"
    MINING = "采掘"
    CHEMICAL = "化工"
    IRON = "钢铁"
    NON_FERROUS_METALS = "有色金属"
    ELECTRONIC = "电子"
    CAR = "汽车"
    APPLIANCES = "家用电器"
    FOOD_DRINK = "食品饮料"
    TEXTILE_APPAREL = "纺织服装"
    LIGHT_MANUFACTURING = "轻工制造"
    PHARMACEUTICAL_BIOLOGY = "医药生物"
    UTILITIES = "公用事业"
    TRANSPORTATION = "交通运输"
    REAL_ESTATE = "房地产"
    COMMERCIAL_TRADE = "商业贸易"
    LEISURE_SERVICES = "休闲服务"
    BANK = "银行"
    NON_BANK = "非银金融"
    COMPREHENSIVE = "综合"
    BUILDING_MATERIALS = "建筑材料"
    BUILDING_DECORATION = "建筑装饰"
    ELECTRICAL_EQUIPMENT = "电气设备"
    EQUIPMENT = "机械设备"
    NATIONAL_DEFENSE_INDUSTRY = "国防军工"
    COMPUTER = "计算机"
    MEDIA = "传媒"
    COMMUNICATION = "通信"
    ELSE = "其他"


class WeightMethod(str, Enum):
    """
    加权类型
    """

    WEIGHTED_MEAN = "等权重"
    WEIGHTED_MARKER_CAP = "市值加权"
    WEIGHTED_MARKER_CAP_FLOAT = "流通市值加权"


class FilterOperator(str, Enum):
    """
    选项
    """

    GT = "gt"
    LT = "lt"
    BETWEEN = "between"


class IndexType(str, Enum):
    """
    择时指标
    """

    MA = "MA"
    MACD = "MACD"


class MarketIndex(str, Enum):
    """
    市场指数
    """

    SSE_COMPREHENSIVE = "上证综指"
    SSE_50 = "上证50"
    HS_300 = "沪深300"
    CSI_500 = "中证500"
    SZSE_COMP_SUB_IND = "深证成指"


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


class PerformanceSource(str, Enum):
    """
    绩效来源

    BACKTEST: 回测
    ALPHA: 因子
    """

    BACKTEST = "BACKTEST"
    ALPHA = "ALPHA"


class Directions(str, Enum):
    SELL = "卖"
    BUY = "买"


class StrategySortKeyWords(str, Enum):
    """
    排序方式关键词

    综合排序

    """

    CUM_RETURN = "cum_return"
    ANNUAL_RETURN = "annual_return"
    SHARPE = "sharpe"
    MAX_DRAWDOWN = "max_drawdown"
    THREE_MONTH_RETURN = "three_month_return"


class RecordType(str, Enum):
    """记录的类型
    strategy
    backtest

    """

    STRATEGY = "strategy"
    BACKTEST = "backtest"
