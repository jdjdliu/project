from datetime import datetime
from typing import List, Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas
from sdk.strategy.constants import Directions


class PositionSchema(BaseModel):
    hold_days: int = Field(..., description="持有天数")
    first_hold_date: datetime = Field(..., description="首次持有日期")
    amount: int = Field(..., description="持仓数量")
    cost_basis: float = Field(..., description="持仓均价")
    last_sale_price: float = Field(..., description="最新价")
    value: float = Field(..., description="总市值")
    cum_return: float = Field(..., description="累计收益")
    last_sale_date: datetime = Field(..., description="最新日期")
    date: datetime = Field(..., description="运行日期")
    name: str = Field(..., description="代码名称")
    sid: str = Field(..., description="标的代码")
    cost_basis_after_adjust: float = Field(..., description="持仓均价（真实价格）")
    price_after_adjust: float = Field(..., description="最新价（真实价格）")
    amount_after_adjust: float = Field(..., description="持仓数量（真实数量）")
    adjust_factor: float = Field(..., description="复权因子")


class TransactionsSchema(BaseModel):
    direction: Directions = Field(..., description="买/卖")
    sid: str = Field(..., description="标的代码")
    price: float = Field(..., description="成交价格")
    amount: float = Field(..., description="持仓数量")
    commission: float = Field(..., description="手续费")
    cost: float = Field(..., description="手续费")
    date: datetime = Field(..., description="日期")
    dt: datetime = Field(..., description="时间")
    value: float = Field(..., description="总价值")
    name: str = Field(..., description="代码名称")
    price_after_adjust: float = Field(..., description="最新价（真实价格）")
    amount_after_adjust: float = Field(..., description="持仓数量（真实数量）")
    adjust_factor: float = Field(..., description="复权因子")


class OrderSchema(BaseModel):
    direction: Directions = Field(..., description="买/卖")
    sid: str = Field(..., description="代码")
    price: float = Field(..., description="价格")
    amount: float = Field(..., description="订单数量")
    date: str = Field(..., description="订单日期")
    dt: datetime = Field(..., description="订单时间")
    name: str = Field(..., description="代码名称")
    amount_after_adjust: float = Field(..., description="持仓数量（真实数量）")
    adjust_factor: float = Field(..., description="复权因子")


class ExtensionSchema(BaseModel):
    # e.g: open/low/
    order_price_field_buy: str = Field(..., description="参考买入时间点")
    # e.g: close/high
    order_price_field_sell: str = Field(..., description="参考卖出时间点")
    is_stock: str = Field(..., description="是否是股票")
    need_settle: str = Field(..., description="是否需要结算")


class PortfolioSchema(BaseModel):
    cum_return: float = Field(..., description="累计收益")
    annual_return: float = Field(..., description="年化收益")
    today_return: float = Field(..., description="当日收益")
    pv: float = Field(..., description="持仓市值")
    max_pv: float = Field(..., description="最大资产值")
    max_drawdown: float = Field(..., description="最大回撤")
    first_date: datetime = Field(..., description="开始运行日期")


class RiskIndicatorSchema(BaseModel):
    alpha: float = Field(..., description="超额收益")
    beta: float = Field(..., description="基准收益")
    volatility: float = Field(..., description="波动率")
    sharpe: float = Field(..., description="夏普率")
    ir: float = Field(..., description="信息比率")


class LogSchema(BaseModel):
    level: str = Field(..., description="日志级别")
    msg: str = Field(..., description="日志内容")


class CreateStrategyDailySchema(BaseModel):
    strategy_id: UUID = Field(..., description="策略id")
    performance_id: UUID = Field(..., description="策略表现id")
    run_date: datetime = Field(..., description="开始运行策略的日期")
    cash: float = Field(..., description="策略资金")
    positions: Any = Field(..., description="持仓详情")
    transactions: Any = Field(..., description="交易详情")
    orders: Any = Field(..., description="订单记录")  # [{"sid": "", "qty": 100}, {}]
    extension: Any = Field()
    portfolio: Any = Field()
    risk_indicator: Any = Field(..., description="风险指标")
    trading_days: int = Field(..., description="交易天数")
    logs: Any = Field(..., description="日志")
    benchmark: Any = Field(..., description="收益率基准")


class StrategyDailySchema(schemas.UUIDIDMixin, BaseModel):
    strategy_id: UUID = Field(..., description="策略id")
    performance_id: UUID = Field(..., description="策略表现id")
    run_date: datetime = Field(..., description="开始运行策略的日期")
    cash: float = Field(..., description="策略资金")
    positions: Any = Field(..., description="持仓详情")
    transactions: Any = Field(..., description="交易详情")
    orders: Any = Field(..., description="订单记录")  # [{"sid": "", "qty": 100}, {}]
    extension: Any = Field()
    portfolio: Any = Field()
    risk_indicator: Any = Field(..., description="风险指标")
    trading_days: int = Field(..., description="交易天数")
    logs: Any = Field(..., description="日志")
    benchmark: Any = Field(..., description="收益率基准")
    # benchmark: '{"000300.SHA": 0.008405923843383789, "000300.SHA.CUM": 0.09317076206207275}'
    class Config:
        orm_mode = True


class StrategyPerformanceSchema(schemas.UUIDIDMixin, BaseModel):
    strategy_id: UUID = Field(..., description="策略id")
    run_date: Any = Field(..., descripition="运行时间")
    sharpe: float = Field(..., description="夏普率")

    win_ratio: float = Field(..., description="胜率")
    win_loss_count: Optional[int] = Field(default=0, description="胜率")

    max_drawdown: float = Field(..., description="最大回撤")
    cum_return: float = Field(..., description="累计收益")
    annual_return: float = Field(..., description="年化收益")
    today_return: float = Field(..., description="当日收益")
    ten_days_return: float = Field(..., description="十日收益")
    week_return: float = Field(..., description="周收益")
    month_return: float = Field(..., description="月收益")
    six_month_return: float = Field(..., description="六月收益")
    three_month_return: float = Field(..., description="三月收益")
    year_return: float = Field(..., description="年收益")
    # e.g.[[float,float],[float,float]]
    cum_return_plot: Any = Field(..., description="策略累计收益曲线")
    benchmark_cum_return_plot: Any = Field(..., description="基准累计收益曲线")
    relative_cum_return_plot: Any = Field(..., description="相对累计收益曲线")
    hold_percent_plot: Any = Field(..., description="持仓占比累计收益曲线")

    class Config:
        orm_mode = True
