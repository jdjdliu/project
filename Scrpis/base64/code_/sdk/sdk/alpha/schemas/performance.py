from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from sdk.common import schemas

from ..constants import PerformanceSource


class CreatePerformanceSchema(BaseModel):
    run_datetime: datetime = Field(..., description="绩效时间")
    source: PerformanceSource = Field(..., description="绩效来源")

    # IC/IR
    ic_mean: Optional[float] = Field(..., description="IC均值")
    ic_std: Optional[float] = Field(..., description="IC标准差")
    ic_significance_ratio: Optional[float] = Field(..., description="|IC| > 0.02比率")
    ic_ir: Optional[float] = Field(..., description="IR值")
    ic_positive_count: Optional[float] = Field(..., description="IC正值次数")
    ic_negative_count: Optional[float] = Field(..., description="IC负值次数")
    ic_skew: Optional[float] = Field(..., description="IC偏度")
    ic_kurt: Optional[float] = Field(..., description="IC峰度")

    # 最小分位表现
    returns_total_min_quantile: Optional[float] = Field(..., description="累计收益(最小分位)")
    returns_255_min_quantile: Optional[float] = Field(..., description="近1年收益(最小分位)")
    returns_66_min_quantile: Optional[float] = Field(..., description="近3月收益(最小分位)")
    returns_22_min_quantile: Optional[float] = Field(..., description="近1月收益(最小分位)")
    returns_5_min_quantile: Optional[float] = Field(..., description="近1周收益(最小分位)")
    returns_1_min_quantile: Optional[float] = Field(..., description="昨日收益(最小分位)")
    max_drawdown_min_quantile: Optional[float] = Field(..., description="最大回撤(最小分位)")
    profit_loss_ratio_min_quantile: Optional[float] = Field(..., description="盈亏比(最小分位)")
    win_ratio_min_quantile: Optional[float] = Field(..., description="胜率(最小分位)")
    sharpe_ratio_min_quantile: Optional[float] = Field(..., description="夏普比率(最小分位)")
    returns_volatility_min_quantile: Optional[float] = Field(..., description="收益波动率(最小分位)")

    # 最大分位表现
    returns_total_max_quantile: Optional[float] = Field(..., description="累计收益(最大分位)")
    returns_255_max_quantile: Optional[float] = Field(..., description="近1年收益(最大分位)")
    returns_66_max_quantile: Optional[float] = Field(..., description="近3月收益(最大分位)")
    returns_22_max_quantile: Optional[float] = Field(..., description="近1月收益(最大分位)")
    returns_5_max_quantile: Optional[float] = Field(..., description="近1周收益(最大分位)")
    returns_1_max_quantile: Optional[float] = Field(..., description="昨日收益(最大分位)")
    max_drawdown_max_quantile: Optional[float] = Field(..., description="最大回撤(最大分位)")
    profit_loss_ratio_max_quantile: Optional[float] = Field(..., description="盈亏比(最大分位)")
    win_ratio_max_quantile: Optional[float] = Field(..., description="胜率(最大分位)")
    sharpe_ratio_max_quantile: Optional[float] = Field(..., description="夏普比率(最大分位)")
    returns_volatility_max_quantile: Optional[float] = Field(..., description="收益波动率(最大分位)")

    # 多空组合表现
    returns_total_ls_combination: Optional[float] = Field(..., description="累计收益(多空组合)")
    returns_255_ls_combination: Optional[float] = Field(..., description="近1年收益(多空组合)")
    returns_66_ls_combination: Optional[float] = Field(..., description="近3月收益(多空组合)")
    returns_22_ls_combination: Optional[float] = Field(..., description="近1月收益(多空组合)")
    returns_5_ls_combination: Optional[float] = Field(..., description="近1周收益(多空组合)")
    returns_1_ls_combination: Optional[float] = Field(..., description="昨日收益(多空组合)")
    max_drawdown_ls_combination: Optional[float] = Field(..., description="最大回撤(多空组合)")
    profit_loss_ratio_ls_combination: Optional[float] = Field(..., description="盈亏比(多空组合)")
    win_ratio_ls_combination: Optional[float] = Field(..., description="胜率(多空组合)")
    sharpe_ratio_ls_combination: Optional[float] = Field(..., description="夏普比率(多空组合)")
    returns_volatility_ls_combination: Optional[float] = Field(..., description="收益波动率(多空组合)")

    # 因子收益率
    beta_mean: Optional[float] = Field(..., description="因子收益均值")
    beta_std: Optional[float] = Field(..., description="因子收益标准差")
    beta_positive_ratio: Optional[float] = Field(..., description="因子收益为正比率")
    abs_t_mean: Optional[float] = Field(..., description="t值绝对值的均值")
    abs_t_value_over_2_ratio: Optional[float] = Field(..., description="t值绝对值大于2的比率")
    p_value_less_ratio: Optional[float] = Field(..., description="因子收益t检验p值小于0.05的比率")

    class Config:
        use_enum_values = True


class CreateIndexPerformanceSchema(BaseModel):
    run_datetime: datetime = Field(..., description="绩效时间")
    source: Optional[str] = Field(..., description="数据来源")

    total_revenue: Optional[float] = Field(..., description="总收益")
    stock_num: Optional[int] = Field(..., description="样本数量")

    class Config:
        orm_mode = True
        use_enum_values = True


class PerformanceSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    CreatePerformanceSchema,
):
    alpha_id: int = Field(..., description="因子ID")

    class Config:
        orm_mode = True
        use_enum_values = True


class IndexPerformanceSchema(
    schemas.UUIDIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    CreateIndexPerformanceSchema,
):
    alpha_id: int = Field(..., description="因子ID")

    class Config:
        orm_mode = True
        use_enum_values = True
