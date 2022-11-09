
from sdk.db.sql import CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin
from tortoise import Model, fields

from .constants import AuditStatus, MemberRole, ProductType, RecordType, StrategyBuildType


class StrategyBacktest(Model, CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin):
    name = fields.CharField(max_length=128)
    description = fields.CharField(null=True,max_length=1024)
    parameter = fields.JSONField(null=True)

    product_type = fields.CharEnumField(enum_type=ProductType, max_length=16, default=ProductType.STOCK)
    build_type = fields.CharEnumField(enum_type=StrategyBuildType, max_length=16, default=StrategyBuildType.WIZARD)

    task_id = fields.UUIDField()

    class Meta:
        app = "strategy"
        table = "strategy__backtest"
        table_description = "策略回测信息"


class BacktestPerformance(Model, CreatedAtMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin):
    backtest_id = fields.UUIDField()
    run_date = fields.DatetimeField()
    daily_data = fields.JSONField(description="每日的数据变化")

    return_ratio = fields.FloatField(null=True, description="收益率")
    annual_return_ratio = fields.FloatField(null=True, description="年化收益率")
    benchmark_ratio = fields.FloatField(null=True)
    alpha = fields.FloatField(null=True)
    sharp = fields.FloatField(null=True)
    ir = fields.FloatField(null=True)
    return_volatility = fields.FloatField(null=True)
    max_drawdown: float = fields.FloatField(null=True)
    win_ratio = fields.FloatField(null=True)
    profit_loss_ratio = fields.FloatField(null=True)

    class Meta:
        app = "strategy"
        table = "strategy__backtest_performance"
        table_description = "构建中的策略绩效"


class Strategy(Model, CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin):
    name = fields.CharField(max_length=128)
    description = fields.CharField(null=True,max_length=1024)
    parameter = fields.JSONField()

    product_type = fields.CharEnumField(enum_type=ProductType, max_length=16, default=ProductType.STOCK)
    build_type = fields.CharEnumField(enum_type=StrategyBuildType, max_length=16, default=StrategyBuildType.WIZARD)

    task_id = fields.UUIDField()

    class Meta:
        app = "strategy"
        table = "strategy__strategy"
        table_description = "策略信息"


class Repo(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    name = fields.CharField(max_length=128)
    description = fields.CharField(max_length=256, null=True)
    is_public = fields.BooleanField(default=True)

    class Meta:
        app = "strategy"
        table = "strategy__repo"
        table_description = "策略库"


class Share(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin):
    record_id = fields.UUIDField()
    record_type = fields.CharEnumField(enum_type=RecordType, max_length=64)
    share_performance = fields.BooleanField(default=False)
    share_notebook = fields.BooleanField(default=False)

    class Meta:
        app = "strategy"
        table = "strategy__share"
        table_description = "策略分享表"


class Member(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    user_id = fields.UUIDField()
    repo_id = fields.UUIDField()
    role = fields.CharEnumField(enum_type=MemberRole, max_length=64)

    class Meta:
        app = "strategy"
        table = "strategy__member"
        table_description = "策略库成员"


class Audit(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    strategy_id = fields.UUIDField()
    repo_id = fields.UUIDField()

    status = fields.CharEnumField(enum_type=AuditStatus, max_length=64)
    auditor = fields.UUIDField(null=True)

    class Meta:
        app = "strategy"
        table = "strategy__audit"
        table_description = "策略库提交记录"


class Collection(Model, CreatorMixin, CreatedAtMixin, UUIDPrimaryKeyMixin):
    strategy_id = fields.UUIDField()

    class Meta:
        app = "strategy"
        table = "strategy__collection"
        table_description = "策略收藏"


class StrategyDaily(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin):
    strategy_id = fields.UUIDField()
    performance_id = fields.UUIDField()
    run_date = fields.DatetimeField()
    cash = fields.FloatField(description="现金")
    positions = fields.JSONField(description="持仓")
    transactions = fields.JSONField(descripition="历史交易")
    orders = fields.JSONField(description="交易信号")
    extension = fields.JSONField(descripition="拓展字段")
    portfolio = fields.JSONField(descripition="投资组合")
    risk_indicator = fields.JSONField(descripition="风险评级")
    trading_days = fields.IntField(descripition="交易天数")
    logs = fields.JSONField()
    is_sync = fields.BooleanField(default=1, description="是否同步")
    benchmark = fields.JSONField(null=True, description="基准以及基准结果")
    # benchmark: '{"000300.SHA": 0.008405923843383789, "000300.SHA.CUM": 0.09317076206207275}'

    class Meta:
        app = "strategy"
        table = "strategy__daily"
        table_description = "策略每日运行数据"


class StrategyPerformance(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin):
    strategy_id = fields.UUIDField()
    run_date = fields.DatetimeField()
    sharpe = fields.FloatField(description="夏普值")
    win_ratio = fields.FloatField(description="胜率")
    win_loss_count = fields.FloatField(null=True, descripition="胜负数")
    max_drawdown = fields.FloatField(index=True, description="最大回撤")
    cum_return = fields.FloatField(index=True, description="累计收益")
    annual_return = fields.FloatField(index=True, description="年化收益")
    today_return = fields.FloatField(index=True, description="当日收益")
    ten_days_return = fields.FloatField(description="近10日收益")
    week_return = fields.FloatField(description="近一周收益")
    month_return = fields.FloatField(description="近一月收益")
    six_month_return = fields.FloatField(description="近六月收益")
    three_month_return = fields.FloatField(index=True, description="近三月收益")
    year_return = fields.FloatField(description="近一年收益")
    cum_return_plot = fields.JSONField(description="累计收益图表")
    benchmark_cum_return_plot = fields.JSONField(ndescription="基准累计收益图表")
    relative_cum_return_plot = fields.JSONField(description="相对累计收益图表")
    hold_percent_plot = fields.JSONField(description="持仓比例图表")

    class Meta:
        app = "strategy"
        table = "strategy__performance"
        table_description = "策略表现"
