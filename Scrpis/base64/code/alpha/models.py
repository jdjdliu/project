from sdk.db.sql import CreatedAtMixin, CreatorMixin, IntPrimaryKeyMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin
from tortoise import Model, fields

from .constants import AlphaBuildType, AlphaType, AuditStatus, MemberRole, PerformanceSource, ProductType


class Alpha(Model, IntPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin, CreatorMixin):
    name = fields.CharField(max_length=128)
    alpha_id = fields.CharField(max_length=16, null=True)

    column = fields.CharField(max_length=128)
    alphas = fields.JSONField(null=True)
    parameter = fields.JSONField()
    expression = fields.TextField(null=True)
    dependencies = fields.JSONField(null=True)

    catalog_id = fields.UUIDField(null=True)

    alpha_type = fields.CharEnumField(enum_type=AlphaType, max_length=16)
    product_type = fields.CharEnumField(enum_type=ProductType, max_length=16)
    build_type = fields.CharEnumField(enum_type=AlphaBuildType, max_length=16, default=AlphaBuildType.WIZARD)

    task_id = fields.UUIDField()

    class Meta:
        app = "alpha"
        table = "alpha__alpha"
        table_description = "因子/指数信息"


class Backtest(Model, IntPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin, CreatorMixin):
    name = fields.CharField(max_length=128)

    column = fields.CharField(max_length=128, null=True)
    alphas = fields.JSONField(null=True)
    parameter = fields.JSONField(null=True)
    expression = fields.TextField(null=True)
    dependencies = fields.JSONField(null=True)

    alpha_type = fields.CharEnumField(enum_type=AlphaType, max_length=16)
    product_type = fields.CharEnumField(enum_type=ProductType, max_length=16)
    build_type = fields.CharEnumField(enum_type=AlphaBuildType, max_length=16, default=AlphaBuildType.WIZARD)

    task_id = fields.UUIDField()

    class Meta:
        app = "alpha"
        table = "alpha__backtest"
        table_description = "回测记录"


class Performance(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin):
    alpha_id = fields.IntField()
    run_datetime = fields.DatetimeField()
    source = fields.CharEnumField(enum_type=PerformanceSource, max_length=16)

    # IC/IR
    ic_mean = fields.FloatField(index=True, null=True)
    ic_std = fields.FloatField(index=True, null=True)
    ic_significance_ratio = fields.FloatField(index=True, null=True)
    ic_ir = fields.FloatField(index=True, null=True)
    ic_positive_count = fields.IntField(index=True, null=True)
    ic_negative_count = fields.IntField(index=True, null=True)
    ic_skew = fields.FloatField(index=True, null=True)
    ic_kurt = fields.FloatField(index=True, null=True)

    # 最小分位表现
    returns_total_min_quantile = fields.FloatField(index=True, null=True)
    returns_255_min_quantile = fields.FloatField(index=True, null=True)
    returns_66_min_quantile = fields.FloatField(index=True, null=True)
    returns_22_min_quantile = fields.FloatField(index=True, null=True)
    returns_5_min_quantile = fields.FloatField(index=True, null=True)
    returns_1_min_quantile = fields.FloatField(index=True, null=True)
    max_drawdown_min_quantile = fields.FloatField(index=True, null=True)
    profit_loss_ratio_min_quantile = fields.FloatField(index=True, null=True)
    win_ratio_min_quantile = fields.FloatField(index=True, null=True)
    sharpe_ratio_min_quantile = fields.FloatField(index=True, null=True)
    returns_volatility_min_quantile = fields.FloatField(index=True, null=True)

    # 最大分位表现
    returns_total_max_quantile = fields.FloatField(index=True, null=True)
    returns_255_max_quantile = fields.FloatField(index=True, null=True)
    returns_66_max_quantile = fields.FloatField(index=True, null=True)
    returns_22_max_quantile = fields.FloatField(index=True, null=True)
    returns_5_max_quantile = fields.FloatField(index=True, null=True)
    returns_1_max_quantile = fields.FloatField(index=True, null=True)
    max_drawdown_max_quantile = fields.FloatField(index=True, null=True)
    profit_loss_ratio_max_quantile = fields.FloatField(index=True, null=True)
    win_ratio_max_quantile = fields.FloatField(index=True, null=True)
    sharpe_ratio_max_quantile = fields.FloatField(index=True, null=True)
    returns_volatility_max_quantile = fields.FloatField(index=True, null=True)

    # 多空组合表现
    returns_total_ls_combination = fields.FloatField(index=True, null=True)
    returns_255_ls_combination = fields.FloatField(index=True, null=True)
    returns_66_ls_combination = fields.FloatField(index=True, null=True)
    returns_22_ls_combination = fields.FloatField(index=True, null=True)
    returns_5_ls_combination = fields.FloatField(index=True, null=True)
    returns_1_ls_combination = fields.FloatField(index=True, null=True)
    max_drawdown_ls_combination = fields.FloatField(index=True, null=True)
    profit_loss_ratio_ls_combination = fields.FloatField(index=True, null=True)
    win_ratio_ls_combination = fields.FloatField(index=True, null=True)
    sharpe_ratio_ls_combination = fields.FloatField(index=True, null=True)
    returns_volatility_ls_combination = fields.FloatField(index=True, null=True)

    # 因子收益率
    beta_mean = fields.FloatField(index=True, null=True)
    beta_std = fields.FloatField(index=True, null=True)
    beta_positive_ratio = fields.FloatField(index=True, null=True)
    abs_t_mean = fields.FloatField(index=True, null=True)
    abs_t_value_over_2_ratio = fields.FloatField(index=True, null=True)
    p_value_less_ratio = fields.FloatField(index=True, null=True)

    class Meta:
        app = "alpha"
        table = "alpha__performance"
        table_description = "因子/指数历史绩效"


class IndexPerformance(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin):
    alpha_id = fields.IntField()
    run_datetime = fields.DatetimeField()
    source = fields.CharEnumField(enum_type=PerformanceSource, max_length=16)

    total_revenue = fields.FloatField(null=True)
    stock_num = fields.IntField(null=True)

    class Meta:
        app = "alpha"
        table = "alpha__index_performance"
        table_description = "因子/指数历史绩效"


class Repo(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    name = fields.CharField(max_length=128)
    description = fields.CharField(max_length=256, null=True)
    is_public = fields.BooleanField(default=True)
    repo_type = fields.CharEnumField(enum_type=AlphaType, max_length=16, default=AlphaType.ALPHA)

    class Meta:
        app = "alpha"
        table = "alpha__repo"
        table_description = "因子库"


class Member(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    user_id = fields.UUIDField()
    repo_id = fields.UUIDField()
    role = fields.CharEnumField(enum_type=MemberRole, max_length=64)

    class Meta:
        app = "alpha"
        table = "alpha__member"
        table_description = "因子库成员"


class Audit(Model, UUIDPrimaryKeyMixin, CreatorMixin, CreatedAtMixin, UpdatedAtMixin):
    alpha_id = fields.IntField()
    repo_id = fields.UUIDField()

    status = fields.CharEnumField(enum_type=AuditStatus, max_length=64)
    auditor = fields.UUIDField(null=True)

    class Meta:
        app = "alpha"
        table = "alpha__audit"
        table_description = "因子库提交记录"


class Collection(Model, CreatorMixin, CreatedAtMixin, UUIDPrimaryKeyMixin):
    alpha_id = fields.IntField()

    class Meta:
        app = "alpha"
        table = "alpha__collection"
        table_description = "因子/指数收藏"


class Catalog(Model, UUIDPrimaryKeyMixin, CreatedAtMixin, UpdatedAtMixin, CreatorMixin):
    name = fields.CharField(max_length=128)
    description = fields.TextField(null=True)
    alpha_type = fields.CharEnumField(enum_type=AlphaType, max_length=16, default=AlphaType.ALPHA)

    class Meta:
        app = "alpha"
        table = "alpha__catalog"
        table_description = "因子/指数分类"
