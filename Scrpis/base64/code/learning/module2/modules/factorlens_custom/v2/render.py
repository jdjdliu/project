import learning.api.tools as T
from jinja2 import Template
from learning.module2.common.utils import display_html


class RenderHtml:
    ic_stats_template = """
    <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
    <div class='kpicontainer'>
        <ul class='kpi'>
            <li><span class='title'>IC均值</span><span class='value'>{{ stats.ic_mean }}</span></li>
            <li><span class='title'>IC标准差</span><span class='value'>{{ stats.ic_std }}</span></li>
            <li><span class='title'>ICIR</span><span class='value'>{{ stats.ic_ir }}</span></li>
            <li><span class='title'>IC正值次数</span><span class='value'>{{ stats.positive_ic_cnt }}次</span></li>
            <li><span class='title'>IC负值次数</span><span class='value'>{{ stats.negative_ic_cnt }}次</span></li>
            <li><span class='title'>IC偏度</span><span class='value'>{{ stats.ic_skew }}</span></li>
            <li><span class='title'>IC峰度</span><span class='value'>{{ stats.ic_kurt }}</span></li>
        </ul>
    </div>
    """
    ols_stats_template = """
    <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
    <div class='kpicontainer'>
        <ul class='kpi'>
            <li><span class='title'>因子收益均值</span><span class='value'>{{ stats.beta_mean }}</span></li>
            <li><span class='title'>因子收益标准差</span><span class='value'>{{ stats.beta_std }}</span></li>
            <li><span class='title'>因子收益为正比率</span><span class='value'>{{ stats.positive_beta_ratio }}%</span></li>
            <li><span class='title'>t值绝对值的均值</span><span class='value'>{{ stats.abs_t_mean }}</span></li>
            <li><span class='title'>t值绝对值大于2的比率</span><span class='value'>{{ stats.abs_t_value_over_two_ratio }}</span></li>
            <li><span class='title'>因子收益t检验p值小于0.05的比率</span><span class='value'>{{ stats.p_value_less_ratio }}</span></li>
        </ul>
    </div>
    """
    group_stats_template = """
    <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
    <div class='kpicontainer'>
        <ul class='kpi'>
            <li><span class='title'>&nbsp;</span>
                {% for k in stats%}
                    <span class='value'>{{ k }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>收益率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].收益率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>近1日收益率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].近1日收益率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>近1周收益率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].近1周收益率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>近1月收益率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].近1月收益率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>年化收益率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].年化收益率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>夏普比率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].夏普比率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>收益波动率</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].收益波动率 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            <li><span class='title'>最大回撤</span>
                {% for k in stats%}
                    <span class='value'>{{ (stats[k].最大回撤 | string)[0:10] }}</span>
                {% endfor %}
            </li>
            </ul>
    </div>
    """

    def __init__(self, ic_data, ic_summary, factor_returns_data, factor_returns_summary, quantile_returns_data, quantile_returns_summary):
        self.ic_df = ic_data
        self.ic_results = ic_summary
        self.ols_stats_df = factor_returns_data
        self.ols_stats_results = factor_returns_summary
        self.group_df = quantile_returns_data
        self.group_df_results = quantile_returns_summary

    def render_results(self, stats_template, results):
        """展示模板信息"""

        def render(stats_template, results):
            html = Template(stats_template).render(stats=results["stats"], title=results["title"])
            display_html(html)

        render(stats_template, results)

    def show_ic(self):
        self.render_results(self.ic_stats_template, self.ic_results)
        T.plot(
            self.ic_df,
            title="IC分析",
            panes=[["ic", "40%"], ["ic_cumsum", "20%"]],
            # height=500，设置高度为500
            options={
                "chart": {"height": 500},
                # 设置颜色
                "series": [
                    {
                        "name": "ic",
                        "color": "#8085e8",
                        "type": "column",
                        "yAxis": 0,
                    },
                    {
                        "name": "ic_cumsum",
                        "color": "#8d4653",
                        "type": "spline",
                        "yAxis": 0,
                    },
                ],
            },
        )

    def show_ols(self):
        self.render_results(self.ols_stats_template, self.ols_stats_results)
        T.plot(
            self.ols_stats_df[["beta", "cum_beta", "roll_beta"]],
            title="因子收益率",
            # high、low显示在第一栏，高度40%，open、close显示在第二栏，其他的在最后一栏
            panes=[["beta", "cum_beta", "40%"], ["roll_beta", "20%"]],
            # height=500，设置高度为500
            options={
                "chart": {"height": 500},
                # 设置颜色
                "series": [
                    {
                        "name": "beta",
                        "color": "#8085e8",
                        "type": "column",
                        "yAxis": 0,
                    },
                    {
                        "name": "cum_beta",
                        "color": "#8d4653",
                        "type": "column",
                        "yAxis": 0,
                    },
                    {
                        "name": "roll_beta",
                        "color": "#91e8e1",
                        "type": "spline",
                        "yAxis": 1,
                    },
                ],
            },
        )

    def show_group(self):
        self.render_results(self.group_stats_template, self.group_df_results)
        T.plot(self.group_df[[i for i in self.group_df.columns if "_pv" in i]])

    def show(self):
        self.show_ic()
        self.show_ols()
        self.show_group()
