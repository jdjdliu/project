from functools import partial

import learning.api.tools as T
from jinja2 import Template


class BaseRenderer:
    def _build_plot_options(self, height=500, compare=None, color_count=1):
        options = {
            "chart": {
                "height": height,
            }
        }
        if compare is not None:
            options["plotOptions"] = {
                "series": {
                    "compare": "percent",
                }
            }
        colors = [{"color": "#F08B55"}, {"color": "#648FD5"}, {"color": "#E06681"}, {"color": "#E2D269"}, {"color": "#4A8E8E"}]
        options.update({"series": [colors[i] for i in range(color_count) if i < len(colors)]})
        return options

    def _write(self, html, output="display"):
        if output == "script":
            return html
        from IPython.display import HTML, display

        display(HTML(html))

    def _render_cols(self, *funcs):
        contents = [func() for func in funcs]
        if any(contents):
            template = Template(
                """<div class="cols">
            {% for content in contents %}
                {% if content %}
                <div class="col">{{ content }}</div>
                {% endif %}
            {% endfor %}
            </div>"""
            )
            html = template.render({"contents": contents})
            return html
        return ""

    def _plot(self, df, display_col_names, display_index, **kwargs):
        if display_col_names is None and display_index is None:
            return T.plot(df, **kwargs)

        old_columns = df.columns
        old_index = df.index
        try:
            if display_col_names is not None:
                df.columns = display_col_names
            if display_index is not None:
                df.index = display_index
            return T.plot(df, **kwargs)
        finally:
            df.columns = old_columns
            df.index = old_index

    def _format_float(self, v, precision=2, updown_color=False, pct=False):
        # 如果传入值为 None 则直接返回「—」表示该值为 None
        if v is None:
            return "—"
        presentation_type = "f"
        if pct:
            presentation_type = "%"
        s = ("{:." + str(precision) + presentation_type + "}").format(v)
        if updown_color:
            s = '<span class="pct {}">{}</span>'.format("up" if v > 0 else ("down" if v < 0 else ""), s)
        return s

    def _section_title(self, title, tips=None):
        template = Template(
            '<div class="type_title_left"><p>{{ title }}'
            "{% if tips %}"
            '<i class="fa fa-question-circle-o" title="{{ tips }}"></i>'
            "{% endif %}"
            "</p></div>"
        )
        html = template.render({"title": title, "tips": tips})
        return html


class ModuleRenderer(BaseRenderer):
    def __init__(self, data, title=None):
        self.data = data
        self.title = title

    def _write_stocks(self, key, name):
        df = self.data
        rows = df[["instrument", "name", "industry_name", "list_board", "weight"]].values
        lines = []
        for row in rows:
            row_dict = {
                "instrument": row[0],
                "name": row[1],
                "industry_name": row[2],
                "list_board": row[3],
                "weight": self._format_float(row[4], precision=4),
            }
            lines.append(
                """
            <tr>
                <td>{instrument}</td>
                <td>{name}</td>
                <td>{industry_name}</td>
                <td>{list_board}</td>
                <td>{weight}</td>
            </tr>
            """.format(
                    **row_dict
                )
            )

        title = name
        title += " <span>(%s)</span>" % "十大权重"
        return (
            self._section_title(title)
            + """
        <div class="factorlens-stocks-wrapper">
            <table class="factor_table stocks">
                <thead>
                    <tr>
                        <th>证券代码</th>
                        <th>证券名称</th>
                        <th>行业分类</th>
                        <th>上市板块</th>
                        <th>权重</th>
                    </tr>
                </thead>
                <tbody>"""
            + "".join(lines)
            + """
                </tbody>
            </table>
        </div>
        """
        )

    def _plot_pie(self, output="script"):
        df = self.data
        df = df[["name", "weight"]].set_index("name")
        return self._plot(df, None, None, title="前十大权重股", chart_type="pie", options=self._build_plot_options(), output=output)

    def _render_section(self, section_title, contents):
        html = "".join(contents)
        if html:
            html = section_title + html
        return html

    def _render_contents(self):
        contents = [
            self._render_cols(
                partial(self._write_stocks, key="top", name="十大权重"),
                partial(self._plot_pie),
            ),
        ]
        return "".join(contents)

    def render(self, output="display"):
        # inject_code_template = self._inject_code_template()
        template = Template(
            """<div class="factorlens-container">
            <div style="display:flex">
                <div style="flex:1">
                    {% if title %}
                    <h2 class="factorlens-title">{{ title }}</h2>
                    {% endif %}
                </div>
            </div>
            {{ contents }}
        </div>"""
        )
        html = template.render(
            {
                # 'inject_code_template': inject_code_template,
                "title": self.title,
                "contents": self._render_contents(),
            }
        )
        return self._write(html, output=output)
