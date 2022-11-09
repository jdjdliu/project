import copy
import html
import inspect
import json
import os
from base64 import urlsafe_b64encode
from pathlib import Path
from typing import Any, Callable

import nbformat
from lxml.html.clean import Cleaner
from nbconvert import HTMLExporter
from sdk.share import CreateNotebookShareRequest
from userservice.models import ShareData
from userservice.settings import WEB_HOST

PAGE_FILE = Path(__file__).parent / "share-page.j2"
TEMPLATE_FILE = Path(__file__).parent / "share-template.j2"

DEFAULT_ML_STUDIO_DIV = """
<div class="ml-studio">
    <div id="aux-foreground">
        <div id="aux-body">
            <div id="secondary-container">
                <div id="aux-pane-grid">
                    <h1 id="aux-GridHeader" class="itemtitle"></h1>
                    <div id="aux-GridContent">
                        <div id="aux-collectionView" class="aux-collectionView"></div>
                    </div>
                </div>
                <div id="tabcontainer">
                    <h1 class="itemtitle"></h1>
                    <div class="fxshell-tabcontainer">
                        <ul class="fsxshell-tablist"></ul>
                    </div>
                    <div class="aux-tabcontent">
                        <div class="tabcontent"></div>
                        <div id="fxshell-tabshield"></div>
                    </div>
                </div>
            </div>
            <div id="chat-control"></div>
        </div>
    </div>
</div>
"""


async def convert_to_html(request: CreateNotebookShareRequest) -> str:
    new_notebook: str = rebuild_code(request.notebook, request.keep_source)
    if not request.keep_log:
        new_notebook = clean_log(new_notebook)

    notebook: nbformat.NotebookNode = nbformat.reads(new_notebook, as_version=4)  # type: ignore

    html = await nbconvert(notebook, request.keep_source)
    html = await process_inline_img(html)
    html = await process_bigcharts(html)
    html = await clean_html(html)

    if request.keep_button:
        # 添加参数确认是否有这个button
        html = (
            '<div class="notebooksharecontainer notebooksharecontainerinitial">'
            '<div class="notebookclonewrapper">'
            '<div class="btn btn-primary notebookclone" data-content="%s">'
            '<i class="fa fa-copy"></i>克隆策略'
            "</div></div>"
            "%s</div>" % ("", html)
        )
    else:
        html = (
            '<div class="notebooksharecontainer notebooksharecontainerinitial">'
            "%s</div>" %  html
        )
    page = PAGE_FILE.read_text()
    page = page.replace("{{CONTENT}}", html)
    page = page.replace("{{TITLE}}", request.name)
    page = page.replace("{{SITE_NAME}}", os.getenv("SITE_NAME", "策略分享"))
    page = page.replace("{{WEB_HOST}}", WEB_HOST)
    page = page.replace("{{NOTEBOOK}}", urlsafe_b64encode(request.notebook.encode()).decode())
    return page


async def nbconvert(notebook: nbformat.NotebookNode, keep_source: bool) -> str:
    notebook_end = copy.deepcopy(notebook)
    notebook_ml_studio = copy.deepcopy(notebook)  # only ml studio
    cells = copy.deepcopy(notebook.cells)
    ml_studio_json = ""
    ml_studio_div = ""
    html_ml_studio = ""
    html_start = ""
    html_end = ""

    for i in range(len(cells)):
        if "machine_learning_studio" in cells[i].metadata:
            pass
            # ml_studio_div = DEFAULT_ML_STUDIO_DIV
            # if "ml_studio_json" in cells[i].metadata:
            #     ml_studio_json = '<div id ="ml_studio_json">' + html.escape(cells[i].metadata.ml_studio_json) + "</div>"
            #     notebook.cells = notebook.cells[:i]
            #     notebook_end.cells = notebook_end.cells[i + 1 :]
        else:
            notebook_ml_studio.cells.remove(cells[i])

    html_exporter = HTMLExporter()
    html_exporter.template_file = str(TEMPLATE_FILE.absolute())
    html_exporter.template_paths = [".", str(TEMPLATE_FILE.parent.absolute())]
    html_exporter.register_filter(name="filterLogs", jinja_filter=filterLogs)
    html_exporter.register_filter(name="filterErrorLogs", jinja_filter=filterErrorLogs)
    html_exporter.register_filter(name="filterRawLog", jinja_filter=filterRawLog)
    html_exporter.register_filter(name="filterTimestamp", jinja_filter=filterTimestamp)

    if len(notebook.cells) > 0:
        (html_start, _) = html_exporter.from_notebook_node(notebook)

    if len(notebook_ml_studio.cells) > 0 and keep_source:
        (html_ml_studio, _) = html_exporter.from_notebook_node(notebook_ml_studio)
        html_ml_studio = (
            '<div class="cell border-box-sizing code_cell rendered">'
            + ml_studio_div
            + ml_studio_json
            + html_ml_studio.replace('<div class="cell border-box-sizing code_cell rendered">', "")
        )
        if len(notebook_end.cells) > 0:
            (html_end, _) = html_exporter.from_notebook_node(notebook_end)
        return html_start + html_ml_studio + html_end
    else:
        return html_start


async def process_inline_img(html: str) -> str:
    return await find_and_replace(
        html,
        '<img src="data:image/',
        ">",
        lambda t: '<div class="notebookshare-inlineimg" data-content="data:image/%s></div>' % t,
    )


async def process_bigcharts(html: str) -> str:
    async def on_chart_data(data: str) -> str:
        share_data = await ShareData.create(data=data, data_type="json")
        return '<div class="notebookshare-highcharts" data-content="{id}"></div>'.format(id=f"{WEB_HOST}/api/userservice/share/data/{share_data.id}")

    return await find_and_replace(
        html,
        '<div class="bigchart-data"><pre style="display:none">bigcharts-data-start/',
        "/bigcharts-data-end</pre></div>",
        on_chart_data,
    )


async def clean_html(html: str) -> str:
    return str(Cleaner(scripts=True, style=True, safe_attrs_only=False).clean_html(html))


async def find_and_replace(s: str, start_tag: str, end_tag: str, replacement: Callable[[str], Any]) -> str:
    start = 0
    parts = []
    while start < len(s):
        i = s.find(start_tag, start)
        if i == -1:
            break
        j = s.find(end_tag, i + len(start_tag))
        if j == -1:
            break

        parts.append(s[start:i])

        inner = s[i + len(start_tag) : j]

        parts.append(await replacement(inner) if inspect.iscoroutinefunction(replacement) else replacement(inner))

        start = j + len(end_tag)

    parts.append(s[start:])
    return "".join(parts)


def filterLogs(outputs: nbformat.NotebookNode) -> Any:
    result = list(filter(lambda output: output.get("metadata") and output.get("metadata").get("is_log"), outputs))
    return result


def filterErrorLogs(outputs: nbformat.NotebookNode) -> Any:
    result = list(
        filter(
            lambda output: output.get("metadata") and output.get("metadata").get("is_log") and output.get("metadata").get("status") == "ERROR",
            outputs,
        )
    )
    return result


def filterRawLog(log: str) -> Any:
    _rawLog = (
        log.replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace('\\"', '"')
        .replace("\\'", "'")
        .replace("\\a", "\a")
        .replace("\\b", "\b")
        # TODO: copy from /bigquant/bigservice/-/blob/master/notebookshareservice/service.py, to fix?
        # .replace("\\e", "\e")
        .replace("\\000", "")
        .replace("\\v", "\v")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\f", "\f")
    )
    return _rawLog[30:].split(": ", maxsplit=1)[1]


def filterTimestamp(log: str) -> Any:
    _rawLog = (
        log.replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace('\\"', '"')
        .replace("\\'", "'")
        .replace("\\a", "\a")
        .replace("\\b", "\b")
        # .replace("\\e", "\e")
        .replace("\\000", "")
        .replace("\\v", "\v")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\f", "\f")
    )
    return _rawLog[0:28]


def clean_log(notebook: str) -> str:
    content = json.loads(notebook)
    for cell in content["cells"]:
        if cell["cell_type"] != "code":
            continue

        cell["outputs"] = []
        # for i in cell["outputs"]:
        #     i["text"] = []
    return json.dumps(content)


def rebuild_code(notebook: str, keep_source: bool) -> str:
    content = json.loads(notebook)

    content["metadata"]["is_mlstudio"] = "false"

    for cell in content["cells"]:
        if cell["cell_type"] != "code":
            continue

        if not keep_source:
            # clean code
            cell["source"] = ""
            # clean ml_studio
            cell["metadata"].pop("ml_studio_json", None)
            cell["metadata"].pop("machine_learning_studio", None)
            # clean log
            cell["outputs"] = [output for output in cell["outputs"] if "text/html" in output.get("data", {})]

        # rebuild log
        for output in cell["outputs"]:
            if "data" in output and "text/plain" in output["data"] and type(output["data"]["text/plain"]) == str:
                output["data"]["text/plain"] = " " * 30 + ": " + output["data"]["text/plain"]

    return json.dumps(content)
