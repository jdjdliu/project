import copy
import json
from datetime import datetime
from typing import Any, Dict

from alpha.schemas import CreateBacktestRequest, CreateIndexBacktestRequest
from alpha.templates import (ALPHA_STOCK_TEMPLATE, BLANK_NOTEBOOK_TEMPLATE,
                             INDEX_STOCK_TEMPLATE)


def generate_notebook(request: CreateBacktestRequest) -> str:
    notebook: Dict[Any, Any] = copy.deepcopy(BLANK_NOTEBOOK_TEMPLATE)

    parameter = request.parameter.dict()

    source_code = ALPHA_STOCK_TEMPLATE.format_map(
        {
            **parameter,
            "name": request.name,
            "expression": request.expression,
            "generated_at": datetime.now().strftime("%Y年%m月%d日 %H:%M"),
        }
    )
    notebook["cells"][0]["source"] = [line + "\n" for line in source_code.split("\n")]
    return json.dumps(notebook, ensure_ascii=False)


def index_generate_notebook(request: CreateIndexBacktestRequest) -> str:
    notebook: Dict[Any, Any] = copy.deepcopy(BLANK_NOTEBOOK_TEMPLATE)

    parameter = request.parameter.dict()
    source_code = INDEX_STOCK_TEMPLATE.format_map(
        {
            **parameter,
            "factor_name": request.parameter.factor_name[0],
            "name": request.name,
            "expression": request.expression, # TODO 
            "generated_at": datetime.now().strftime("%Y年%m月%d日 %H:%M"),
        }
    )
    notebook["cells"][0]["source"] = [line + "\n" for line in source_code.split("\n")]
    return json.dumps(notebook, ensure_ascii=False)
