import copy
import json
from typing import Any, Dict, List

from sdk.strategy import FilterCondParameter, FilterOperator, IndexType, SelectTimeParam
from strategy.schemas import CreateStrategyBacktestFromWizardRequest
from strategy.templates import BLANK_NOTEBOOK_TEMPLATE, STRATEGY_TEMPLATE


def generate_select_time_parameters(parameters: List[SelectTimeParam]):
    select_time_parameters = []
    for (cond_num, parameter) in enumerate(parameters):
        if parameter.index == IndexType.MA:
            select_time_parameters.append("bear_con{} = mean(close, {}) < mean(close, {})".format(cond_num + 1, parameter.val_1, parameter.val_2))
        if parameter.index == IndexType.MACD:
            select_time_parameters.append(
                "bear_con{} = ta_macd_hist(close, fastperiod={}, slowperiod={}, signalperiod={})>ta_macd_dea(close, fastperiod={}, slowperiod={}, signalperiod={})".format(
                    cond_num + 1, parameter.val_1, parameter.val_2, parameter.val_3, parameter.val_1, parameter.val_2, parameter.val_3
                )
            )

    select_time_parameters.append("is_hold = {}".format("&".join(["bear_con" + str(i) for i in range(1, len(parameters) + 1)])))

    return select_time_parameters


def generate_filter_cond_composition(parameters: List[FilterCondParameter]):
    filter_cond_list = []
    for filter_cond in parameters:
        if filter_cond.filter == FilterOperator.BETWEEN:
            filter_cond_list.append("({}>{})&({}<{})".format(filter_cond.factor, filter_cond.min_val, filter_cond.factor, filter_cond.max_val))
        if filter_cond.filter == FilterOperator.GT:
            filter_cond_list.append("({}>{})".format(filter_cond.factor, filter_cond.value))
        if filter_cond.filter == FilterOperator.LT:
            filter_cond_list.append("({}<{})".format(filter_cond.factor, filter_cond.value))
    return "filter_cond={}".format("&".join(filter_cond_list))


def generate_notebook(request: CreateStrategyBacktestFromWizardRequest) -> str:
    notebook: Dict[Any, Any] = copy.deepcopy(BLANK_NOTEBOOK_TEMPLATE)

    parameter = request.parameter.dict()

    source_code = STRATEGY_TEMPLATE.format_map(
        {
            **parameter,
            "select_time_parameters": generate_select_time_parameters(request.parameter.select_time_parameters),
            "filter_cond_composition": generate_filter_cond_composition(request.parameter.filter_cond),
        }
    )
    notebook["cells"][0]["source"] = [line + "\n" for line in source_code.split("\n")]

    return json.dumps(notebook, ensure_ascii=False)
