import os
import uuid

from learning.module2.common.data import Outputs
from sdk.auth import Credential
from sdk.strategy import StrategyClient
from sdk.strategy.schemas import UpdateStrategyParameter, UpdateStrategyRequest
from sdk.strategy.schemas.strategy import StrategySchema
from sdk.utils import BigLogger

# log = logbook.Logger('forward_register')
log = BigLogger("forward_register")
bigquant_cacheable = False
bigquant_public = False


class BigQuantModule:
    def __init__(
        self,
        algo_name,
        market_type,
        capital_base,
        first_date,  # e.g.  '2016-10-11'
        description,
        unique_id,  # strategy_id
        benchmark_symbol,
        price_type,
        product_type,
        data_frequency,
    ):
        self._owner = os.environ["JPY_USER"]
        self._algo_name = algo_name
        self._market_type = market_type
        self._capital_base = capital_base
        self._first_date = first_date
        self._description = description
        self._unique_id = unique_id  # strategy_id
        self._benchmark_symbol = benchmark_symbol
        self._price_type = "真实价格"
        self._product_type = "股票"
        self._data_frequency = "daily"

    def run(self):
        credential = Credential.from_env()
        # CMB必须要先创建才能有回测
        strategy: StrategySchema = StrategyClient.get_strategy_by_id(strategy_id=self._unique_id, credential=credential)
        # strategy: StrategySchema = StrategyClient.create_or_update_strategy(requets, credential=credential)
        # returned_dict = invoke_micro_service(
        #     "papertradingservice",
        #     "forward_register",
        #     owner=self._owner,
        #     algo_name=self._algo_name,
        #     market_type=self._market_type,
        #     capital_base=self._capital_base,
        #     first_date=self._first_date,
        #     description=self._description,
        #     unique_id=self._unique_id,
        #     benchmark_symbol=self._benchmark_symbol,
        # )
        return Outputs(
            algo_id=strategy.id,
            algo_unique_id=strategy.id,
            algo_name=strategy.name,
            algo_desc=strategy.description,
            first_date=strategy.parameter.start_date,
            benchmark_symbol=self._benchmark_symbol,
            price_type=self._price_type,
            product_type=self._product_type,
            data_frequency=self._data_frequency,
        )


def bigquant_postrun(outputs):
    return outputs
