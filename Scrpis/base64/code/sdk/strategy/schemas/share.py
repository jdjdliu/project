from uuid import UUID

from pydantic import BaseModel, Field
from sdk.strategy.constants import RecordType


class StrategyShareCreateRequest(BaseModel):
    record_id: UUID = Field(..., description="strategy or backtest id")
    record_type: RecordType = Field(..., description="stratgy/backtest")
    share_performance: bool = Field(default=False, description="是否分享performance")
    share_notebook: bool = Field(default=False, description="是否分享notebook")
