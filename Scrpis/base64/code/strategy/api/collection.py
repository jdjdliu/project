from math import ceil
from typing import List, Optional, Sequence
from uuid import UUID

from fastapi import APIRouter, Depends
from sdk.auth import Credential, auth_required
from sdk.httputils import PagerSchema, ResponseSchema
from strategy.models import Collection, Strategy, StrategyPerformance
from strategy.schemas import CollectionSchema, StrategyDashboardSchema, StrategyIdListRequest

router = APIRouter()


@router.get("", response_model=ResponseSchema[PagerSchema[StrategyDashboardSchema]])
async def get_user_collection_strategies(
    page_id: Optional[int] = 1,
    page_num: Optional[int] = 20,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[PagerSchema[StrategyDashboardSchema]]:
    collection_ids = [collection.strategy_id for collection in await Collection.filter(creator=credential.user.id).only("strategy_id")]
    response_list = []
    strategys = await StrategyPerformance.filter(strategy_id__in=collection_ids)
    for strategy in strategys:
        strategy_base_info = await Strategy.get(id=strategy.strategy_id)
        strategy_collected = await Collection.get_or_none(strategy_id=strategy.strategy_id)
        response_list.append(
            StrategyDashboardSchema(
                owner_id=strategy_base_info.creator,
                strategy_id=strategy_base_info.id,
                name=strategy_base_info.name,
                description=strategy_base_info.description,
                capital_base=strategy_base_info.parameter["capital_base"],
                sharpe=strategy.sharpe,
                max_drawdown=strategy.max_drawdown,
                cum_return=strategy.cum_return,
                annual_return=strategy.annual_return,
                today_return=strategy.today_return,
                three_month_return=strategy.three_month_return,
                cum_return_plot=strategy.cum_return_plot,
                benchmark_cum_return_plot=strategy.benchmark_cum_return_plot,
                relative_cum_return_plot=strategy.relative_cum_return_plot,
                collected=True if strategy_collected else False,
            )
        )
    return ResponseSchema(
        data=PagerSchema(
            page=page_id,
            page_size=page_num,
            total_page=ceil(len(response_list) / page_num),
            items_count=len(response_list),
            items=response_list[(page_id - 1) * page_num : page_id * page_num],
        )
    )


@router.post("", response_model=ResponseSchema[List[CollectionSchema]])
async def collection_strategies(
    request: StrategyIdListRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[CollectionSchema]]:
    strategies = await Strategy.filter(id__in=request.strategy_ids).only("id")
    strategies_ids = set([strategy.id for strategy in strategies])

    collecteds = await Collection.filter(creator=credential.user.id, strategy_id__in=strategies_ids)
    collected_ids = set([collected.strategy_id for collected in collecteds])

    ids = strategies_ids - collected_ids

    collections = [
        Collection(
            strategy_id=strategy_id,
            creator=credential.user.id,
        )
        for strategy_id in ids
    ]

    items: Sequence[Collection] = await Collection.bulk_create(collections)

    return ResponseSchema(data=[CollectionSchema.from_orm(item) for item in collecteds] + [CollectionSchema.from_orm(item) for item in items])


@router.delete("/{strategy_id}", response_model=ResponseSchema[CollectionSchema])
async def del_collection_strategy(
    strategy_id: UUID,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[CollectionSchema]:
    col = await Collection.get(strategy_id=strategy_id, creator=credential.user.id)
    await col.delete()
    return ResponseSchema(data=col)
