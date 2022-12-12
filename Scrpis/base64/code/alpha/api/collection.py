from typing import List, Sequence

from fastapi import APIRouter, Depends

from alpha.models import Alpha, Collection
from alpha.schemas import AlphaIdListRequest, AlphaSchema, CollectionSchema
from sdk.alpha.constants import AlphaType
from sdk.auth import Credential, auth_required
from sdk.httputils import ResponseSchema

router = APIRouter()


@router.get("", response_model=ResponseSchema[List[AlphaSchema]])
async def get_user_collection_alphas(
    alpha_type: AlphaType,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[AlphaSchema]]:
    collection_ids = [collection.alpha_id for collection in await Collection.filter(creator=credential.user.id).only("alpha_id")]
    return ResponseSchema(data=[AlphaSchema.from_orm(alpha) for alpha in await Alpha.filter(id__in=collection_ids).filter(alpha_type=alpha_type)])


@router.post("", response_model=ResponseSchema[List[CollectionSchema]])
async def collection_alphas(
    request: AlphaIdListRequest,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[List[CollectionSchema]]:
    alphas = await Alpha.filter(id__in=request.alpha_ids).only("id", "alpha_id")
    alphas_ids = set([alpha.id for alpha in alphas])

    collecteds = await Collection.filter(creator=credential.user.id, alpha_id__in=alphas_ids)
    collected_ids = set([collected.alpha_id for collected in collecteds])

    ids = alphas_ids - collected_ids

    collections = [
        Collection(
            alpha_id=alpha_id,
            creator=credential.user.id,
        )
        for alpha_id in ids
    ]

    items: Sequence[Collection] = await Collection.bulk_create(collections)

    return ResponseSchema(data=[CollectionSchema.from_orm(item) for item in collecteds] + [CollectionSchema.from_orm(item) for item in items])


@router.delete("/{alpha_id}", response_model=ResponseSchema[CollectionSchema])
async def del_collection_alpha(
    alpha_id: int,
    credential: Credential = Depends(auth_required()),
) -> ResponseSchema[CollectionSchema]:
    col = await Collection.get(alpha_id=alpha_id, creator=credential.user.id)
    await col.delete()
    return ResponseSchema(data=col)
