from pathlib import Path
from typing import List

import yaml
from fastapi import APIRouter

from alpha.models import Alpha, Catalog
from alpha.schemas import AlphaCatalogResponse, AlphaSchema, CatalogSchema, IndexCatalogResponse, IndexSchema, PredefinedMarket
from sdk.httputils import ResponseSchema

from ..constants import AlphaType

router = APIRouter()


DATA_FILEPATH = Path(__file__).parent.parent / "predefined_alphas.yaml"


with DATA_FILEPATH.open() as f:
    DATA = yaml.load(f, Loader=yaml.Loader)["data"]


@router.get("", response_model=ResponseSchema[List[PredefinedMarket]])
async def get_predefined_alphas() -> ResponseSchema[List[PredefinedMarket]]:
    return ResponseSchema(data=DATA)  # 向导式构建使用的


@router.get("/alpha", response_model=ResponseSchema[List[AlphaCatalogResponse]])
async def get_alpha_catalog() -> ResponseSchema[List[AlphaCatalogResponse]]:
    catalogs = await Catalog.filter(alpha_type=AlphaType.ALPHA)
    data = [
        AlphaCatalogResponse(
            catalog=CatalogSchema.from_orm(catalog),
            alphas=[AlphaSchema.from_orm(alpha) for alpha in await Alpha.filter(catalog_id=catalog.id, alpha_type=AlphaType.ALPHA)],
        )
        for catalog in catalogs
    ]
    return ResponseSchema(data=data)


@router.get("/index", response_model=ResponseSchema[List[IndexCatalogResponse]])
async def get_index_catalog() -> ResponseSchema[List[IndexCatalogResponse]]:
    catalogs = await Catalog.filter(alpha_type=AlphaType.INDEX)
    data = [
        IndexCatalogResponse(
            catalog=CatalogSchema.from_orm(catalog),
            alphas=[IndexSchema.from_orm(alpha) for alpha in await Alpha.filter(catalog_id=catalog.id, alpha_type=AlphaType.INDEX)],
        )
        for catalog in catalogs
    ]
    return ResponseSchema(data=data)
