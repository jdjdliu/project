"""HTTP Request And Response Generic Schema"""
from typing import Any, Generic, List, Literal, Mapping, Optional, Sequence, Type, TypeVar, Union, overload

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel
from tortoise.models import Model
from tortoise.queryset import QuerySet

T = TypeVar("T")
ITEM = TypeVar("ITEM", bound=Mapping[Any, Any])
MODEL = TypeVar("MODEL", bound=Model)
BASE_MODEL = TypeVar("BASE_MODEL", bound=BaseModel)


class QueryParamSchema(BaseModel):
    page: int = Field(default=1, description="page number")
    page_size: int = Field(default=20, description="per page size, set -1 to disable paging")

    class Config:
        allow_mutation = False


class PagerSchema(GenericModel, Generic[T]):
    page: int = Field(description="current page size")
    page_size: int = Field(description="page size")
    total_page: int = Field(description="per page size")
    items: List[T] = Field(description="objects list")
    items_count: int = Field(description="total items count")

    class Config:
        allow_mutation = False

    @overload
    @classmethod
    async def from_queryset(
        cls: Type["PagerSchema[MODEL]"],
        queryset: QuerySet[MODEL],
        schema: Literal[None] = None,
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "PagerSchema[MODEL]":
        ...

    @overload
    @classmethod
    async def from_queryset(
        cls: Type["PagerSchema[BASE_MODEL]"],
        queryset: QuerySet[MODEL],
        schema: Type[BASE_MODEL],
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "PagerSchema[BASE_MODEL]":
        ...

    @classmethod
    async def from_queryset(
        cls: Union[
            Type["PagerSchema[MODEL]"],
            Type["PagerSchema[BASE_MODEL]"],
        ],
        queryset: QuerySet[MODEL],
        schema: Optional[Type[BASE_MODEL]] = None,
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "Union[PagerSchema[MODEL], PagerSchema[BASE_MODEL]]":
        if page_params is not None:
            page, page_size = page_params.page, page_params.page_size

        items_count = await queryset.count()

        items: Union[Sequence[MODEL], Sequence[BASE_MODEL]]

        if page_size == 0:
            total_page = 1
            items = await queryset
        else:
            total_page = (items_count // page_size) + 1 if items_count % page_size > 0 else items_count // page_size
            items = await queryset.offset((page - 1) * page_size).limit(page_size)

        if schema is not None:
            return cls(
                page=page,
                page_size=page_size,
                total_page=total_page,
                items=[schema.from_orm(item) for item in items],
                items_count=items_count,
            )

        return cls(
            page=page,
            page_size=page_size,
            total_page=total_page,
            items=items,
            items_count=items_count,
        )

    @overload
    @classmethod
    async def from_list(
        cls: Type["PagerSchema[ITEM]"],
        items: Sequence[ITEM],
        schema: Literal[None] = None,
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "PagerSchema[ITEM]":
        ...

    @overload
    @classmethod
    async def from_list(
        cls: Type["PagerSchema[BASE_MODEL]"],
        items: Sequence[ITEM],
        schema: Type[BASE_MODEL],
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "PagerSchema[BASE_MODEL]":
        ...

    @classmethod
    async def from_list(
        cls: Union[
            Type["PagerSchema[ITEM]"],
            Type["PagerSchema[BASE_MODEL]"],
        ],
        items: Sequence[ITEM],
        schema: Optional[Type[BASE_MODEL]] = None,
        page_params: Optional[QueryParamSchema] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> "Union[PagerSchema[ITEM], PagerSchema[BASE_MODEL]]":
        if page_params is not None:
            page, page_size = page_params.page, page_params.page_size

        items = list(items)
        items_count = len(items)

        if page_size == 0:
            total_page = 1
        else:
            total_page = (items_count // page_size) + 1 if items_count % page_size > 0 else items_count // page_size
            items = items[(page - 1) * page_size : page * page_size]

        if schema is not None:
            return cls(
                page=page,
                page_size=page_size,
                total_page=total_page,
                items_count=items_count,
                items=[schema(**item) for item in items],
            )

        return cls(
            page=page,
            page_size=page_size,
            total_page=total_page,
            items_count=items_count,
            items=items,
        )


class ResponseSchema(GenericModel, Generic[T]):
    code: int = 0
    message: Optional[str] = "SUCCESS"
    data: Optional[T] = None
