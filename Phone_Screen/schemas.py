from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

T = TypeVar("T")

class FoodSchema(BaseModel):
    id: int = Field(...)
    sentence: str = Field(...)
    date: str = Field(...)
    stars: str = Field(...)

class ResponseSchema(GenericModel, Generic[T]):
    code: int = 200
    message: Optional[str] = "SUCCESS"
    data: Optional[T] = None