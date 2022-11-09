from tortoise import fields
from tortoise.models import Model


class DatetimeMixin(Model):
    created_at = fields.DatetimeField(null=True, auto_now_add=True)
    updated_at = fields.DatetimeField(null=True, auto_now=True)

    class Meta:
        abstract = True
