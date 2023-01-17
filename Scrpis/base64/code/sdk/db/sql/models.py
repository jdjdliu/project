from tortoise import fields


class IntPrimaryKeyMixin:
    id = fields.IntField(pk=True)


class UUIDPrimaryKeyMixin:
    id = fields.UUIDField(pk=True)


class CreatorMixin:
    creator = fields.UUIDField()


class CreatedAtMixin:
    created_at = fields.DatetimeField(auto_now_add=True)


class UpdatedAtMixin:
    updated_at = fields.DatetimeField(auto_now=True)
