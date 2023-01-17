from sdk.db.sql import CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin
from tortoise import Model, fields


class User(
    Model,
    UUIDPrimaryKeyMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
):
    username = fields.CharField(max_length=64)
    yst_code = fields.CharField(max_length=16, null=True)
    role_group_id = fields.TextField(null=True)

    class Meta:
        app = "userservice"
        table = "userservice__user"


class Share(
    Model,
    UUIDPrimaryKeyMixin,
    CreatorMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
):
    name = fields.CharField(max_length=64)
    notebook = fields.TextField()
    html = fields.TextField()

    class Meta:
        app = "userservice"
        table = "userservice__share"


class ShareData(
    Model,
    UUIDPrimaryKeyMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
):
    data = fields.TextField()
    data_type = fields.CharField(max_length=16)

    class Meta:
        app = "userservice"
        table = "userservice__share_data"


class TemplateFile(
    Model,
    UUIDPrimaryKeyMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
):
    userbox_id: int = fields.IntField()
    templates: str = fields.TextField()

    class Meta:
        app = "userservice"
        table = "userservice__template_file"
