from sdk.db.sql import CreatedAtMixin, CreatorMixin, UpdatedAtMixin, UUIDPrimaryKeyMixin
from sdk.task import TaskStatus, TaskType
from tortoise import Model, fields


class Task(
    Model,
    UUIDPrimaryKeyMixin,
    CreatedAtMixin,
    UpdatedAtMixin,
    CreatorMixin,
):
    dag_id = fields.CharField(max_length=100, description="任务id")
    task_type = fields.CharEnumField(enum_type=TaskType, description="the type of task")

    is_active = fields.BooleanField(description="是否激活")
    status = fields.CharEnumField(default=TaskStatus.INIT, enum_type=TaskStatus, description="the status of task")

    notebook = fields.TextField(description="notebook内容")
    config = fields.JSONField(description="用户配置")

    class Meta:
        app = "task"
        table = "task__task"
        table_description = "用户任务"
