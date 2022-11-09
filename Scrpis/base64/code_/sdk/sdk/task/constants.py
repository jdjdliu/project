from enum import Enum


class TaskType(str, Enum):
    ONCE = "ONCE"
    CRON = "CRON"


class TaskStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    INIT = "INIT"
    INITING = "INITING"
    INIT_FAILED = "INIT_FAILED"
