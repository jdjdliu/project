from typing import List, Optional

from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise

from .settings import TORTOISE_ORM
from .utils import build_config


def init_db(app: FastAPI, enabled_apps: Optional[List[str]] = None) -> FastAPI:
    config = build_config(TORTOISE_ORM, enabled_apps)

    register_tortoise(
        app=app,
        config=config,
        add_exception_handlers=True,
    )

    return app
