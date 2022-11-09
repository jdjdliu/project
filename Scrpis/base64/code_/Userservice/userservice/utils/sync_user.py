import json
from typing import Dict, List
from uuid import UUID

from sdk.db.redis import RedisClient
from userservice.models import User
from userservice.schemas import UserSchema
from userservice.settings import REDIS_USER_CLUSTER_MODE, REDIS_USER_URL


async def sync_user_info() -> List[UserSchema]:
    R = RedisClient.get_client(REDIS_USER_URL, REDIS_USER_CLUSTER_MODE)

    users: Dict[UUID, UserSchema] = {}

    for key in R.keys("token::*"):
        data_json = R.get(key)

        if not data_json:
            continue

        data = json.loads(data_json)
        user_id = UUID(data["userId"])

        if data.get("userName", None) is None:
            continue

        if user_id in users:
            continue

        user = await User.filter(id=user_id).first()
        if user is None:
            user = await User.create(
                id=user_id,
                username=data["userName"],
                yst_code=data.get("ystCode", None),
                role_group_id=data.get("roleGroupId", None),
            )

        users[user_id] = UserSchema.from_orm(user)

    return users
