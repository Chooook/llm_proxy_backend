from datetime import datetime, timedelta
import uuid

from aioredis import Redis
from jose import jwt, JWTError
from fastapi import HTTPException, Request

from settings import settings

SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_DAYS = settings.ACCESS_TOKEN_EXPIRE_DAYS


def create_guest_user():
    return str(uuid.uuid4())


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({'exp': expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(request: Request, redis: Redis):
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        stored_user_id = await redis.get(f"token:{token}")
        if stored_user_id != user_id:
            raise HTTPException(
                status_code=401, detail="Token invalid or revoked")

        return user_id

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def renew_token(token: str, redis: Redis):
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("sub")
    if not user_id or await redis.get(f"token:{token}") != user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    await redis.expire(f"token:{token}", 90 * 24 * 3600)
