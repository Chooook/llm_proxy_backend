from contextlib import asynccontextmanager

import aioredis
from fastapi import FastAPI, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt

from api.v1.router import router as v1_router
from utils.auth_utils import (
    create_guest_user, create_access_token, renew_token)
from settings import settings

FRONTEND_URL = f'http://{settings.HOST}:{settings.FRONTEND_PORT}'


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    host = settings.HOST
    port = settings.REDIS_PORT
    db = settings.REDIS_DB
    try:
        fastapi_app.state.redis = aioredis.Redis.from_url(
            f'redis://{host}:{port}/{db}', decode_responses=True)
    except aioredis.RedisError as e:
        print(e, flush=True)
        raise
    yield
    await fastapi_app.state.redis.close()

app = FastAPI(debug=settings.DEBUG, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def refresh_jwt_token(request: Request, call_next):
    secret_key = settings.SECRET_KEY
    algorithm = settings.JWT_ALGORITHM
    redis = app.state.redis
    token = request.cookies.get("access_token")
    if token:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[algorithm])
            user_id = payload.get("sub")
            if user_id and await redis.get(f"token:{token}") == user_id:
                await redis.expire(f"token:{token}", 90 * 24 * 3600)
        except JWTError:
            pass

    response = await call_next(request)
    return response

app.include_router(v1_router)

@app.get("/")
async def root(request: Request, response: Response):
    access_token_expire_seconds = settings.ACCESS_TOKEN_EXPIRE_DAYS * 24 * 3600
    redis = app.state.redis
    token = request.cookies.get("access_token")

    if not token:
        user_id = create_guest_user()
        token = create_access_token(data={"sub": user_id})
        await redis.setex(f"token:{token}", 90 * 24 * 3600, user_id)
    else:
        try:
            await renew_token(token, redis)

        except JWTError:
            user_id = create_guest_user()
            token = create_access_token(data={"sub": user_id})
            await redis.setex(
                f"token:{token}", access_token_expire_seconds, user_id)

    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=90 * 24 * 3600,
    )
