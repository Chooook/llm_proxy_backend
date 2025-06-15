from contextlib import asynccontextmanager

from redis import RedisError
from redis.asyncio import Redis
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt

from api.v1.router import router as v1_router
from utils.auth_utils import renew_token, store_new_token
from settings import settings

FRONTEND_URL = f'http://{settings.HOST}:{settings.FRONTEND_PORT}'


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        fastapi_app.state.redis = Redis(host=settings.HOST,
                                        port=settings.REDIS_PORT,
                                        db=settings.REDIS_DB,
                                        decode_responses=True)
    except RedisError as e:
        print(e, flush=True)
        raise
    yield
    await fastapi_app.state.redis.close()

app = FastAPI(debug=settings.DEBUG, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

@app.middleware('http')
async def refresh_jwt_token(request: Request, call_next):
    secret_key = settings.SECRET_KEY
    algorithm = settings.JWT_ALGORITHM
    redis = app.state.redis
    token = request.cookies.get('access_token')
    if token:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[algorithm])
            user_id = payload.get('sub')
            if user_id and await redis.get(f'token:{token}') == user_id:
                await redis.expire(f'token:{token}', 90 * 24 * 3600)
        except JWTError:
            pass

    response = await call_next(request)
    return response

app.include_router(v1_router)

@app.get('/')
async def root(request: Request, response: Response):
    redis = app.state.redis
    token = request.cookies.get('access_token')

    if not token:
        token = await store_new_token(redis)
        new_user = True
    else:
        try:
            await renew_token(token, redis)
            new_user = False
        except HTTPException:
            new_user = True
            token = await store_new_token(redis)

    response.set_cookie(
        key='access_token',
        value=token,
        httponly=True,
        secure=False,
        samesite='lax',
        max_age=90 * 24 * 3600,
    )
    if new_user:
        response.status_code = 303
        response.headers['location'] = '/'
