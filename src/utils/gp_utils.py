import asyncio
import os
import subprocess
from contextlib import asynccontextmanager
from urllib.parse import quote_plus

import asyncio_atexit
import asyncpg
from asyncpg import Pool
from loguru import logger

from settings import settings

USERNAME = settings.GP_USERNAME
PASSWORD = quote_plus(settings.GP_PASSWORD)
HOST = settings.GP_HOST
PORT = settings.GP_PORT
DATABASE = settings.GP_DATABASE
SCHEMA = settings.GP_SCHEMA
KRB_PASSWORD = os.getenv('KRB_PASSWORD', '')

MIN_CONNECTIONS = settings.PG_MIN_CONNECTIONS
MAX_CONNECTIONS = settings.PG_MAX_CONNECTIONS
MAX_INACTIVE_LIFETIME = settings.GP_MAX_INACTIVE_LIFETIME

pool: Pool | None = None


async def run_query(query, params=()):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            async with get_pg_conn() as conn:
                return await conn.fetch(query, *params)
        except asyncpg.PostgresError as e:
            if attempt == max_retries - 1:
                logger.error(f'⚠️ Ошибка при выполнении запроса: {e}')
                raise

            # Check if error might be related to Kerberos
            if 'authentication' in str(e).lower() or 'GSSAPI' in str(e):
                logger.warning(
                    '⚠️ Обнаружена ошибка аутентификации, обновляем Kerberos ticket')
                await renew_kerberos_ticket()
                return None
            else:
                raise
    return None


@asynccontextmanager
async def get_pg_conn():
    global pool
    if pool is None:
        await __init_db()

    try:
        async with pool.acquire() as conn:
            yield conn
    except asyncpg.PostgresError as e:
        if 'authentication' in str(e).lower() or 'GSSAPI' in str(e):
            logger.warning(
                '⚠️ Ошибка соединения, возможно истек Kerberos ticket')
            await renew_kerberos_ticket()
            await __close_db()
            await __init_db()
            async with pool.acquire() as conn:
                yield conn
        else:
            raise


async def __init_db():
    global pool
    if pool is not None:
        return

    await check_or_renew_kerberos_ticket()

    pool = await asyncpg.create_pool(
        dsn=f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/'
            f'{DATABASE}?client_encoding=utf8',
        min_size=MIN_CONNECTIONS,
        max_size=MAX_CONNECTIONS,
        max_inactive_connection_lifetime=MAX_INACTIVE_LIFETIME)

    logger.info(
        f'ℹ️ Connection pool GP создан (size: {MIN_CONNECTIONS}-{MAX_CONNECTIONS})')
    __register_cleanup_handlers()


def __register_cleanup_handlers():
    asyncio_atexit.register(__close_db)


async def __close_db():
    global pool
    if pool:
        await pool.close()
        logger.info('ℹ️ Connection pool закрыт')
        pool = None


async def check_or_renew_kerberos_ticket():
    try:
        subprocess.run(['klist'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logger.warning('⚠️ Kerberos ticket not found or expired, renewing...')
        await renew_kerberos_ticket()


async def renew_kerberos_ticket():
    if not KRB_PASSWORD:
        raise RuntimeError('⚠️ Kerberos credentials not configured in env')

    try:
        proc = await asyncio.create_subprocess_exec(
            'kinit',
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate(input=f"{KRB_PASSWORD}\n".encode())


        if proc.returncode == 0:
            logger.info('✅ Kerberos ticket renewed successfully')
        else:
            logger.error(
                f'⚠️ Failed to renew Kerberos ticket: {proc.stderr}')
            raise RuntimeError(
                f'⚠️ Kerberos ticket renewal failed: {proc.stderr}')
    except subprocess.CalledProcessError as e:
        logger.error(f'⚠️ Error renewing Kerberos ticket: {e.stderr}')
        raise RuntimeError(f'⚠️ Kerberos ticket renewal failed: {e.stderr}')
    except Exception as e:
        logger.error(f'⚠️ Unexpected error during Kerberos renewal: {e}')
        raise
