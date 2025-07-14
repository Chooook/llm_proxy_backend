import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
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

MIN_CONNECTIONS = settings.GP_MIN_CONNECTIONS
MAX_CONNECTIONS = settings.GP_MAX_CONNECTIONS
MAX_INACTIVE_LIFETIME = settings.GP_MAX_INACTIVE_LIFETIME

pool: Pool | None = None


async def run_query(query, params=()):
    if not check_kerberos_ticket():
        renew_kerberos_ticket()
    try:
        async with get_pg_conn() as conn:
            return await conn.fetch(query, *params)
    except asyncpg.PostgresError as e:
        logger.error(f'‼️ GP query failed: {e}')
        raise


@asynccontextmanager
async def get_pg_conn():
    global pool
    if pool is None:
        await __init_db()
    async with pool.acquire() as conn:
        yield conn


async def __init_db():
    global pool
    if pool is not None:
        return
    if not check_kerberos_ticket():
        renew_kerberos_ticket()

    pool = await asyncpg.create_pool(
        dsn=f'postgresql://{USERNAME}@{HOST}:{PORT}/{DATABASE}',
        min_size=MIN_CONNECTIONS,
        max_size=MAX_CONNECTIONS,
        max_inactive_connection_lifetime=MAX_INACTIVE_LIFETIME)

    logger.info(
        'ℹ️ Connection pool GP created '
        f'(size: {MIN_CONNECTIONS}-{MAX_CONNECTIONS})')
    asyncio_atexit.register(__close_db)


async def __close_db():
    global pool
    if pool:
        await pool.close()
        logger.info('ℹ️ Connection pool closed')
        pool = None


def check_kerberos_ticket() -> bool:
    try:
        klist_output = subprocess.run(
            ['klist'],
            capture_output=True,
            text=True
        ).stdout
        if 'krbtgt' not in klist_output:
            return False
        # parse ticket expiration datetime
        for line in klist_output.splitlines():
            if 'krbtgt' in line:
                parts = [p.strip() for p in line.split('  ') if p.strip()]
                if len(parts) >= 2:
                    expiry_str = parts[1]
                    expiry_time = datetime.strptime(
                        expiry_str, '%m/%d/%Y %H:%M:%S')
                    return datetime.now() + timedelta(minutes=5) < expiry_time
        return False
    except Exception as e:
        logger.error(f'‼️ Kerberos ticket check error: {e}')
        return False


def renew_kerberos_ticket() -> bool:
    try:
        result = subprocess.run(
            ['kinit', '-R'],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f'‼️ Kerberos ticket renew error: {result.stderr}')
            return False

        # check if ticket is valid after renew
        if not check_kerberos_ticket():
            logger.error('‼️ Ticket is invalid after renew!')
            return False

        logger.info('♻️ Kerberos ticker renewed successfully')
        return True

    except Exception as e:
        logger.error(f'‼️ Kerberos ticket renewal critical error: {e}')
        return False
