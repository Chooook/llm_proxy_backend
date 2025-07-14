import subprocess
from datetime import datetime, timedelta

import asyncio_atexit
import asyncpg
from loguru import logger

from settings import settings

USERNAME = settings.GP_USERNAME
HOST = settings.GP_HOST
PORT = settings.GP_PORT
DATABASE = settings.GP_DATABASE

connection: asyncpg.Connection | None = None


async def run_query(query, params=()):
    if not check_kerberos_ticket():
        renew_kerberos_ticket()
    try:
        conn: asyncpg.Connection = await get_pg_conn()
        return await conn.fetch(query, *params)
    except asyncpg.PostgresError as e:
        logger.error(f'⚠️ GP query failed: {e}')
        raise


async def get_pg_conn():
    global connection
    if connection is None:
        await __init_db()
    return connection


async def __init_db():
    global connection
    if connection is not None:
        return
    if not check_kerberos_ticket():
        renew_kerberos_ticket()

    connection = await asyncpg.connect(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USERNAME)

    logger.info(f'ℹ️ Connection for GP created')
    asyncio_atexit.register(__close_db)


async def __close_db():
    global connection
    if connection:
        await connection.close()
        logger.info('ℹ️ GP connection closed')
        connection = None


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
        logger.error(f'⚠️ Kerberos ticket check error: {e}')
        return False


def renew_kerberos_ticket() -> bool:
    try:
        result = subprocess.run(
            ['kinit', '-R'],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f'⚠️ Kerberos ticket renew error: {result.stderr}')
            return False

        # check if ticket is valid after renew
        if not check_kerberos_ticket():
            logger.error('⚠️ Ticket is invalid after renew!')
            return False

        logger.info('ℹ️ Kerberos ticker renewed successfully')
        return True

    except Exception as e:
        logger.error(f'⚠️ Kerberos ticket renewal critical error: {e}')
        return False
