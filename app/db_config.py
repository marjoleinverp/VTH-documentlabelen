"""
Database Configuration - LEEF Datashare API (PostgreSQL only)

Features:
- Connection pooling (ThreadedConnectionPool)
- Context managers voor veilig connection beheer
- Health check functionaliteit

SECURITY: Alle credentials moeten via environment variables worden gezet.
Zie .env.example voor de vereiste variabelen.
"""
import os
import sys
import time
import atexit
from contextlib import contextmanager
from typing import Optional, Any, List, Tuple
from logging_config import get_logger

logger = get_logger(__name__)

DATABASE_TYPE = 'postgresql'

import psycopg2
import psycopg2.extras
from psycopg2 import pool

# PostgreSQL configuratie - GEEN defaults voor wachtwoord
_postgres_password = os.getenv('POSTGRES_PASSWORD')

if not _postgres_password:
    logger.error("KRITIEKE FOUT: POSTGRES_PASSWORD environment variable is niet gezet!")
    if os.getenv('ENVIRONMENT', 'development') == 'production':
        sys.exit(1)
    else:
        logger.warning("Draait in development mode zonder wachtwoord.")
        _postgres_password = ''

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'leef_datashare'),
    'user': os.getenv('POSTGRES_USER', 'leef_datashare'),
    'password': _postgres_password,
    'connect_timeout': 10,
    'options': '-c statement_timeout=30000'
}

# Pool configuratie
POOL_MIN_CONNECTIONS = int(os.getenv('DB_POOL_MIN', 2))
POOL_MAX_CONNECTIONS = int(os.getenv('DB_POOL_MAX', 20))

# Singleton connection pool
_connection_pool: Optional[pool.ThreadedConnectionPool] = None

PLACEHOLDER = '%s'


def _get_pool() -> pool.ThreadedConnectionPool:
    """Get of initialiseer de connection pool (singleton)"""
    global _connection_pool

    if _connection_pool is None:
        try:
            _connection_pool = pool.ThreadedConnectionPool(
                minconn=POOL_MIN_CONNECTIONS,
                maxconn=POOL_MAX_CONNECTIONS,
                **POSTGRES_CONFIG
            )
            logger.info(f"Database connection pool geinitialiseerd (min={POOL_MIN_CONNECTIONS}, max={POOL_MAX_CONNECTIONS})")
        except Exception as e:
            logger.error(f"Kon connection pool niet initialiseren: {e}")
            raise

    return _connection_pool


def get_connection():
    """Get een connection uit de pool."""
    return _get_pool().getconn()


def return_connection(conn):
    """Geef een connection terug aan de pool"""
    if conn and _connection_pool:
        _connection_pool.putconn(conn)


@contextmanager
def get_db_connection():
    """Context manager voor database connections."""
    conn = None
    try:
        conn = get_connection()
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_connection(conn)


def get_cursor(conn):
    """Get dict cursor voor PostgreSQL"""
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def close_pool():
    """Sluit alle connections in de pool (cleanup bij shutdown)"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Database connection pool gesloten")


# Registreer cleanup bij application shutdown
atexit.register(close_pool)


def execute_query(query: str, params: Optional[Tuple] = None, fetch: bool = True) -> Any:
    """Voer query uit en return resultaten."""
    with get_db_connection() as conn:
        cursor = get_cursor(conn)
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if fetch or query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            conn.rollback()
            raise


def check_database_health() -> dict:
    """Controleer database connectiviteit en gezondheid."""
    start_time = time.time()
    result = {
        "healthy": False,
        "type": "PostgreSQL",
        "latency_ms": 0
    }

    try:
        with get_db_connection() as conn:
            cursor = get_cursor(conn)
            cursor.execute("SELECT 1")
            cursor.fetchone()

            latency = (time.time() - start_time) * 1000
            result["healthy"] = True
            result["latency_ms"] = round(latency, 2)

            if _connection_pool:
                result["pool_min"] = POOL_MIN_CONNECTIONS
                result["pool_max"] = POOL_MAX_CONNECTIONS

    except Exception as e:
        result["error"] = str(e)
        result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
        logger.error(f"Database health check failed: {e}")

    return result
