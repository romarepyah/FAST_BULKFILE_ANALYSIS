"""Database connection via psycopg v3 — uses SUPABASE_DB_URL for direct PostgreSQL access."""

import logging
import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


def get_conn():
    """Return a new psycopg connection with dict_row factory.

    Usage:
        with get_conn() as conn:
            rows = conn.execute("SELECT ...").fetchall()

    The connection auto-commits on successful exit and rolls back on exception.
    """
    from .config import Config
    dsn = Config.SUPABASE_DB_URL
    if not dsn:
        raise ValueError("SUPABASE_DB_URL must be set in .env")
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=False)


def run_sql_file(filepath: str):
    """Execute a raw SQL file against the database (used by run_migrations.py)."""
    with open(filepath) as f:
        sql = f.read()
    with get_conn() as conn:
        conn.execute(sql)
        conn.commit()
