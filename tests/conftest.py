import pytest
from alembic import command
from alembic.config import Config
from testcontainers.postgres import PostgresContainer

import crypto_pipeline.db.connection as _conn_module
from crypto_pipeline.config import settings


@pytest.fixture(scope="session")
def pg():
    with PostgresContainer("postgres:16") as container:
        # Point settings at the test container
        settings.postgres_host = container.get_container_host_ip()
        settings.postgres_port = int(container.get_exposed_port(5432))
        settings.postgres_user = container.username
        settings.postgres_password = container.password
        settings.postgres_db = container.dbname

        # Force pool to reinitialize with test credentials
        _conn_module._pool = None

        # Apply migrations
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")

        yield container

        if _conn_module._pool:
            _conn_module._pool.closeall()
        _conn_module._pool = None


@pytest.fixture
def clean_db(pg):
    from crypto_pipeline.db.connection import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE price_ticks, price_alerts RESTART IDENTITY")
    yield
