"""create_price_tables

Revision ID: 82071463c28c
Revises: 
Create Date: 2026-05-12 22:32:59.305689

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '82071463c28c'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS price_ticks (
            id          SERIAL PRIMARY KEY,
            coin        VARCHAR(20)       NOT NULL,
            price_usd   DOUBLE PRECISION  NOT NULL,
            timestamp   TIMESTAMPTZ       NOT NULL,
            created_at  TIMESTAMPTZ       DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_price_ticks_coin_ts
        ON price_ticks (coin, timestamp DESC)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_price_ticks_ts
        ON price_ticks (timestamp DESC)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS price_alerts (
            id             SERIAL PRIMARY KEY,
            coin           VARCHAR(20)       NOT NULL,
            current_price  DOUBLE PRECISION  NOT NULL,
            avg_price      DOUBLE PRECISION  NOT NULL,
            change_pct     DOUBLE PRECISION  NOT NULL,
            direction      VARCHAR(4)        NOT NULL,
            timestamp      TIMESTAMPTZ       NOT NULL
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_price_alerts_coin_ts
        ON price_alerts (coin, timestamp DESC)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS price_alerts")
    op.execute("DROP TABLE IF EXISTS price_ticks")
