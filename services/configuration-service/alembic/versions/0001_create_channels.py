"""create channels and seed the two supported channels

Seeded by a data migration rather than a startup script: deterministic, runs
before Uvicorn, and covered by the alembic integration test. See spec 3.9.
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

channels = sa.table("channels", sa.column("name", sa.String), sa.column("enabled", sa.Boolean))


def upgrade() -> None:
    op.create_table(
        "channels",
        sa.Column("name", sa.String(length=50), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
    )
    op.bulk_insert(
        channels, [{"name": "email", "enabled": True}, {"name": "telegram", "enabled": True}]
    )


def downgrade() -> None:
    op.drop_table("channels")
