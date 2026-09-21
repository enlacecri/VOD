"""add queue_name to jobs

Revision ID: b2d5f8e90123
Revises: a1c4e7f89012
Create Date: 2026-09-21 12:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2d5f8e90123'
down_revision: Union[str, Sequence[str], None] = 'a1c4e7f89012'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'jobs',
        sa.Column('queue_name', sa.String(length=64), server_default='vod_tasks', nullable=False)
    )


def downgrade() -> None:
    op.drop_column('jobs', 'queue_name')
