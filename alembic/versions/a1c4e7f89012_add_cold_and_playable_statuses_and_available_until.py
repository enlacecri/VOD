"""add cold and playable statuses and available_until

Revision ID: a1c4e7f89012
Revises: bd64ce9e0ae7
Create Date: 2026-09-21 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1c4e7f89012'
down_revision: Union[str, Sequence[str], None] = 'bd64ce9e0ae7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add COLD and PLAYABLE to PostgreSQL enum 'videostatus'
    # In PostgreSQL 12+, ALTER TYPE ... ADD VALUE IF NOT EXISTS is natively supported.
    op.execute("ALTER TYPE videostatus ADD VALUE IF NOT EXISTS 'COLD' BEFORE 'CREATED'")
    op.execute("ALTER TYPE videostatus ADD VALUE IF NOT EXISTS 'PLAYABLE' BEFORE 'VALIDATING'")

    # 2. Add available_until_seconds column
    op.add_column('assets', sa.Column('available_until_seconds', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('assets', 'available_until_seconds')
    # Note: PostgreSQL does not support ALTER TYPE ... DROP VALUE directly.
    # Unused enum values remain harmlessly in the type definition if rolled back.
