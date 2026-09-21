"""add_check_constraints

Revision ID: 7e23839e86a2
Revises: ffb808d44632
Create Date: 2026-08-03 19:37:16.666338

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7e23839e86a2'
down_revision: Union[str, Sequence[str], None] = 'ffb808d44632'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_check_constraint('chk_asset_progress', 'assets', 'progress >= 0 AND progress <= 100')
    op.create_check_constraint('chk_asset_size', 'assets', 'size >= 0')
    op.create_check_constraint('chk_asset_duration', 'assets', 'duration_seconds >= 0')
    op.create_check_constraint('chk_asset_width', 'assets', 'source_width >= 0')
    op.create_check_constraint('chk_asset_height', 'assets', 'source_height >= 0')

    op.create_check_constraint('chk_rendition_width', 'renditions', 'width >= 0')
    op.create_check_constraint('chk_rendition_height', 'renditions', 'height >= 0')
    op.create_check_constraint('chk_rendition_duration', 'renditions', 'duration_seconds >= 0')
    op.create_check_constraint('chk_rendition_vbr', 'renditions', 'video_bitrate >= 0')
    op.create_check_constraint('chk_rendition_abr', 'renditions', 'audio_bitrate >= 0')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('chk_asset_progress', 'assets', type_='check')
    op.drop_constraint('chk_asset_size', 'assets', type_='check')
    op.drop_constraint('chk_asset_duration', 'assets', type_='check')
    op.drop_constraint('chk_asset_width', 'assets', type_='check')
    op.drop_constraint('chk_asset_height', 'assets', type_='check')

    op.drop_constraint('chk_rendition_width', 'renditions', type_='check')
    op.drop_constraint('chk_rendition_height', 'renditions', type_='check')
    op.drop_constraint('chk_rendition_duration', 'renditions', type_='check')
    op.drop_constraint('chk_rendition_vbr', 'renditions', type_='check')
    op.drop_constraint('chk_rendition_abr', 'renditions', type_='check')
