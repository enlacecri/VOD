"""phase 4 ranking snapshots and prewarm runs

Revision ID: c3e6f9a01234
Revises: b2d5f8e90123
Create Date: 2026-09-21 16:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c3e6f9a01234'
down_revision: Union[str, Sequence[str], None] = 'b2d5f8e90123'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'ranking_snapshots',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('source', sa.String(length=64), nullable=False),
        sa.Column('period_start', sa.DateTime(timezone=True), nullable=True),
        sa.Column('period_end', sa.DateTime(timezone=True), nullable=True),
        sa.Column('generated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('item_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('extra_metadata', sa.JSON(), nullable=True),
    )

    op.create_table(
        'ranking_snapshot_items',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('snapshot_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('enlace_id', sa.String(length=128), nullable=False),
        sa.Column('rank', sa.Integer(), nullable=False),
        sa.Column('score', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['snapshot_id'], ['ranking_snapshots.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('snapshot_id', 'enlace_id', name='uq_snapshot_enlace_id'),
    )
    op.create_index('ix_ranking_snapshot_items_snapshot_id', 'ranking_snapshot_items', ['snapshot_id'])
    op.create_index('ix_ranking_snapshot_items_enlace_id', 'ranking_snapshot_items', ['enlace_id'])

    op.create_table(
        'prewarm_runs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('ranking_snapshot_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('target_top_n', sa.Integer(), nullable=False),
        sa.Column('enqueue_limit', sa.Integer(), nullable=False),
        sa.Column('batch_queue_depth_before', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('batch_queue_depth_after', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('examined', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('matched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('already_ready', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('active', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('cold_candidates', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('failed_skipped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('missing_catalog', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('enqueued', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='RUNNING'),
        sa.Column('error', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['ranking_snapshot_id'], ['ranking_snapshots.id'], ondelete='SET NULL'),
    )
    op.create_index('ix_prewarm_runs_ranking_snapshot_id', 'prewarm_runs', ['ranking_snapshot_id'])


def downgrade() -> None:
    op.drop_index('ix_prewarm_runs_ranking_snapshot_id', table_name='prewarm_runs')
    op.drop_table('prewarm_runs')
    op.drop_index('ix_ranking_snapshot_items_enlace_id', table_name='ranking_snapshot_items')
    op.drop_index('ix_ranking_snapshot_items_snapshot_id', table_name='ranking_snapshot_items')
    op.drop_table('ranking_snapshot_items')
    op.drop_table('ranking_snapshots')
