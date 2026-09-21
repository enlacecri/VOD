"""phase 5 ingest and workflow steps

Revision ID: d4f7a0b12345
Revises: c3e6f9a01234
Create Date: 2026-09-21 17:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd4f7a0b12345'
down_revision: Union[str, Sequence[str], None] = 'c3e6f9a01234'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # 1. Create ingest_items table
    op.create_table(
        'ingest_items',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('relative_path', sa.String(length=512), nullable=False),
        sa.Column('filename', sa.String(length=255), nullable=False),
        sa.Column('size_bytes', sa.BigInteger(), nullable=False),
        sa.Column('mtime', sa.Float(), nullable=False),
        sa.Column('source_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('status', sa.Enum('DETECTED', 'WAITING_STABLE', 'METADATA_PENDING', 'METADATA_READY', 'REGISTERED', 'DISPATCHED', 'CONFLICT', 'SOURCE_CHANGED', 'FAILED', name='ingeststatus'), nullable=False),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('metadata_snapshot', sa.JSON(), nullable=True),
        sa.Column('first_observed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_observed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('stable_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('registered_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_error', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['assets.id'], ondelete='SET NULL'),
    )
    op.create_index('ix_ingest_items_relative_path', 'ingest_items', ['relative_path'])
    op.create_index('ix_ingest_items_source_fingerprint', 'ingest_items', ['source_fingerprint'])
    op.create_index('ix_ingest_items_status', 'ingest_items', ['status'])
    op.create_index('ix_ingest_items_asset_id', 'ingest_items', ['asset_id'])

    # 2. Create asset_workflow_steps table
    op.create_table(
        'asset_workflow_steps',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('step_type', sa.Enum('AZURE_BACKUP', 'SUBTITLES', 'ENLACE_SYNC', name='workflowsteptype'), nullable=False),
        sa.Column('scope_key', sa.String(length=64), nullable=False, server_default='default'),
        sa.Column('status', sa.Enum('PENDING', 'QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', name='workflowstepstatus'), nullable=False),
        sa.Column('queue_name', sa.String(length=64), nullable=False),
        sa.Column('rq_job_id', sa.String(length=128), nullable=True),
        sa.Column('attempt_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error', sa.String(), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['assets.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('asset_id', 'step_type', 'scope_key', name='uq_asset_step_scope'),
    )
    op.create_index('ix_asset_workflow_steps_asset_id', 'asset_workflow_steps', ['asset_id'])
    op.create_index('ix_asset_workflow_steps_status', 'asset_workflow_steps', ['status'])

    # 3. Create asset_transcripts table
    op.create_table(
        'asset_transcripts',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_language', sa.String(length=16), nullable=False, server_default='es'),
        sa.Column('transcript_text', sa.Text(), nullable=False),
        sa.Column('segments_json', sa.JSON(), nullable=True),
        sa.Column('provider', sa.String(length=64), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['assets.id'], ondelete='CASCADE'),
    )
    op.create_index('ix_asset_transcripts_asset_id', 'asset_transcripts', ['asset_id'])

    # 4. Create asset_subtitle_tracks table
    op.create_table(
        'asset_subtitle_tracks',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('transcript_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('language', sa.String(length=16), nullable=False),
        sa.Column('vtt_path', sa.String(length=512), nullable=False),
        sa.Column('is_master', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['assets.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['transcript_id'], ['asset_transcripts.id'], ondelete='SET NULL'),
        sa.UniqueConstraint('asset_id', 'language', name='uq_asset_subtitle_language'),
    )
    op.create_index('ix_asset_subtitle_tracks_asset_id', 'asset_subtitle_tracks', ['asset_id'])
    op.create_index('ix_asset_subtitle_tracks_transcript_id', 'asset_subtitle_tracks', ['transcript_id'])


def downgrade() -> None:
    op.drop_table('asset_subtitle_tracks')
    op.drop_table('asset_transcripts')
    op.drop_table('asset_workflow_steps')
    op.drop_table('ingest_items')
    
    op.execute("DROP TYPE IF EXISTS workflowstepstatus")
    op.execute("DROP TYPE IF EXISTS workflowsteptype")
    op.execute("DROP TYPE IF EXISTS ingeststatus")
