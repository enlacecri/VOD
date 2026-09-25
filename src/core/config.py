from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Optional, Any

class Settings(BaseSettings):
    VOD_POSTGRES_PORT: int = 5434
    VOD_REDIS_PORT: int = 6380
    VOD_NGINX_PORT: int = 8085
    VOD_API_PORT: int = 8005

    DATABASE_URL: str = "postgresql+psycopg://vod_user:vod_password@localhost:5434/vod_db"
    REDIS_URL: str = "redis://localhost:6380/0"
    
    INGEST_ROOT: str = "./storage/input"
    STAGING_ROOT: str = "./storage/staging"
    OUTPUT_ROOT: str = "./storage/output"
    PROCESSED_ROOT: str = "./storage/processed"
    LOG_ROOT: str = "./storage/logs"
    PROGRESSIVE_ROOT: str = "./storage/progressive"
    PROGRESSIVE_MIN_SEGMENTS: int = 5
    
    HLS_TIME: int = 6
    
    # Phase 2 Settings
    FFPROBE_PATH: str = "ffprobe"
    FFPROBE_TIMEOUT_SECONDS: int = 15
    FFMPEG_TIMEOUT_SECONDS: int = 14400  # 4 hours
    FFMPEG_GRACEFUL_STOP_SECONDS: int = 10
    MAX_FILE_SIZE_BYTES: int = 50 * 1024 * 1024 * 1024  # 50 GB
    PROBE_JSON_LIMIT_BYTES: int = 1024 * 1024           # 1 MB
    HASH_CHUNK_SIZE: int = 64 * 1024                    # 64 KB
    
    RQ_QUEUE_NAME: str = "vod_tasks"
    RQ_JOB_TIMEOUT_SECONDS: int = 3600
    MAX_PROBE_ATTEMPTS: int = 3
    MAX_TRANSCODE_ATTEMPTS: int = 3
    ERROR_MESSAGE_MAX_LENGTH: int = 1000
    STAGING_ORPHAN_AGE_SECONDS: int = 86400
    MIN_FREE_DISK_BYTES: int = 1024 * 1024 * 1024  # 1 GiB
    ADMIN_API_KEY: str = ""
    
    # Phase 3 Settings
    FFMPEG_PATH: str = "ffmpeg"
    FFMPEG_ENCODER_CHECK_TIMEOUT_SECONDS: int = 10
    HLS_GOP_SECONDS: int = 2
    VIDEO_ENCODER: str = "h264_videotoolbox"
    VIDEO_ENCODER_FALLBACK: str = "libx264"
    TRANSCODE_CONCURRENCY: int = 1
    
    CDN_BASE_URL: str = "https://videocdn.enlace.plus"
    HLS_PLAYBACK_BASE_URL: str = "http://localhost:8085"
    HLS_VALIDATION_TIMEOUT_SECONDS: int = 300
    
    # Tolerances and Intervals
    ASPECT_RATIO_TOLERANCE: float = 0.05
    BANDWIDTH_OVERHEAD_MAX_RATIO: float = 1.3
    HEARTBEAT_INTERVAL_SECONDS: int = 5
    PROGRESS_PERSIST_INTERVAL_SECONDS: int = 5

    # Phase 4 Settings (Dynamic TOP N and Batch Prewarming)
    VOD_PREWARM_TOP_N: int = 3000
    VOD_PREWARM_ENQUEUE_LIMIT: int = 50
    VOD_BATCH_MAX_QUEUE_DEPTH: int = 50

    # Phase 5 Settings (Automatic New Video Pipeline)
    VOD_NEW_INGEST_ROOT: str = "./storage/new_input"
    VOD_INGEST_STABLE_SECONDS: int = 30
    VOD_SUBTITLE_LANGUAGES: str = "es,en"
    VOD_BACKUP_STORAGE_DIR: str = "./storage/backup"

    # Phase 6 Settings (Smart Transcode Orchestrator)
    TRANSCODE_ORCHESTRATOR_ENABLED: bool = True
    TRANSCODE_NODE_NAME: str = ""
    TRANSCODE_MAX_CONCURRENT: int = 1
    TRANSCODE_RESERVED_PRIORITY_SLOTS: int = 0
    TRANSCODE_CPU_START_THRESHOLD: float = 75.0
    TRANSCODE_CPU_HARD_THRESHOLD: float = 90.0
    TRANSCODE_MIN_AVAILABLE_MEMORY_MB: int = 2048
    TRANSCODE_MIN_FREE_DISK_GB: int = 5
    TRANSCODE_RESOURCE_CHECK_ENABLED: bool = True
    TRANSCODE_SLOT_TTL_SECONDS: int = 60
    TRANSCODE_HEARTBEAT_INTERVAL_SECONDS: int = 20
    TRANSCODE_RETRY_MIN_SECONDS: int = 5
    TRANSCODE_RETRY_MAX_SECONDS: int = 30
    TRANSCODE_JOB_TIMEOUT_SECONDS: Optional[int] = None

    @field_validator("TRANSCODE_JOB_TIMEOUT_SECONDS", mode="before")
    @classmethod
    def parse_optional_timeout(cls, v: Any) -> Optional[int]:
        if v == "" or v is None:
            return None
        return int(v)

    @property
    def resolved_node_name(self) -> str:
        if self.TRANSCODE_NODE_NAME and self.TRANSCODE_NODE_NAME.strip():
            return self.TRANSCODE_NODE_NAME.strip()
        import socket
        return socket.gethostname()

    @property
    def normalized_cdn_url(self) -> str:
        return self.CDN_BASE_URL.rstrip("/")

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()
