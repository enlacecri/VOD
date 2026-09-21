import enum

class VideoStatus(str, enum.Enum):
    COLD = "cold"
    CREATED = "created"
    PROBING = "probing"
    QUEUED = "queued"
    PROCESSING = "processing"
    PLAYABLE = "playable"
    VALIDATING = "validating"
    READY = "ready"
    FAILED = "failed"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            val_lower = value.lower()
            for member in cls:
                if member.value == val_lower or member.name.lower() == val_lower:
                    return member
        return None

class EventType(str, enum.Enum):
    TRANSITION = "TRANSITION"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    PROMOTED = "PROMOTED"
    ERROR = "ERROR"

class JobType(str, enum.Enum):
    PROBE = "probe"
    TRANSCODE = "transcode"
    VALIDATE = "validate"
    
class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    
# Phase 3 Error Codes (constants)
E_FFMPEG_NOT_FOUND = "E_FFMPEG_NOT_FOUND"
E_FFMPEG_TIMEOUT = "E_FFMPEG_TIMEOUT"
E_ENCODER_UNAVAILABLE = "E_ENCODER_UNAVAILABLE"
E_TRANSCODE_FAILED = "E_TRANSCODE_FAILED"
E_PROGRESS_INVALID = "E_PROGRESS_INVALID"
E_SOURCE_HASH_MISMATCH = "E_SOURCE_HASH_MISMATCH"
E_HLS_VALIDATION_FAILED = "E_HLS_VALIDATION_FAILED"
E_OUTPUT_EXISTS = "E_OUTPUT_EXISTS"
E_ATOMIC_PROMOTION_FAILED = "E_ATOMIC_PROMOTION_FAILED"

# Phase 5 Enums
class IngestStatus(str, enum.Enum):
    DETECTED = "DETECTED"
    WAITING_STABLE = "WAITING_STABLE"
    METADATA_PENDING = "METADATA_PENDING"
    METADATA_READY = "METADATA_READY"
    REGISTERED = "REGISTERED"
    DISPATCHED = "DISPATCHED"
    CONFLICT = "CONFLICT"
    SOURCE_CHANGED = "SOURCE_CHANGED"
    FAILED = "FAILED"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            val_upper = value.upper()
            for member in cls:
                if member.value == val_upper or member.name == val_upper:
                    return member
        return None

class WorkflowStepType(str, enum.Enum):
    AZURE_BACKUP = "AZURE_BACKUP"
    SUBTITLES = "SUBTITLES"
    ENLACE_SYNC = "ENLACE_SYNC"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            val_upper = value.upper()
            for member in cls:
                if member.value == val_upper or member.name == val_upper:
                    return member
        return None

class WorkflowStepStatus(str, enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            val_upper = value.upper()
            for member in cls:
                if member.value == val_upper or member.name == val_upper:
                    return member
        return None


