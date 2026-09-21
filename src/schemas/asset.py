import re
import uuid
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict, Field, field_validator

ENLACE_ID_REGEX = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

class AssetCreate(BaseModel):
    enlace_id: str
    source_uri: str
    
    @field_validator('enlace_id')
    @classmethod
    def validate_enlace_id(cls, v: str) -> str:
        if not ENLACE_ID_REGEX.match(v):
            raise ValueError("enlace_id must match ^[A-Za-z0-9_-]{1,128}$")
        return v
        
    @field_validator('source_uri')
    @classmethod
    def validate_source_uri(cls, v: str) -> str:
        # Prevención básica de path traversal. 
        # La validación completa física y contra INGEST_ROOT se hace en el core/rutas.
        if ".." in v or v.startswith("/"):
            raise ValueError("source_uri must be a relative path and cannot contain traversal elements (..)")
        return v

class AssetResponse(BaseModel):
    vod_uuid: uuid.UUID
    enlace_id: str
    status: str
    progress: int
    manifest_url: Optional[str] = None
    error_message: Optional[str] = None
    was_reused: bool = False
    
    model_config = ConfigDict(from_attributes=True)
