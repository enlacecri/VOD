import uuid
from pathlib import Path
from typing import Optional, Union

def normalize_vod_uuid(vod_uuid: Union[uuid.UUID, str]) -> str:
    """Returns upper-cased string representation of VOD UUID."""
    return str(vod_uuid).upper()

def build_canonical_manifest_path(vod_uuid: Union[uuid.UUID, str], enlace_id: str) -> str:
    """
    Returns the internal / filesystem relative path of the master manifest.
    Example: EnlacePlus/_definst_/amlst:217CBED8-667B-4A9B-B000-D3003160B0C5/PREDI-VICTO89/manifest.m3u8
    """
    uuid_upper = normalize_vod_uuid(vod_uuid)
    return f"EnlacePlus/_definst_/amlst:{uuid_upper}/{enlace_id}/manifest.m3u8"

def build_canonical_manifest_url(
    vod_uuid: Union[uuid.UUID, str], 
    enlace_id: str, 
    base_url: Optional[str] = None
) -> str:
    """
    Returns the stable public canonical URL for asset playback.
    Example: /EnlacePlus/*definst*/amlst:217CBED8-667B-4A9B-B000-D3003160B0C5/PREDI-VICTO89/manifest.m3u8
    Or with base_url: https://videocdn.enlace.plus/EnlacePlus/*definst*/amlst:.../manifest.m3u8
    """
    uuid_upper = normalize_vod_uuid(vod_uuid)
    url_path = f"/EnlacePlus/*definst*/amlst:{uuid_upper}/{enlace_id}/manifest.m3u8"
    if base_url:
        return f"{base_url.rstrip('/')}{url_path}"
    return url_path

def get_canonical_output_dir(
    output_root: Union[Path, str], 
    vod_uuid: Union[uuid.UUID, str], 
    enlace_id: str
) -> Path:
    """
    Returns the absolute Path on disk where the canonical HLS output resides.
    Example: /storage/output/EnlacePlus/_definst_/amlst:217CBED8-667B-4A9B-B000-D3003160B0C5/PREDI-VICTO89
    """
    uuid_upper = normalize_vod_uuid(vod_uuid)
    root = Path(output_root).resolve()
    return root / "EnlacePlus" / "_definst_" / f"amlst:{uuid_upper}" / str(enlace_id)
