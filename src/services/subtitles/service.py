import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.canonical import get_canonical_output_dir
from src.models.asset import Asset
from src.models.transcript import AssetTranscript, AssetSubtitleTrack
from src.services.subtitles.base import MasterTranscript
from src.services.subtitles.factory import get_transcription_provider, get_translation_provider
from src.services.subtitles.manifest_updater import (
    generate_vtt_content,
    generate_subtitle_playlist,
    update_master_manifest_with_subtitles,
)

logger = logging.getLogger(__name__)

class SubtitleProcessingError(Exception):
    pass

class SubtitleService:
    """
    Coordinates master transcription, language translation, WebVTT sidecar generation,
    database persistence, and atomic master manifest updates.
    """
    def __init__(self, languages: Optional[List[str]] = None):
        if languages:
            self.languages = languages
        else:
            langs_str = getattr(settings, "VOD_SUBTITLE_LANGUAGES", "es,en")
            self.languages = [lang.strip() for lang in langs_str.split(",") if lang.strip()]

    def process_subtitles(self, asset: Asset, db: Session, media_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Executes the subtitle workflow:
          1. Generates master transcript.
          2. Persists AssetTranscript record.
          3. Generates WebVTT and HLS index for each configured language.
          4. Persists AssetSubtitleTrack records.
          5. Atomically updates master playlist manifest.m3u8.
        """
        transcriber = get_transcription_provider()
        translator = get_translation_provider()

        # Locate source media file if provided or deduce from source_uri
        source_file = media_path
        if not source_file:
            cand = Path(settings.VOD_NEW_INGEST_ROOT).resolve() / (asset.source_uri or "")
            if cand.exists():
                source_file = cand
            else:
                cand_legacy = Path(settings.INGEST_ROOT).resolve() / (asset.source_uri or "")
                if cand_legacy.exists():
                    source_file = cand_legacy
                else:
                    # Fallback to dummy path for static testing
                    source_file = Path(asset.source_uri or "video.mp4")

        # 1. Transcribe master
        master: MasterTranscript = transcriber.transcribe(source_file, language_hint="es")

        # 2. Persist AssetTranscript
        transcript_record = db.query(AssetTranscript).filter(
            AssetTranscript.asset_id == asset.id
        ).first()

        if not transcript_record:
            transcript_record = AssetTranscript(
                asset_id=asset.id,
                source_language=master.source_language,
                transcript_text=master.text,
                segments_json=master.to_segments_json(),
                provider=master.provider,
                metadata_json=master.metadata,
            )
            db.add(transcript_record)
            db.commit()
            db.refresh(transcript_record)
        else:
            transcript_record.source_language = master.source_language
            transcript_record.transcript_text = master.text
            transcript_record.segments_json = master.to_segments_json()
            transcript_record.provider = master.provider
            db.commit()

        # Determine output directory
        out_dir = get_canonical_output_dir(settings.OUTPUT_ROOT, asset.vod_uuid, asset.enlace_id)
        subtitles_root = out_dir / "subtitles"
        subtitles_root.mkdir(parents=True, exist_ok=True)

        generated_tracks: List[str] = []
        max_duration = asset.duration_seconds or 12.0

        # 3. For each configured language, generate WebVTT & HLS playlist
        for lang in self.languages:
            segments = translator.translate(master, target_language=lang)
            vtt_text = generate_vtt_content(segments)
            lang_dir = subtitles_root / lang
            lang_dir.mkdir(parents=True, exist_ok=True)

            vtt_path = lang_dir / "subtitles.vtt"
            with open(vtt_path, "w", encoding="utf-8") as f:
                f.write(vtt_text)

            # Determine track duration
            seg_max = max((s.end for s in segments), default=max_duration)
            track_duration = max(seg_max, max_duration)

            sub_playlist = generate_subtitle_playlist("subtitles.vtt", track_duration)
            playlist_path = lang_dir / "index.m3u8"
            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write(sub_playlist)

            # Persist or update AssetSubtitleTrack in DB
            track_record = db.query(AssetSubtitleTrack).filter(
                AssetSubtitleTrack.asset_id == asset.id,
                AssetSubtitleTrack.language == lang,
            ).first()

            rel_vtt = f"subtitles/{lang}/subtitles.vtt"
            is_master = (lang == master.source_language)

            if not track_record:
                track_record = AssetSubtitleTrack(
                    asset_id=asset.id,
                    transcript_id=transcript_record.id,
                    language=lang,
                    vtt_path=rel_vtt,
                    is_master=is_master,
                )
                db.add(track_record)
            else:
                track_record.transcript_id = transcript_record.id
                track_record.vtt_path = rel_vtt
                track_record.is_master = is_master

            generated_tracks.append(lang)

        db.commit()

        # 4. Atomically update master manifest if it exists
        manifest_path = out_dir / "manifest.m3u8"
        if manifest_path.exists():
            update_master_manifest_with_subtitles(
                master_manifest_path=manifest_path,
                languages=self.languages,
                default_lang=master.source_language,
            )
            logger.info(f"Updated master manifest with subtitles at {manifest_path}")

        return {
            "transcript_id": str(transcript_record.id),
            "source_language": master.source_language,
            "languages": generated_tracks,
            "subtitles_dir": str(subtitles_root),
        }
