from pathlib import Path
from typing import Optional, List
from src.services.subtitles.base import (
    TranscriptionProvider,
    TranslationProvider,
    MasterTranscript,
    TranscriptSegment,
)

class StaticTranscriptionProvider(TranscriptionProvider):
    """
    Predictable static transcription provider for testing and development.
    Generates structured transcript segments with timestamps.
    """
    def transcribe(self, media_path: Path, language_hint: Optional[str] = None) -> MasterTranscript:
        src_lang = language_hint or "es"
        segments = [
            TranscriptSegment(start=0.0, end=4.0, text="Bienvenidos a Enlace Plus."),
            TranscriptSegment(start=4.0, end=8.0, text="Contenido audiovisual cristiano para toda la familia."),
            TranscriptSegment(start=8.0, end=12.0, text="Disfruta de nuestra programación continua."),
        ]
        full_text = " ".join(s.text for s in segments)
        return MasterTranscript(
            source_language=src_lang,
            text=full_text,
            segments=segments,
            provider="StaticTranscriptionProvider",
            metadata={"filename": media_path.name},
        )

class StaticTranslationProvider(TranslationProvider):
    """
    Static translation provider for testing.
    Translates known Spanish test phrases to English or appends target language code.
    """
    TRANSLATIONS = {
        "en": {
            "Bienvenidos a Enlace Plus.": "Welcome to Enlace Plus.",
            "Contenido audiovisual cristiano para toda la familia.": "Christian audiovisual content for the entire family.",
            "Disfruta de nuestra programación continua.": "Enjoy our continuous programming.",
        }
    }

    def translate(self, transcript: MasterTranscript, target_language: str) -> List[TranscriptSegment]:
        if target_language == transcript.source_language:
            return [TranscriptSegment(s.start, s.end, s.text) for s in transcript.segments]

        mapping = self.TRANSLATIONS.get(target_language, {})
        translated_segments = []
        for s in transcript.segments:
            translated_text = mapping.get(s.text, f"[{target_language.upper()}] {s.text}")
            translated_segments.append(
                TranscriptSegment(start=s.start, end=s.end, text=translated_text)
            )
        return translated_segments
