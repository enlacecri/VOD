from typing import Optional
from src.services.subtitles.base import TranscriptionProvider, TranslationProvider
from src.services.subtitles.static_provider import (
    StaticTranscriptionProvider,
    StaticTranslationProvider,
)

_current_transcription_provider: Optional[TranscriptionProvider] = None
_current_translation_provider: Optional[TranslationProvider] = None

def get_transcription_provider() -> TranscriptionProvider:
    global _current_transcription_provider
    if _current_transcription_provider is None:
        _current_transcription_provider = StaticTranscriptionProvider()
    return _current_transcription_provider

def set_transcription_provider(provider: Optional[TranscriptionProvider]) -> None:
    global _current_transcription_provider
    _current_transcription_provider = provider

def get_translation_provider() -> TranslationProvider:
    global _current_translation_provider
    if _current_translation_provider is None:
        _current_translation_provider = StaticTranslationProvider()
    return _current_translation_provider

def set_translation_provider(provider: Optional[TranslationProvider]) -> None:
    global _current_translation_provider
    _current_translation_provider = provider
