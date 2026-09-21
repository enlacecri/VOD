from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any

@dataclass
class TranscriptSegment:
    start: float
    end: float
    text: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start": round(self.start, 3),
            "end": round(self.end, 3),
            "text": self.text,
        }

@dataclass
class MasterTranscript:
    source_language: str
    text: str
    segments: List[TranscriptSegment] = field(default_factory=list)
    provider: str = "static"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_segments_json(self) -> List[Dict[str, Any]]:
        return [s.to_dict() for s in self.segments]

class TranscriptionProvider(ABC):
    @abstractmethod
    def transcribe(self, media_path: Path, language_hint: Optional[str] = None) -> MasterTranscript:
        """
        Generate a master transcript with timestamps and word/phrase segments.
        """
        pass

class TranslationProvider(ABC):
    @abstractmethod
    def translate(self, transcript: MasterTranscript, target_language: str) -> List[TranscriptSegment]:
        """
        Translate master transcript segments to a target language.
        """
        pass
