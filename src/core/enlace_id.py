import re
from typing import Optional, Tuple, Any

# The single canonical regular expression for enlace_id across the entire VOD platform.
# Allowed: A-Z, a-z, 0-9, underscore (_), hyphen (-)
# Length: 1 to 128 characters
# Prohibited: dots (.), spaces/whitespace, slashes, @, or any other characters.
# No trimming, no replacement, no case modification.
ENLACE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,128}$"
ENLACE_ID_REGEX = re.compile(ENLACE_ID_PATTERN)


class InvalidEnlaceIdError(ValueError):
    """Raised by require_valid_enlace_id when an enlace_id violates canonical rules."""
    def __init__(self, enlace_id: Any, reason: str):
        super().__init__(f"Invalid enlace_id '{enlace_id}': {reason}")
        self.enlace_id = enlace_id
        self.reason = reason


def validate_enlace_id(value: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validates that value strictly conforms to the canonical enlace_id format:
    ^[A-Za-z0-9_-]{1,128}$

    Strict rules:
      - Must be a non-empty string.
      - Length must be between 1 and 128 characters inclusive.
      - Must NOT contain whitespace (leading, trailing, or internal).
      - Must NOT contain dots (.).
      - Must NOT contain symbols (@, /, #, etc.).
      - No normalization: no trim, no case change, no character replacement.

    Returns:
      (True, None) if valid.
      (False, reason_code) if invalid, where reason_code is one of:
        - "empty": if value is None, empty, or not a string.
        - "too_long": if length > 128.
        - "whitespace": if value contains any whitespace.
        - "contains_dots": if value contains dot (.) characters.
        - "unsupported_characters": if value contains any characters outside [A-Za-z0-9_-].
    """
    if value is None or not isinstance(value, str) or len(value) == 0:
        return False, "empty"
    if len(value) > 128:
        return False, "too_long"
    if any(c.isspace() for c in value):
        return False, "whitespace"
    if "." in value:
        return False, "contains_dots"
    if not ENLACE_ID_REGEX.fullmatch(value):
        return False, "unsupported_characters"
    return True, None


def require_valid_enlace_id(value: Optional[str]) -> str:
    """
    Enforces that value is a valid enlace_id according to canonical rules.
    Returns the exact valid string unchanged.
    Raises InvalidEnlaceIdError with the specific reason code if invalid.
    """
    is_valid, reason = validate_enlace_id(value)
    if not is_valid:
        raise InvalidEnlaceIdError(value, reason or "unsupported_characters")
    return value
