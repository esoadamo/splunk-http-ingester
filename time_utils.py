import re
from dateutil import parser
from datetime import datetime, timezone
from typing import List, Optional


def extract_timestamp(log_line: str) -> Optional[datetime]:
    # List of regex patterns for different timestamp formats
    patterns: List[str] = [
        r'\b(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\b',  # ISO 8601
        r'\b(\d{2}[-/]\d{2}[-/]\d{4}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\b',  # Date with time
        r'\b(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:\s+\d{4})?)\b',  # Month name, day, time
        r'\b(\d{1,2}\s+\w{3}\s+(?:\d{4}\s+)?\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?)\b',  # Day, month name, time
        r'\b(\w{3}\s+\d{1,2}(?:\s+\d{4})?\s+\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?)\b'  # Month name, day, year, time
    ]

    for pattern in patterns:
        match: Optional[re.Match] = re.search(pattern, log_line)
        if match:
            timestamp_str: str = match.group(1)
            try:
                timestamp: datetime = parser.parse(timestamp_str, fuzzy=True)

                # If year is not present, assume current year
                if timestamp.year == 1900:
                    current_year: int = datetime.now().year
                    timestamp = timestamp.replace(year=current_year)

                timestamp = timestamp.astimezone(timezone.utc)

                return timestamp
            except ValueError:
                pass

    return None


def extract_timestamp_iso(log_line) -> Optional[str]:
    timestamp = extract_timestamp(log_line)
    return timestamp.replace(tzinfo=timezone.utc).isoformat() if timestamp else None


def extract_timestamp_unix(log_line: str) -> Optional[str]:
    timestamp = extract_timestamp(log_line)
    return str(timestamp.timestamp()) if timestamp else None
