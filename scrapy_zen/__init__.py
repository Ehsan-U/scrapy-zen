__version__ = "0.4.1"

from w3lib.url import canonicalize_url


def normalize_url(value: str) -> str:
    if "http://" in value or "https://" in value:
        return canonicalize_url(value)
    return value
