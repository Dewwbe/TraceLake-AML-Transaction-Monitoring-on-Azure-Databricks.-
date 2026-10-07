"""Name normalisation and fuzzy scoring (pure Python, no Spark dependency)."""

from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz
from rapidfuzz.distance import JaroWinkler

TITLES = frozenset({"mr", "mrs", "ms", "miss", "dr", "sir", "prof"})


def strip_diacritics(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalise_name(name: str | None) -> str:
    """Lower-case, strip diacritics and punctuation, drop titles, collapse whitespace.

    Token order is preserved; order-insensitive comparison happens in ``score_names``.
    Non-Latin scripts are kept as-is (no transliteration), a documented limitation.
    """
    if not name:
        return ""
    text = strip_diacritics(name).casefold()
    text = re.sub(r"[^\w\s]|_", " ", text)
    tokens = [t for t in text.split() if t not in TITLES]
    return " ".join(tokens)


def score_names(a_norm: str | None, b_norm: str | None) -> float:
    """Similarity in [0, 100]: the better of Jaro-Winkler and token-sort ratio."""
    if not a_norm or not b_norm:
        return 0.0
    jw = JaroWinkler.normalized_similarity(a_norm, b_norm) * 100.0
    ts = fuzz.token_sort_ratio(a_norm, b_norm)
    return round(max(jw, ts), 2)


def band_for(score: float, review_min: float, strong_min: float) -> str | None:
    if score >= strong_min:
        return "STRONG"
    if score >= review_min:
        return "REVIEW"
    return None
