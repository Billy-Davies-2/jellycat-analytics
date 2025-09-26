import os
import re
from functools import lru_cache
from typing import List, Dict, Tuple

_DEFAULT_MODEL = os.getenv("TRANSFORMERS_MODEL", "distilbert-base-uncased-finetuned-sst-2-english")


def _clean_text(text: str) -> str:
    text = re.sub(r"<.*?>", " ", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _select_device() -> int:
    """Return device index for transformers pipeline: 0 if GPU available, else -1.
    Override with TRANSFORMERS_USE_GPU={auto,1,0} (default: auto).
    """
    pref = os.getenv("TRANSFORMERS_USE_GPU", "auto").strip().lower()
    if pref in ("0", "false", "no", "cpu"):
        return -1
    try:
        import torch  # type: ignore
        if pref in ("1", "true", "yes", "gpu"):
            return 0 if torch.cuda.is_available() else -1
        # auto
        return 0 if torch.cuda.is_available() else -1
    except Exception:
        return -1


@lru_cache(maxsize=1)
def _get_pipeline():
    try:
        from transformers import pipeline  # type: ignore
    except Exception:
        return None
    device = _select_device()
    return pipeline("sentiment-analysis", model=_DEFAULT_MODEL, device=device)


def analyze_sentiment(text: str) -> str:
    """Return a simple POSITIVE/NEGATIVE/NEUTRAL label using transformers.

    Falls back to a keyword heuristic if transformers isn't available.
    """
    text = _clean_text(text)
    if not text:
        return "NEUTRAL"
    pipe = _get_pipeline()
    if pipe is not None:
        res = pipe(text)[0]
        return str(res.get("label", "NEUTRAL")).upper()
    # Fallback heuristic
    positive_words = [
        "soft", "cute", "adorable", "love", "great", "quality", "well-made",
        "luxurious", "amazing", "helpful", "perfect", "joyful", "durable",
    ]
    negative_words = [
        "expensive", "overpriced", "fake", "imitation", "delay", "disappointed",
        "poor", "issue", "cancelled", "bad", "waste", "floppy", "light",
    ]
    lower_text = text.lower()
    pos = sum(lower_text.count(w) for w in positive_words)
    neg = sum(lower_text.count(w) for w in negative_words)
    if pos > neg:
        return "POSITIVE"
    if neg > pos:
        return "NEGATIVE"
    return "NEUTRAL"


def analyze_sentiment_with_score(text: str) -> Tuple[str, float]:
    text = _clean_text(text)
    if not text:
        return ("NEUTRAL", 0.0)
    pipe = _get_pipeline()
    if pipe is not None:
        res = pipe(text)[0]
        return (str(res.get("label", "NEUTRAL")).upper(), float(res.get("score", 0.0)))
    # Fallback heuristic with no score
    return (analyze_sentiment(text), 0.0)


def batch_analyze_sentiment(texts: List[str]) -> List[Dict[str, float]]:
    if not texts:
        return []
    cleaned = [_clean_text(t) for t in texts]
    pipe = _get_pipeline()
    if pipe is not None:
        outputs = pipe(cleaned)
        return [{"label": str(o.get("label", "NEUTRAL")).upper(), "score": float(o.get("score", 0.0))} for o in outputs]
    # Fallback heuristic batch
    return [{"label": analyze_sentiment(t), "score": 0.0} for t in cleaned]
