from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from ..config import settings
from ..utils import simple_sentiment


@dataclass(frozen=True)
class SentimentResult:
    score: float
    label: str | None
    confidence: float | None


_MODEL_CACHE = {"session": None, "tokenizer": None}


def _label_from_score(score: float) -> str:
    if score > 0.2:
        return "positive"
    if score < -0.2:
        return "negative"
    return "neutral"


def _load_tokenizer(model_dir: Path):
    from transformers import AutoTokenizer

    return AutoTokenizer.from_pretrained(str(model_dir))


def _load_onnx_session(model_path: Path):
    import onnxruntime as ort

    return ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])


def load_model():
    if _MODEL_CACHE["session"] is not None and _MODEL_CACHE["tokenizer"] is not None:
        return _MODEL_CACHE["session"], _MODEL_CACHE["tokenizer"]

    if not settings.sentiment_model_path:
        raise RuntimeError("sentiment_model_path not configured")

    model_dir = Path(settings.sentiment_model_path)
    model_path = model_dir / "model.onnx"
    if not model_path.exists():
        raise FileNotFoundError(f"missing model.onnx at {model_path}")

    tokenizer = _load_tokenizer(model_dir)
    session = _load_onnx_session(model_path)

    _MODEL_CACHE["session"] = session
    _MODEL_CACHE["tokenizer"] = tokenizer
    return session, tokenizer


def _stub_predict(texts: List[str]) -> List[SentimentResult]:
    results = []
    for text in texts:
        score = float(simple_sentiment(text))
        results.append(SentimentResult(score=score, label=None, confidence=None))
    return results


def predict(texts: List[str]) -> List[Tuple[float, str | None, float | None]]:
    if settings.sentiment_provider == "stub":
        return [(r.score, r.label, r.confidence) for r in _stub_predict(texts)]

    try:
        session, tokenizer = load_model()
        enc = tokenizer(
            texts,
            padding=True,
            truncation=True,
            return_tensors="np",
        )
        inputs = {k: v for k, v in enc.items()}
        outputs = session.run(None, inputs)
        logits = outputs[0]

        results = []
        for row in logits:
            exp = row - row.max()
            probs = exp / exp.sum()
            idx = int(probs.argmax())
            confidence = float(probs[idx])
            # assume model outputs [negative, neutral, positive]
            labels = ["negative", "neutral", "positive"]
            label = labels[idx] if idx < len(labels) else None
            score = float(probs[2] - probs[0]) if len(probs) >= 3 else float(simple_sentiment(""))
            results.append((score, label, confidence))
        return results
    except Exception:
        return [(r.score, r.label, r.confidence) for r in _stub_predict(texts)]
