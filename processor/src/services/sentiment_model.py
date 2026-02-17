from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np

from ..config import settings
from ..utils import simple_sentiment


@dataclass(frozen=True)
class SentimentResult:
    score: float
    label: str | None
    confidence: float | None


_MODEL_CACHE = {"session": None, "tokenizer": None}


def _label_from_score(score: float) -> str:
    if score > settings.sentiment_pos_threshold:
        return "positive"
    if score < settings.sentiment_neg_threshold:
        return "negative"
    return "neutral"


def _load_tokenizer(model_dir: Path):
    if settings.sentiment_light_runtime:
        from tokenizers import Tokenizer

        tokenizer_path = model_dir / "tokenizer.json"
        if not tokenizer_path.exists():
            raise FileNotFoundError(f"missing tokenizer.json at {tokenizer_path}")
        return Tokenizer.from_file(str(tokenizer_path))

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


def _encode_light(tokenizer, texts: List[str], input_names: set[str]):
    encodings = tokenizer.encode_batch(texts)
    if not encodings:
        return {}
    max_len = max(len(enc.ids) for enc in encodings)
    max_len = min(max_len, 512)
    pad_id = tokenizer.token_to_id("[PAD]") or 0
    input_ids = np.full((len(encodings), max_len), pad_id, dtype=np.int64)
    attention_mask = np.zeros((len(encodings), max_len), dtype=np.int64)
    for idx, enc in enumerate(encodings):
        ids = enc.ids[:max_len]
        input_ids[idx, : len(ids)] = ids
        attention_mask[idx, : len(ids)] = 1
    inputs = {"input_ids": input_ids, "attention_mask": attention_mask}
    if "token_type_ids" in input_names:
        inputs["token_type_ids"] = np.zeros_like(input_ids, dtype=np.int64)
    return inputs


def _encode_full(tokenizer, texts: List[str], input_names: set[str]):
    enc = tokenizer(texts, padding=True, truncation=True, return_tensors="np")
    inputs = {k: v for k, v in enc.items() if k in input_names}
    if "token_type_ids" in input_names and "token_type_ids" not in inputs:
        inputs["token_type_ids"] = np.zeros_like(inputs["input_ids"], dtype=np.int64)
    return inputs


def predict(texts: List[str]) -> List[Tuple[float, str | None, float | None]]:
    if settings.sentiment_provider == "stub":
        return [(r.score, r.label, r.confidence) for r in _stub_predict(texts)]

    try:
        session, tokenizer = load_model()
        input_names = {inp.name for inp in session.get_inputs()}
        if settings.sentiment_light_runtime:
            inputs = _encode_light(tokenizer, texts, input_names)
        else:
            inputs = _encode_full(tokenizer, texts, input_names)
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
