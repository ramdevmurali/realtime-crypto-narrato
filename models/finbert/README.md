# Local Sentiment Model Assets (FinBERT)

Place the ONNX model and tokenizer files in this directory. Example layout:

- `model.onnx`
- `tokenizer.json` (or `vocab.txt` + tokenizer config)
- `config.json` (if required by the tokenizer)

This repo does not include the model files. Download and place them here
manually, then ensure `SENTIMENT_MODEL_PATH` points to this directory (e.g.
`/models/finbert` inside containers).
