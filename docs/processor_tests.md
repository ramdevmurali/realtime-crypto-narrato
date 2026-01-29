# Processor tests (for now just the windows suite)

- `test_windows.py`
  - What it is: checks the PriceWindow helper.
  - Tests:
    - prunes old points beyond ~16m.
    - get_return: builds a fake timeline with prices at -15m, -5m, -1m, and now, then checks that the 1m/5m/15m returns match the expected percentages; also proves it returns None when you only have the latest tick (no history).
    - get_vol: feeds a short series of prices; with fewer than 3 points it must return None, and with enough ticks it should spit out a positive stddev that matches the hand-calculated value (using pytest.approx).

## How to run
- `python3 -m pytest -q processor/tests`
- Async tests use pytest-asyncio.
