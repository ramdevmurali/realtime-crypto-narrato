# Math & Methods

## 1. Overview
We process a stream of price ticks `(t, P_t)` and maintain rolling windows defined by `WINDOW_LABELS`
(default `1m,5m,15m`). From these windows we compute returns, resampled volatility, z-scores,
EWMA‑smoothed z-scores, percentile bands, and apply fixed thresholds to trigger anomalies with a cooldown.

## 2. Data Model
- **Ticks**: ordered pairs `(t, P_t)` where `P_t` is the latest price.
- **Windows**: rolling buffers covering the last `Δ` for each configured window label.
- **Strict window**: only prices inside `[t-Δ, t]` are used (no spillover outside the window).

## 3. Returns (Strict Window)
For window `Δ`:

```
r_t(Δ) = (P_latest - P_ref) / P_ref
```

Where:
- `P_latest` is the latest price at or before `t`.
- `P_ref` is the first price at or after `t-Δ`.

The return is **undefined** if:
- There is no price at/after `t-Δ`, or no price at/before `t`.
- The first and latest points are the same timestamp.
- The gap to the reference exceeds the maximum allowed gap:

```
max_gap = min(Δ, Δ * WINDOW_MAX_GAP_FACTOR)
```

## 4. Volatility (Resampled)
We resample prices at a fixed cadence `S = VOL_RESAMPLE_SEC` inside the window:
- Sampling starts at the **first** price time `>= t-Δ` and proceeds in steps of `S` until `t`.
- Each sample uses the last observed price at or before the sample time.

Volatility is the population standard deviation of resampled simple returns:

```
σ_t(Δ) = std({ (P_i - P_{i-1}) / P_{i-1} })
```

Undefined if:
- Fewer than 3 **actual** points inside the window.
- Fewer than 3 resampled points.
- Any resample gap exceeds `max_gap`.

## 5. History‑Based Z‑Scores
For a value `x_t` and series `{x_i}`:

```
z_t = (x_t - μ) / σ,  μ = mean({x_i})
```

Where `{x_i}` is the **history of prior window values** (returns or vols) for that window.
Undefined if `σ = 0` or fewer than 2 points.

## 6. EWMA Smoothed Z
Smoothed z-score with `α in (0,1]`:

```
z_t^ewma = z_{t-1}^ewma + α (z_t - z_{t-1}^ewma)
```

Values are capped to `[-6, 6]`. If `z_t` is undefined, `z_t^ewma` is undefined.

## 7. Percentile Bands
For the **history of prior window returns** `{r_i}`, compute:

```
P05 = percentile({r_i}, 0.05)
P95 = percentile({r_i}, 0.95)
```

Undefined if fewer than 3 points. Percentiles use linear interpolation.

## 8. Attention Score

```
attention = max_Δ ( |r_t(Δ)| / τ_Δ )
```

Computed across windows with defined returns.

## 9. Anomaly Rule
An anomaly is emitted when:

```
|r_t(Δ)| >= τ_Δ
```

Where `τ_Δ` is the configured threshold for the window. A cooldown
(`ANOMALY_COOLDOWN_SEC`) suppresses repeated alerts for the same symbol/window.

## 10. Edge Cases
- **Insufficient data**: returns/volatility/z-scores/percentiles are undefined when the window has too few points.
- **Zero variance**: z-scores are undefined when `σ = 0`.
- **Strict window**: data outside `[t-Δ, t]` is ignored.
- **Late messages**: dropped if older than the last seen timestamp (modulo tolerance).

## 11. Design Rationale (Math-First)
We prioritize lightweight statistical signals over heavy ML in the core stream path. This keeps:
- **Latency low** for real-time detection on each tick.
- **Interpretability high**, so alerts can be explained by simple, auditable metrics.
- **Throughput stable** within an async pipeline (no large model inference in the hot path).

Heavier learned models are better isolated in separate services when needed, rather than embedded in the processor's critical loop.
