# Math & Methods

## 1. Methods (Short)
We process a stream of price ticks \((t, P_t)\) and maintain rolling windows of 1m, 5m, and 15m. From these windows we compute returns, volatility, z-scores, EWMA-smoothed z-scores, percentile bands, and apply fixed thresholds to trigger anomalies with a cooldown.

## 2. Data Model
- **Ticks**: ordered pairs \((t, P_t)\) where \(P_t\) is the latest price.
- **Windows**: rolling buffers covering the last 1m, 5m, and 15m of ticks.

## 3. Returns
For window \(\Delta \in \{1m, 5m, 15m\}\):
\[
r_t(\Delta) = \frac{P_t - P_{t-\Delta}}{P_{t-\Delta}}
\]
If there is no price at or before \(t-\Delta\), the return is undefined.

## 4. Volatility
Volatility is the standard deviation of intra-window returns:
\[
\sigma_t(\Delta) = \operatorname{std}\left(\{r_{t_i,t_{i-1}}\}_{t_i \ge t-\Delta}\right)
\]

## 5. Z-Scores
For a value \(x_t\) and series \(\{x_i\}\):
\[
z_t = \frac{x_t - \mu}{\sigma}, \quad \mu=\operatorname{mean}(\{x_i\})
\]
Undefined if \(\sigma=0\) or fewer than 2 points.

## 6. EWMA Smoothed Z
Smoothed z-score with \(\alpha \in (0,1]\):
\[
z_t^{\text{ewma}} = z_{t-1}^{\text{ewma}} + \alpha (z_t - z_{t-1}^{\text{ewma}})
\]
Values are capped to \([-6, 6]\). If \(z_t\) is undefined, \(z_t^{\text{ewma}}\) is undefined.

## 7. Percentile Bands
For intra-window returns \(\{r_i\}\), compute:
\[
\text{P05}=\operatorname{percentile}(\{r_i\}, 0.05), \quad \text{P95}=\operatorname{percentile}(\{r_i\}, 0.95)
\]
Undefined if fewer than 3 points.

## 8. Anomaly Rule
An anomaly is emitted when:
\[
|r_t(\Delta)| \ge \tau_\Delta
\]
where \(\tau_\Delta\) is the configured threshold for the window. A cooldown of 60 seconds suppresses repeated alerts for the same symbol/window.

## 9. Edge Cases
- **Insufficient data**: returns/volatility/z-scores/percentiles are undefined when the window has too few points.
- **Zero variance**: z-scores are undefined when \(\sigma=0\).

## 10. Design Rationale (Math-First)
We prioritize lightweight statistical signals over heavy ML in the core stream path. This keeps:
- **Latency low** for real-time detection on each tick.
- **Interpretability high**, so alerts can be explained by simple, auditable metrics.
- **Throughput stable** within an async pipeline (no large model inference in the hot path).

Heavier learned models are better isolated in separate services when needed, rather than embedded in the processor's critical loop.
