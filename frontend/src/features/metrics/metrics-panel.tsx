import { PlaceholderCard } from '../../components/placeholder-card'
import type { LatestMetrics } from '../../lib/types'
import { useMetrics } from './use-metrics'

const PREFERRED_FIELDS = [
  'return_1m',
  'return_z_ewma_1m',
  'vol_1m',
  'vol_z_1m',
  'attention',
  'return_5m',
]
const RESERVED_FIELDS = new Set(['time', 'symbol'])

function formatMetricTime(value: unknown): string {
  if (typeof value !== 'string') {
    return 'n/a'
  }
  const parsedMs = Date.parse(value)
  if (Number.isNaN(parsedMs)) {
    return value
  }
  return new Date(parsedMs).toLocaleTimeString([], { hour12: false })
}

function formatMetricValue(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toFixed(4) : 'n/a'
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false'
  }
  if (typeof value === 'string') {
    return value
  }
  return 'n/a'
}

function isDisplayableMetricValue(value: unknown): boolean {
  return (
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'string'
  )
}

function pickMetricFields(items: LatestMetrics[]): string[] {
  const preferred = PREFERRED_FIELDS.filter((field) =>
    items.some((item) => isDisplayableMetricValue(item[field]))
  )

  const fallback = Array.from(
    new Set(
      items.flatMap((item) =>
        Object.keys(item).filter(
          (key) => !RESERVED_FIELDS.has(key) && isDisplayableMetricValue(item[key])
        )
      )
    )
  )

  return Array.from(new Set([...preferred, ...fallback])).slice(0, 4)
}

export function MetricsPanel() {
  const { items, isLoading, isError, error, lastUpdatedAt, symbols, failedSymbols } = useMetrics({
    pollMs: 5000,
  })
  const metricFields = pickMetricFields(items)

  return (
    <PlaceholderCard
      title="Metrics"
      description="Latest computed metrics from /metrics/latest (auto-refresh every 5s)"
    >
      <p>Symbols: {symbols.join(', ')}</p>
      <p>Last update: {lastUpdatedAt ? lastUpdatedAt.toLocaleTimeString() : 'n/a'}</p>
      {failedSymbols.length > 0 && (
        <p className="text-amber-700">
          Partial data: failed for {failedSymbols.join(', ')}.
        </p>
      )}
      {isLoading && <p>Loading metrics...</p>}
      {isError && <p>Failed to load metrics: {error?.message}</p>}
      {!isLoading && !isError && items.length === 0 && <p>No metrics yet.</p>}
      {!isLoading && !isError && items.length > 0 && metricFields.length === 0 && (
        <p>No displayable metric fields found.</p>
      )}
      {!isLoading && !isError && items.length > 0 && metricFields.length > 0 && (
        <ul className="space-y-2">
          {items.map((item) => (
            <li
              key={`${item.symbol}|${item.time}`}
              className="rounded border border-slate-200 p-2"
            >
              <p className="font-medium">{item.symbol}</p>
              <p className="text-xs text-slate-500">time: {formatMetricTime(item.time)}</p>
              <p className="text-sm">
                {metricFields.map((field) => `${field}: ${formatMetricValue(item[field])}`).join(' · ')}
              </p>
            </li>
          ))}
        </ul>
      )}
    </PlaceholderCard>
  )
}
