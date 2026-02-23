import { PlaceholderCard } from '../../components/placeholder-card'
import type { Alert } from '../../lib/types'
import { useAlerts } from './use-alerts'

function getDirectionBadge(direction: string): { label: string; className: string } {
  const normalized = direction.trim().toLowerCase()
  if (normalized === 'up') {
    return { label: 'up', className: 'bg-emerald-100 text-emerald-700' }
  }
  if (normalized === 'down') {
    return { label: 'down', className: 'bg-rose-100 text-rose-700' }
  }
  if (normalized === 'flat') {
    return { label: 'flat', className: 'bg-slate-200 text-slate-700' }
  }
  return { label: 'unknown', className: 'bg-slate-200 text-slate-700' }
}

function getFreshnessBadge(headlineFresh?: boolean): { label: string; className: string } {
  if (headlineFresh === true) {
    return { label: 'fresh', className: 'bg-emerald-100 text-emerald-700' }
  }
  if (headlineFresh === false) {
    return { label: 'stale', className: 'bg-amber-100 text-amber-800' }
  }
  return { label: 'unknown', className: 'bg-slate-200 text-slate-700' }
}

function truncateSummary(text: string, maxChars = 140): string {
  const normalized = text.trim().replace(/\s+/g, ' ')
  if (normalized.length <= maxChars) {
    return normalized
  }
  return `${normalized.slice(0, maxChars).trimEnd()}…`
}

type AlertsPanelProps = {
  onSelectAlert: (alert: Alert) => void
  selectedAlertKey?: string | null
}

function getAlertRowKey(alert: Alert): string {
  return `${alert.time}|${alert.symbol}|${alert.window}|${alert.direction}`
}

export function AlertsPanel({ onSelectAlert, selectedAlertKey }: AlertsPanelProps) {
  const { items, isLoading, isError, error, isLive, lastEventAt } = useAlerts({ limit: 5, interval: 2 })

  return (
    <PlaceholderCard
      title="Alerts"
      description="Realtime anomaly alerts from /alerts and /alerts/stream"
    >
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
      {isLoading && <p>Loading alerts...</p>}
      {isError && <p>Failed to load alerts: {error?.message}</p>}
      {!isLoading && !isError && items.length === 0 && <p>No alerts yet.</p>}
      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.map((alert) => {
            const rowKey = getAlertRowKey(alert)
            const isSelected = selectedAlertKey === rowKey
            const direction = getDirectionBadge(alert.direction)
            const freshness = getFreshnessBadge(alert.headline_fresh)
            const ageText =
              typeof alert.headline_age_sec === 'number' ? `${alert.headline_age_sec}s` : 'n/a'
            const summaryText =
              typeof alert.summary === 'string' && alert.summary.trim().length > 0
                ? truncateSummary(alert.summary)
                : 'No summary yet'

            return (
              <li
                key={rowKey}
                aria-pressed={isSelected}
                className={`cursor-pointer rounded border p-2 transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-500 ${
                  isSelected
                    ? 'border-sky-500 bg-sky-50/60'
                    : 'border-slate-200 hover:border-slate-300'
                }`}
                onClick={() => onSelectAlert(alert)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    onSelectAlert(alert)
                  }
                }}
                role="button"
                tabIndex={0}
              >
                <p className="flex flex-wrap items-center gap-2">
                  <strong>{alert.symbol}</strong> · {alert.window}
                  <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${direction.className}`}>
                    {direction.label}
                  </span>
                  <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${freshness.className}`}>
                    {freshness.label}
                  </span>
                  <span className="text-xs text-slate-500">age: {ageText}</span>
                </p>
                <p>
                  return: {alert.return} · threshold: {alert.threshold}
                </p>
                <p>summary: {summaryText}</p>
                <p>time: {alert.time}</p>
              </li>
            )
          })}
        </ul>
      )}
    </PlaceholderCard>
  )
}
