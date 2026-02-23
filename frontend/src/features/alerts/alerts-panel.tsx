import { PlaceholderCard } from '../../components/placeholder-card'
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

export function AlertsPanel() {
  const { items, isLoading, isError, error, isLive, lastEventAt } = useAlerts({ limit: 5, interval: 2 })

  return (
    <PlaceholderCard
      title="Alerts"
      description="Realtime anomaly alerts from /alerts and /alerts/stream"
    >
      {isLoading && <p>Loading alerts...</p>}
      {isError && <p>Failed to load alerts: {error?.message}</p>}
      {!isLoading && !isError && items.length === 0 && <p>No alerts yet.</p>}
      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.map((alert) => {
            const direction = getDirectionBadge(alert.direction)
            const freshness = getFreshnessBadge(alert.headline_fresh)
            const ageText =
              typeof alert.headline_age_sec === 'number' ? `${alert.headline_age_sec}s` : 'n/a'

            return (
              <li
                key={`${alert.time}|${alert.symbol}|${alert.window}|${alert.direction}`}
                className="rounded border border-slate-200 p-2"
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
                <p>summary: {alert.summary ?? 'No summary yet.'}</p>
                <p>time: {alert.time}</p>
              </li>
            )
          })}
        </ul>
      )}
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
    </PlaceholderCard>
  )
}
