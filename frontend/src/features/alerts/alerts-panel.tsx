import { PlaceholderCard } from '../../components/placeholder-card'
import { useAlerts } from './use-alerts'

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
          {items.map((alert) => (
            <li
              key={`${alert.time}|${alert.symbol}|${alert.window}|${alert.direction}`}
              className="rounded border border-slate-200 p-2"
            >
              <p>
                <strong>{alert.symbol}</strong> · {alert.window} · {alert.direction}
              </p>
              <p>
                return: {alert.return} · threshold: {alert.threshold}
              </p>
              <p>summary: {alert.summary ?? 'No summary yet.'}</p>
              <p>time: {alert.time}</p>
            </li>
          ))}
        </ul>
      )}
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
    </PlaceholderCard>
  )
}
