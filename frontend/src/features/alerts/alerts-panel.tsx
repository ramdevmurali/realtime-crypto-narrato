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
      {!isLoading && !isError && <p>{items.length} alert(s) loaded.</p>}
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
    </PlaceholderCard>
  )
}
