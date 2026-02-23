import type { Alert } from '../../lib/types'

type AlertDetailPanelProps = {
  alert: Alert | null
}

export function AlertDetailPanel({ alert }: AlertDetailPanelProps) {
  if (!alert) {
    return <p>Select an alert</p>
  }

  return (
    <section>
      <p>symbol: {alert.symbol}</p>
      <p>window: {alert.window}</p>
      <p>direction: {alert.direction}</p>
      <p>return: {alert.return}</p>
      <p>threshold: {alert.threshold}</p>
      <p>time: {alert.time}</p>
      <p>summary: {alert.summary ?? 'No summary yet'}</p>
    </section>
  )
}
