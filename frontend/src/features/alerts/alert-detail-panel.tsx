import type { Alert } from '../../lib/types'

type AlertDetailPanelProps = {
  alert: Alert | null
}

export function AlertDetailPanel({ alert }: AlertDetailPanelProps) {
  if (!alert) {
    return <p>Select an alert</p>
  }

  const freshness =
    alert.headline_fresh === true
      ? 'fresh'
      : alert.headline_fresh === false
        ? 'stale'
        : 'unknown'
  const ageText =
    typeof alert.headline_age_sec === 'number' ? `${alert.headline_age_sec}s` : 'n/a'
  const summaryText =
    typeof alert.summary === 'string' && alert.summary.trim().length > 0
      ? alert.summary
      : 'No summary yet'
  const headlineText =
    typeof alert.headline === 'string' && alert.headline.trim().length > 0
      ? alert.headline
      : null

  return (
    <section>
      <p>symbol: {alert.symbol}</p>
      <p>window: {alert.window}</p>
      <p>direction: {alert.direction}</p>
      <p>return: {alert.return}</p>
      <p>threshold: {alert.threshold}</p>
      <p>time: {alert.time}</p>
      <p>freshness: {freshness}</p>
      <p>age: {ageText}</p>
      <p>summary: {summaryText}</p>
      {headlineText && <p>headline: {headlineText}</p>}
      <p>
        sentiment: {typeof alert.sentiment === 'number' ? alert.sentiment : 'n/a'}
      </p>
    </section>
  )
}
