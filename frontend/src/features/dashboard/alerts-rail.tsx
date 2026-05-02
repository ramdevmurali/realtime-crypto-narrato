import type { Alert } from '../../lib/types'
import { getDirectionDotClass } from './ui-utils'

type AlertsRailProps = {
  items: Alert[]
  isLoading: boolean
  isError: boolean
  errorMessage: string | null
  selectedAlertKey: string | null
  onSelectAlert: (alert: Alert) => void
}

function alertKey(alert: Alert): string {
  return `${alert.time}|${alert.symbol}|${alert.window}|${alert.direction}`
}

function trimSummary(value: string | null): string {
  if (!value) {
    return 'summarizing...'
  }
  const text = value.trim().replace(/\s+/g, ' ')
  if (text.length <= 96) {
    return text
  }
  return `${text.slice(0, 96).trimEnd()}…`
}

export function AlertsRail({
  items,
  isLoading,
  isError,
  errorMessage,
  selectedAlertKey,
  onSelectAlert,
}: AlertsRailProps) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <header className="mb-3">
        <h2 className="text-sm font-semibold text-slate-900">Alerts</h2>
        <p className="text-xs text-slate-500">Realtime anomaly stream</p>
      </header>

      {isLoading && <p className="text-sm text-slate-500">Loading alerts...</p>}
      {isError && <p className="text-sm text-rose-700">Failed to load alerts: {errorMessage ?? 'unknown error'}</p>}
      {!isLoading && !isError && items.length === 0 && <p className="text-sm text-slate-500">No alerts in current window.</p>}

      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.slice(0, 8).map((alert) => {
            const key = alertKey(alert)
            const selected = selectedAlertKey === key
            const freshness = alert.headline_fresh === true ? 'fresh' : alert.headline_fresh === false ? 'stale' : 'unknown'
            return (
              <li
                key={key}
                className={`cursor-pointer rounded-xl border px-3 py-2 transition-colors ${
                  selected
                    ? 'border-slate-900 bg-slate-900 text-white'
                    : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300'
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
                <p className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide">
                  <span className={`h-2 w-2 rounded-full ${getDirectionDotClass(alert.direction)}`} />
                  {alert.symbol} · {alert.window} · {alert.direction}
                  <span className={`${selected ? 'text-slate-200' : 'text-slate-500'}`}>{freshness}</span>
                </p>
                <p className={`mt-1 text-xs ${selected ? 'text-slate-200' : 'text-slate-500'}`}>
                  return {(alert.return * 100).toFixed(2)}% · threshold {(alert.threshold * 100).toFixed(2)}%
                </p>
                <p className={`mt-1 text-sm ${selected ? 'text-white' : 'text-slate-700'}`}>{trimSummary(alert.summary)}</p>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
