import { getHealthChipClass } from './ui-utils'
import type { DashboardHealthState } from './types'

type LiveStatusRailProps = {
  healthState: DashboardHealthState
  healthAgeSec: number | null
  alertsLive: boolean
  headlinesLive: boolean
  metricsFailedSymbols: string[]
  lastPriceUpdate: Date | null
  lastMetricUpdate: Date | null
}

function toClock(value: Date | null): string {
  if (!value) {
    return 'n/a'
  }
  return value.toLocaleTimeString([], { hour12: false })
}

function yesNo(value: boolean): string {
  return value ? 'yes' : 'no'
}

export function LiveStatusRail({
  healthState,
  healthAgeSec,
  alertsLive,
  headlinesLive,
  metricsFailedSymbols,
  lastPriceUpdate,
  lastMetricUpdate,
}: LiveStatusRailProps) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <header className="mb-3 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-slate-900">Live Status</h2>
        <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${getHealthChipClass(healthState)}`}>
          {healthState}
        </span>
      </header>

      <ul className="space-y-2 text-sm text-slate-600">
        <li className="flex items-center justify-between">
          <span>Health age</span>
          <strong className="font-medium text-slate-800">{healthAgeSec === null ? 'n/a' : `${healthAgeSec}s`}</strong>
        </li>
        <li className="flex items-center justify-between">
          <span>Alerts stream</span>
          <strong className="font-medium text-slate-800">{yesNo(alertsLive)}</strong>
        </li>
        <li className="flex items-center justify-between">
          <span>Headlines stream</span>
          <strong className="font-medium text-slate-800">{yesNo(headlinesLive)}</strong>
        </li>
        <li className="flex items-center justify-between">
          <span>Price refresh</span>
          <strong className="font-medium text-slate-800">{toClock(lastPriceUpdate)}</strong>
        </li>
        <li className="flex items-center justify-between">
          <span>Metrics refresh</span>
          <strong className="font-medium text-slate-800">{toClock(lastMetricUpdate)}</strong>
        </li>
        <li className="flex items-center justify-between">
          <span>Metric fallbacks</span>
          <strong className="font-medium text-slate-800">
            {metricsFailedSymbols.length === 0 ? 'none' : metricsFailedSymbols.join(', ')}
          </strong>
        </li>
      </ul>
    </section>
  )
}
