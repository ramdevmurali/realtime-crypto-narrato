import { useMemo, useState } from 'react'

import { AlertsRail } from '../features/dashboard/alerts-rail'
import { DashboardControls } from '../features/dashboard/dashboard-controls'
import { HeadlinesRail } from '../features/dashboard/headlines-rail'
import { KPIStack } from '../features/dashboard/kpi-stack'
import { LiveStatusRail } from '../features/dashboard/live-status-rail'
import { MarketOverviewChart } from '../features/dashboard/market-overview-chart'
import { SignalTrendChart } from '../features/dashboard/signal-trend-chart'
import { useDashboardData } from '../features/dashboard/use-dashboard-data'
import type { SymbolFilter, TimeWindow } from '../features/dashboard/types'
import type { Alert } from '../lib/types'

function getAlertKey(alert: Alert): string {
  return `${alert.time}|${alert.symbol}|${alert.window}|${alert.direction}`
}

export function DashboardPage() {
  const [symbolFilter, setSymbolFilter] = useState<SymbolFilter>('both')
  const [window, setWindow] = useState<TimeWindow>(30)
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null)

  const dashboard = useDashboardData({ symbolFilter, window })

  const selectedAlertKey = useMemo(
    () => (selectedAlert ? getAlertKey(selectedAlert) : null),
    [selectedAlert]
  )

  return (
    <main className="mx-auto min-h-screen w-full max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <header className="mb-4">
        <p className="text-xs font-medium uppercase tracking-[0.2em] text-slate-500">Realtime Crypto Narrato</p>
        <div className="mt-2 flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-900 sm:text-3xl">Market Intelligence Dashboard</h1>
            <p className="mt-1 text-sm text-slate-500">Live prices, signal shifts, headline freshness, and anomaly context.</p>
          </div>
        </div>
      </header>

      <DashboardControls
        symbolFilter={symbolFilter}
        onChangeSymbol={setSymbolFilter}
        window={window}
        onChangeWindow={setWindow}
      />

      <section className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-12">
        <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-8">
          <header className="mb-3 flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-slate-900">Market Overview</h2>
              <p className="text-xs text-slate-500">Price trajectory with anomaly overlays</p>
            </div>
            <p className="text-xs text-slate-500">window {window}m</p>
          </header>
          <MarketOverviewChart
            priceSeriesBySymbol={dashboard.priceSeriesBySymbol}
            alertOverlay={dashboard.alertOverlay}
            selectedSymbols={dashboard.selectedSymbols}
            window={window}
          />
        </article>

        <div className="lg:col-span-4">
          <LiveStatusRail
            healthState={dashboard.healthState}
            healthAgeSec={dashboard.healthAgeSec}
            alertsLive={dashboard.alerts.isLive}
            headlinesLive={dashboard.headlines.isLive}
            metricsFailedSymbols={dashboard.metrics.failedSymbols}
            lastPriceUpdate={dashboard.prices.lastUpdatedAt}
            lastMetricUpdate={dashboard.metrics.lastUpdatedAt}
          />
        </div>

        <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-8">
          <header className="mb-3">
            <h2 className="text-sm font-semibold text-slate-900">Signal Trends</h2>
            <p className="text-xs text-slate-500">EWMA return and volatility z-score behavior</p>
          </header>
          <SignalTrendChart
            metricSeriesBySymbol={dashboard.metricSeriesBySymbol}
            selectedSymbols={dashboard.selectedSymbols}
          />
        </article>

        <div className="lg:col-span-4">
          <KPIStack items={dashboard.marketStatus} />
        </div>

        <div className="order-1 lg:order-none lg:col-span-6">
          <AlertsRail
            items={dashboard.alerts.items}
            isLoading={dashboard.alerts.isLoading}
            isError={dashboard.alerts.isError}
            errorMessage={dashboard.alerts.errorMessage}
            selectedAlertKey={selectedAlertKey}
            onSelectAlert={setSelectedAlert}
          />
        </div>

        <div className="order-2 lg:order-none lg:col-span-6">
          <HeadlinesRail
            items={dashboard.headlines.items}
            isLoading={dashboard.headlines.isLoading}
            isError={dashboard.headlines.isError}
            errorMessage={dashboard.headlines.errorMessage}
          />
        </div>
      </section>
    </main>
  )
}
