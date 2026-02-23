import { useState } from 'react'

import { PlaceholderCard } from '../components/placeholder-card'
import { AlertsPanel } from '../features/alerts/alerts-panel'
import { HeadlinesPanel } from '../features/headlines/headlines-panel'
import type { Alert } from '../lib/types'

export function DashboardPage() {
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null)

  return (
    <main className="mx-auto flex min-h-screen max-w-6xl flex-col gap-6 px-4 py-8 sm:px-6">
      <header>
        <p className="text-sm font-medium uppercase tracking-wide text-slate-500">Realtime Crypto Narrato</p>
        <h1 className="text-2xl font-bold text-slate-900">Operations Dashboard</h1>
      </header>

      <section className="grid gap-4 lg:grid-cols-2">
        <AlertsPanel selectedAlert={selectedAlert} onSelectAlert={setSelectedAlert} />
        <HeadlinesPanel />
        <PlaceholderCard
          title="Metrics"
          description="Latest computed metrics from /metrics/latest"
        >
          Metrics cards and trend widgets will be added next.
        </PlaceholderCard>
        <PlaceholderCard title="Prices" description="Recent prices from /prices">
          Price timeline and range controls will be added next.
        </PlaceholderCard>
      </section>
    </main>
  )
}
