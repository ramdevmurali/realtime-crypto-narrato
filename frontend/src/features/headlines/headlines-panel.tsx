import { PlaceholderCard } from '../../components/placeholder-card'
import { useHeadlines } from './use-headlines'

function formatHeadlineTime(time: string): string {
  const parsedMs = Date.parse(time)
  if (Number.isNaN(parsedMs)) {
    return time
  }
  return new Date(parsedMs).toLocaleTimeString([], { hour12: false })
}

function getHeadlineAgeSec(time: string): number | null {
  const parsedMs = Date.parse(time)
  if (Number.isNaN(parsedMs)) {
    return null
  }
  const ageMs = Date.now() - parsedMs
  return Math.max(0, Math.floor(ageMs / 1000))
}

function getHeadlineFreshness(ageSec: number | null): 'fresh' | 'stale' | 'unknown' {
  if (ageSec === null) {
    return 'unknown'
  }
  if (ageSec <= 900) {
    return 'fresh'
  }
  return 'stale'
}

function getFreshnessBadgeClass(status: 'fresh' | 'stale' | 'unknown'): string {
  if (status === 'fresh') {
    return 'bg-emerald-100 text-emerald-700'
  }
  if (status === 'stale') {
    return 'bg-amber-100 text-amber-800'
  }
  return 'bg-slate-200 text-slate-700'
}

export function HeadlinesPanel() {
  const { items, isLoading, isError, error, isLive, lastEventAt } = useHeadlines({
    limit: 5,
    interval: 2,
  })

  return (
    <PlaceholderCard
      title="Headlines"
      description="Latest headlines from /headlines and /headlines/stream"
    >
      {isLoading && <p>Loading headlines...</p>}
      {isError && <p>Failed to load headlines: {error?.message}</p>}
      {!isLoading && !isError && items.length === 0 && <p>No headlines yet.</p>}
      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.map((headline) => {
            const ageSec = getHeadlineAgeSec(headline.time)
            const freshness = getHeadlineFreshness(ageSec)
            const freshnessClass = getFreshnessBadgeClass(freshness)

            return (
              <li
                key={
                  headline.url ??
                  `${headline.time}|${headline.title}|${headline.source ?? ''}`
                }
                className="rounded border border-slate-200 p-2"
              >
                <p className="font-medium">{headline.title}</p>
                <p className="text-sm">
                  source: {headline.source ?? 'unknown'} · time:{' '}
                  {formatHeadlineTime(headline.time)}
                </p>
                <p className="flex flex-wrap items-center gap-2 text-sm">
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs font-medium ${freshnessClass}`}
                  >
                    {freshness}
                  </span>
                  <span className="text-xs text-slate-500">
                    age: {ageSec === null ? 'n/a' : `${ageSec}s`}
                  </span>
                  <span>
                    sentiment:{' '}
                    {typeof headline.sentiment === 'number'
                      ? headline.sentiment
                      : 'n/a'}
                  </span>
                </p>
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
