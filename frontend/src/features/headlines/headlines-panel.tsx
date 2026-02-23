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
  if (items.length > 0) {
    const latest = items[0]
    void formatHeadlineTime(latest.time)
    const ageSec = getHeadlineAgeSec(latest.time)
    void getHeadlineFreshness(ageSec)
    void getFreshnessBadgeClass(getHeadlineFreshness(ageSec))
  }

  return (
    <PlaceholderCard
      title="Headlines"
      description="Latest headlines from /headlines and /headlines/stream"
    >
      {isLoading && <p>Loading headlines...</p>}
      {isError && <p>Failed to load headlines: {error?.message}</p>}
      {!isLoading && !isError && <p>{items.length} headline(s) loaded.</p>}
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
    </PlaceholderCard>
  )
}
