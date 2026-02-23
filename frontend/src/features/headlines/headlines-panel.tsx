import { PlaceholderCard } from '../../components/placeholder-card'
import { useHeadlines } from './use-headlines'

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
      {!isLoading && !isError && <p>{items.length} headline(s) loaded.</p>}
      <p>Live: {isLive ? 'connected' : 'waiting for stream'}</p>
      <p>Last event: {lastEventAt ? lastEventAt.toLocaleTimeString() : 'n/a'}</p>
    </PlaceholderCard>
  )
}
