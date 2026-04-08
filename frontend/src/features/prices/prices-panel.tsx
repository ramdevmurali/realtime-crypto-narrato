import { PlaceholderCard } from '../../components/placeholder-card'
import { usePrices } from './use-prices'

function formatPrice(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value)
}

function formatTime(value: string): string {
  const parsedMs = Date.parse(value)
  if (Number.isNaN(parsedMs)) {
    return value
  }
  return new Date(parsedMs).toLocaleTimeString([], { hour12: false })
}

export function PricesPanel() {
  const { items, isLoading, isError, error, lastUpdatedAt, symbols } = usePrices({
    limit: 10,
    pollMs: 5000,
  })

  return (
    <PlaceholderCard
      title="Prices"
      description="Recent prices from /prices (auto-refresh every 5s)"
    >
      <p>Symbols: {symbols.join(', ')}</p>
      <p>Last update: {lastUpdatedAt ? lastUpdatedAt.toLocaleTimeString() : 'n/a'}</p>
      {isLoading && <p>Loading prices...</p>}
      {isError && <p>Failed to load prices: {error?.message}</p>}
      {!isLoading && !isError && items.length === 0 && <p>No prices yet.</p>}
      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.map((item) => (
            <li
              key={`${item.time}|${item.symbol}|${item.price}`}
              className="rounded border border-slate-200 p-2"
            >
              <p className="font-medium">{item.symbol}</p>
              <p className="text-sm">price: {formatPrice(item.price)}</p>
              <p className="text-xs text-slate-500">time: {formatTime(item.time)}</p>
            </li>
          ))}
        </ul>
      )}
    </PlaceholderCard>
  )
}
