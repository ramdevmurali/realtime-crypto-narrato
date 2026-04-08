import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { getPrices } from '../../lib/api'
import type { PricePoint } from '../../lib/types'

type UsePricesOptions = {
  symbols?: string[]
  limit?: number
  pollMs?: number
}

type UsePricesResult = {
  items: PricePoint[]
  isLoading: boolean
  isError: boolean
  error: Error | null
  refetch: () => void
  lastUpdatedAt: Date | null
  symbols: string[]
}

const DEFAULT_SYMBOLS = ['btcusdt', 'ethusdt']
const DEFAULT_LIMIT = 20
const DEFAULT_POLL_MS = 5000

function parseTime(value: string): number {
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

function normalizePrices(items: PricePoint[], limit: number): PricePoint[] {
  const deduped = new Map<string, PricePoint>()

  for (const item of items) {
    const key = `${item.time}|${item.symbol}|${item.price}`
    if (!deduped.has(key)) {
      deduped.set(key, item)
    }
  }

  return Array.from(deduped.values())
    .sort((a, b) => parseTime(b.time) - parseTime(a.time))
    .slice(0, limit)
}

function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value
  }
  return new Error('Failed to load prices')
}

export function usePrices(options: UsePricesOptions = {}): UsePricesResult {
  const limit = options.limit ?? DEFAULT_LIMIT
  const pollMs = options.pollMs ?? DEFAULT_POLL_MS
  const symbols = useMemo(
    () =>
      (options.symbols ?? DEFAULT_SYMBOLS)
        .map((symbol) => symbol.trim().toLowerCase())
        .filter((symbol) => symbol.length > 0),
    [options.symbols]
  )

  const queryKey = useMemo(
    () => ['prices', { symbols: symbols.join(','), limit }] as const,
    [limit, symbols]
  )

  const query = useQuery<PricePoint[], Error>({
    queryKey,
    queryFn: async () => {
      const settled = await Promise.allSettled(
        symbols.map((symbol) => getPrices({ symbol, limit }))
      )

      const successful = settled.filter(
        (result): result is PromiseFulfilledResult<PricePoint[]> => result.status === 'fulfilled'
      )
      const failed = settled.filter(
        (result): result is PromiseRejectedResult => result.status === 'rejected'
      )

      if (successful.length === 0 && failed.length > 0) {
        throw toError(failed[0].reason)
      }

      const merged = successful.flatMap((result) => result.value)
      return normalizePrices(merged, limit)
    },
    refetchInterval: pollMs,
    refetchOnReconnect: true,
  })

  return {
    items: query.data ?? [],
    isLoading: query.isLoading,
    isError: query.isError,
    error: query.error,
    refetch: () => {
      void query.refetch()
    },
    lastUpdatedAt: query.dataUpdatedAt > 0 ? new Date(query.dataUpdatedAt) : null,
    symbols,
  }
}
