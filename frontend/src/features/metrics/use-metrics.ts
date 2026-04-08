import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { getLatestMetrics } from '../../lib/api'
import type { LatestMetrics } from '../../lib/types'

type UseMetricsOptions = {
  symbols?: string[]
  pollMs?: number
}

type UseMetricsResult = {
  items: LatestMetrics[]
  isLoading: boolean
  isError: boolean
  error: Error | null
  refetch: () => void
  lastUpdatedAt: Date | null
  symbols: string[]
  failedSymbols: string[]
}

const DEFAULT_SYMBOLS = ['btcusdt', 'ethusdt']
const DEFAULT_POLL_MS = 5000

function parseTime(value: string): number {
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value
  }
  return new Error('Failed to load metrics')
}

function normalizeMetrics(items: LatestMetrics[]): LatestMetrics[] {
  return [...items].sort((a, b) => parseTime(b.time) - parseTime(a.time))
}

export function useMetrics(options: UseMetricsOptions = {}): UseMetricsResult {
  const pollMs = options.pollMs ?? DEFAULT_POLL_MS
  const symbols = useMemo(
    () =>
      (options.symbols ?? DEFAULT_SYMBOLS)
        .map((symbol) => symbol.trim().toLowerCase())
        .filter((symbol) => symbol.length > 0),
    [options.symbols]
  )

  const queryKey = useMemo(
    () => ['metrics', { symbols: symbols.join(',') }] as const,
    [symbols]
  )

  const query = useQuery<{ items: LatestMetrics[]; failedSymbols: string[] }, Error>({
    queryKey,
    queryFn: async () => {
      const settled = await Promise.allSettled(symbols.map((symbol) => getLatestMetrics(symbol)))
      const successful: LatestMetrics[] = []
      const failedSymbols: string[] = []

      for (const [index, result] of settled.entries()) {
        if (result.status === 'fulfilled') {
          successful.push(result.value)
          continue
        }
        failedSymbols.push(symbols[index])
      }

      if (successful.length === 0 && failedSymbols.length > 0) {
        const firstFailure = settled.find(
          (result): result is PromiseRejectedResult => result.status === 'rejected'
        )
        throw toError(firstFailure?.reason)
      }

      return {
        items: normalizeMetrics(successful),
        failedSymbols,
      }
    },
    refetchInterval: pollMs,
    refetchOnReconnect: true,
  })

  return {
    items: query.data?.items ?? [],
    isLoading: query.isLoading,
    isError: query.isError,
    error: query.error,
    refetch: () => {
      void query.refetch()
    },
    lastUpdatedAt: query.dataUpdatedAt > 0 ? new Date(query.dataUpdatedAt) : null,
    symbols,
    failedSymbols: query.data?.failedSymbols ?? [],
  }
}
