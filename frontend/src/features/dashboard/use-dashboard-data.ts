import { useEffect, useMemo, useState } from 'react'

import { useAlerts } from '../alerts/use-alerts'
import { useHeadlines } from '../headlines/use-headlines'
import { useMetrics } from '../metrics/use-metrics'
import { usePrices } from '../prices/use-prices'
import type { Alert, Headline, LatestMetrics, PricePoint } from '../../lib/types'
import type {
  AlertOverlayPoint,
  DashboardHealthState,
  HeadlineFreshnessPoint,
  MarketStatusCard,
  MetricSeriesPoint,
  MetricSnapshot,
  PriceSeriesPoint,
  SymbolFilter,
  SymbolKey,
  TimeWindow,
} from './types'

const SYMBOLS: SymbolKey[] = ['btcusdt', 'ethusdt']
const METRIC_HISTORY_MAX = 360
const ALERT_OVERLAY_TOLERANCE_MS = 180_000

type UseDashboardDataOptions = {
  symbolFilter: SymbolFilter
  window: TimeWindow
}

type UseDashboardDataResult = {
  symbolFilter: SymbolFilter
  window: TimeWindow
  selectedSymbols: SymbolKey[]
  healthState: DashboardHealthState
  healthAgeSec: number | null
  marketStatus: MarketStatusCard[]
  priceSeriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>
  metricSeriesBySymbol: Record<SymbolKey, MetricSeriesPoint[]>
  alertOverlay: AlertOverlayPoint[]
  headlineFreshness: HeadlineFreshnessPoint[]
  alerts: {
    items: Alert[]
    isLoading: boolean
    isError: boolean
    isLive: boolean
    lastEventAt: Date | null
    errorMessage: string | null
  }
  headlines: {
    items: Headline[]
    isLoading: boolean
    isError: boolean
    isLive: boolean
    lastEventAt: Date | null
    errorMessage: string | null
  }
  prices: {
    isLoading: boolean
    isError: boolean
    lastUpdatedAt: Date | null
    errorMessage: string | null
  }
  metrics: {
    isLoading: boolean
    isError: boolean
    lastUpdatedAt: Date | null
    failedSymbols: string[]
    errorMessage: string | null
  }
}

function parseTs(value: string): number {
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value !== 'number') {
    return null
  }
  return Number.isFinite(value) ? value : null
}

function asSymbol(value: string): SymbolKey | null {
  const normalized = value.trim().toLowerCase()
  if (normalized === 'btcusdt' || normalized === 'ethusdt') {
    return normalized
  }
  return null
}

function selectSymbols(filter: SymbolFilter): SymbolKey[] {
  if (filter === 'both') {
    return SYMBOLS
  }
  return [filter]
}

function applyWindowFilter<T extends { ts: number }>(points: T[], window: TimeWindow, nowMs: number): T[] {
  if (points.length === 0) {
    return points
  }
  const cutoff = nowMs - window * 60 * 1000
  const filtered = points.filter((point) => point.ts >= cutoff)
  if (filtered.length > 0) {
    return filtered
  }
  return points
}

function buildPriceSeries(
  items: PricePoint[],
  window: TimeWindow,
  nowMs: number
): Record<SymbolKey, PriceSeriesPoint[]> {
  const grouped: Record<SymbolKey, PriceSeriesPoint[]> = {
    btcusdt: [],
    ethusdt: [],
  }

  for (const item of items) {
    const symbol = asSymbol(item.symbol)
    if (!symbol) {
      continue
    }
    const ts = parseTs(item.time)
    if (ts <= 0) {
      continue
    }
    grouped[symbol].push({
      symbol,
      ts,
      time: item.time,
      price: item.price,
    })
  }

  for (const symbol of SYMBOLS) {
    grouped[symbol] = applyWindowFilter(
      grouped[symbol].sort((a, b) => a.ts - b.ts),
      window,
      nowMs
    )
  }

  return grouped
}

function toMetricSnapshot(item: LatestMetrics): MetricSnapshot | null {
  const symbol = asSymbol(item.symbol)
  if (!symbol) {
    return null
  }

  return {
    time: item.time,
    symbol,
    return_1m: toNumberOrNull(item.return_1m),
    return_z_ewma_1m: toNumberOrNull(item.return_z_ewma_1m),
    vol_z_1m: toNumberOrNull(item.vol_z_1m),
    attention: toNumberOrNull(item.attention),
  }
}

function findNearestPricePoint(series: PriceSeriesPoint[], targetTs: number): PriceSeriesPoint | null {
  if (series.length === 0) {
    return null
  }

  let low = 0
  let high = series.length - 1

  while (low < high) {
    const mid = Math.floor((low + high) / 2)
    if (series[mid].ts < targetTs) {
      low = mid + 1
    } else {
      high = mid
    }
  }

  const candidateA = series[Math.max(0, low - 1)]
  const candidateB = series[Math.min(series.length - 1, low)]
  const diffA = Math.abs(candidateA.ts - targetTs)
  const diffB = Math.abs(candidateB.ts - targetTs)

  return diffA <= diffB ? candidateA : candidateB
}

function buildAlertOverlay(
  alerts: Alert[],
  selectedSymbols: SymbolKey[],
  priceSeriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>
): AlertOverlayPoint[] {
  const symbolSet = new Set(selectedSymbols)
  const points: AlertOverlayPoint[] = []

  for (const alert of alerts) {
    const symbol = asSymbol(alert.symbol)
    if (!symbol || !symbolSet.has(symbol)) {
      continue
    }

    const targetTs = parseTs(alert.time)
    if (targetTs <= 0) {
      continue
    }

    const nearest = findNearestPricePoint(priceSeriesBySymbol[symbol], targetTs)
    if (!nearest) {
      continue
    }

    if (Math.abs(nearest.ts - targetTs) > ALERT_OVERLAY_TOLERANCE_MS) {
      continue
    }

    points.push({
      ts: nearest.ts,
      time: alert.time,
      symbol,
      price: nearest.price,
      direction: alert.direction,
    })
  }

  const deduped = new Map<string, AlertOverlayPoint>()
  for (const point of points) {
    const key = `${point.ts}|${point.symbol}|${point.direction}`
    if (!deduped.has(key)) {
      deduped.set(key, point)
    }
  }

  return Array.from(deduped.values()).sort((a, b) => a.ts - b.ts)
}

function buildHeadlineFreshness(
  headlines: Headline[],
  window: TimeWindow,
  nowMs: number
): HeadlineFreshnessPoint[] {
  const points: HeadlineFreshnessPoint[] = []

  for (const headline of headlines) {
    const ts = parseTs(headline.time)
    if (ts <= 0) {
      continue
    }
    points.push({
      ts,
      time: headline.time,
      ageSec: Math.max(0, Math.floor((nowMs - ts) / 1000)),
    })
  }

  return applyWindowFilter(points.sort((a, b) => a.ts - b.ts), window, nowMs)
}

function computeHealthState(
  args: {
    errors: number
    hasPartialMetrics: boolean
    streamsLive: boolean
    healthAgeSec: number | null
  }
): DashboardHealthState {
  const { errors, hasPartialMetrics, streamsLive, healthAgeSec } = args

  if (healthAgeSec === null || healthAgeSec > 300) {
    return 'stale'
  }
  if (errors === 0 && !hasPartialMetrics && streamsLive && healthAgeSec <= 90) {
    return 'live'
  }
  if (errors >= 2 || (!streamsLive && healthAgeSec > 180)) {
    return 'stale'
  }
  return 'degraded'
}

function latestMetricBySymbol(items: LatestMetrics[], history: Record<SymbolKey, MetricSeriesPoint[]>) {
  const map = new Map<SymbolKey, MetricSnapshot>()

  for (const item of items) {
    const snapshot = toMetricSnapshot(item)
    if (!snapshot) {
      continue
    }
    const existing = map.get(snapshot.symbol)
    if (!existing || parseTs(existing.time) < parseTs(snapshot.time)) {
      map.set(snapshot.symbol, snapshot)
    }
  }

  for (const symbol of SYMBOLS) {
    if (map.has(symbol)) {
      continue
    }
    const series = history[symbol]
    const latest = series[series.length - 1]
    if (latest) {
      map.set(symbol, latest)
    }
  }

  return map
}

export function useDashboardData(options: UseDashboardDataOptions): UseDashboardDataResult {
  const selectedSymbols = useMemo(() => selectSymbols(options.symbolFilter), [options.symbolFilter])
  const [nowMs, setNowMs] = useState(0)

  useEffect(() => {
    const tick = () => setNowMs(Date.now())
    tick()
    const timerId = window.setInterval(tick, 1000)
    return () => window.clearInterval(timerId)
  }, [])

  const prices = usePrices({
    symbols: SYMBOLS,
    limit: 2000,
    pollMs: 5000,
  })
  const metrics = useMetrics({
    symbols: SYMBOLS,
    pollMs: 5000,
  })
  const alerts = useAlerts({
    limit: 40,
    interval: 2,
  })
  const headlines = useHeadlines({
    limit: 40,
    interval: 2,
  })

  const [metricHistory, setMetricHistory] = useState<Record<SymbolKey, MetricSeriesPoint[]>>({
    btcusdt: [],
    ethusdt: [],
  })

  useEffect(() => {
    if (metrics.items.length === 0) {
      return
    }
    const timerId = window.setTimeout(() => {
      setMetricHistory((prev) => {
        const next: Record<SymbolKey, MetricSeriesPoint[]> = {
          btcusdt: [...prev.btcusdt],
          ethusdt: [...prev.ethusdt],
        }

        for (const item of metrics.items) {
          const snapshot = toMetricSnapshot(item)
          if (!snapshot) {
            continue
          }

          const ts = parseTs(snapshot.time)
          if (ts <= 0) {
            continue
          }

          const series = next[snapshot.symbol]
          const latest = series[series.length - 1]
          if (latest && latest.ts === ts) {
            continue
          }

          series.push({ ...snapshot, ts })
          if (series.length > METRIC_HISTORY_MAX) {
            next[snapshot.symbol] = series.slice(series.length - METRIC_HISTORY_MAX)
          }
        }

        return next
      })
    }, 0)

    return () => window.clearTimeout(timerId)
  }, [metrics.items])

  const priceSeriesBySymbol = useMemo(
    () => buildPriceSeries(prices.items, options.window, nowMs),
    [nowMs, options.window, prices.items]
  )

  const metricSeriesBySymbol = useMemo(() => {
    const filtered: Record<SymbolKey, MetricSeriesPoint[]> = {
      btcusdt: applyWindowFilter(metricHistory.btcusdt, options.window, nowMs),
      ethusdt: applyWindowFilter(metricHistory.ethusdt, options.window, nowMs),
    }
    return filtered
  }, [metricHistory, nowMs, options.window])

  const alertOverlay = useMemo(
    () => buildAlertOverlay(alerts.items, selectedSymbols, priceSeriesBySymbol),
    [alerts.items, priceSeriesBySymbol, selectedSymbols]
  )

  const headlineFreshness = useMemo(
    () => buildHeadlineFreshness(headlines.items, options.window, nowMs),
    [headlines.items, nowMs, options.window]
  )

  const latestMetricsMap = useMemo(
    () => latestMetricBySymbol(metrics.items, metricHistory),
    [metricHistory, metrics.items]
  )

  const marketStatus = useMemo<MarketStatusCard[]>(() => {
    return SYMBOLS.map((symbol) => {
      const latestPricePoint = priceSeriesBySymbol[symbol][priceSeriesBySymbol[symbol].length - 1] ?? null
      const latestMetric = latestMetricsMap.get(symbol) ?? null

      const freshnessCandidates = [latestPricePoint?.ts ?? 0, parseTs(latestMetric?.time ?? '')].filter(
        (value) => value > 0
      )
      const newestTs = freshnessCandidates.length > 0 ? Math.max(...freshnessCandidates) : 0

      return {
        symbol,
        price: latestPricePoint?.price ?? null,
        return1m: latestMetric?.return_1m ?? null,
        returnZ: latestMetric?.return_z_ewma_1m ?? null,
        volZ: latestMetric?.vol_z_1m ?? null,
        attention: latestMetric?.attention ?? null,
        freshnessSec: newestTs > 0 ? Math.max(0, Math.floor((nowMs - newestTs) / 1000)) : null,
      }
    })
  }, [latestMetricsMap, nowMs, priceSeriesBySymbol])

  const healthAgeSec = useMemo(() => {
    const candidates = [
      prices.lastUpdatedAt?.getTime() ?? 0,
      metrics.lastUpdatedAt?.getTime() ?? 0,
      alerts.lastEventAt?.getTime() ?? 0,
      headlines.lastEventAt?.getTime() ?? 0,
    ].filter((value) => value > 0)

    if (candidates.length === 0) {
      return null
    }

    const newest = Math.max(...candidates)
    return Math.max(0, Math.floor((nowMs - newest) / 1000))
  }, [alerts.lastEventAt, headlines.lastEventAt, metrics.lastUpdatedAt, nowMs, prices.lastUpdatedAt])

  const healthState = useMemo(() => {
    const errors = [prices.isError, metrics.isError, alerts.isError, headlines.isError].filter(Boolean).length
    return computeHealthState({
      errors,
      hasPartialMetrics: metrics.failedSymbols.length > 0,
      streamsLive: alerts.isLive && headlines.isLive,
      healthAgeSec,
    })
  }, [alerts.isError, alerts.isLive, headlines.isError, headlines.isLive, healthAgeSec, metrics.failedSymbols.length, metrics.isError, prices.isError])

  return {
    symbolFilter: options.symbolFilter,
    window: options.window,
    selectedSymbols,
    healthState,
    healthAgeSec,
    marketStatus,
    priceSeriesBySymbol,
    metricSeriesBySymbol,
    alertOverlay,
    headlineFreshness,
    alerts: {
      items: alerts.items,
      isLoading: alerts.isLoading,
      isError: alerts.isError,
      isLive: alerts.isLive,
      lastEventAt: alerts.lastEventAt,
      errorMessage: alerts.error?.message ?? null,
    },
    headlines: {
      items: headlines.items,
      isLoading: headlines.isLoading,
      isError: headlines.isError,
      isLive: headlines.isLive,
      lastEventAt: headlines.lastEventAt,
      errorMessage: headlines.error?.message ?? null,
    },
    prices: {
      isLoading: prices.isLoading,
      isError: prices.isError,
      lastUpdatedAt: prices.lastUpdatedAt,
      errorMessage: prices.error?.message ?? null,
    },
    metrics: {
      isLoading: metrics.isLoading,
      isError: metrics.isError,
      lastUpdatedAt: metrics.lastUpdatedAt,
      failedSymbols: metrics.failedSymbols,
      errorMessage: metrics.error?.message ?? null,
    },
  }
}
