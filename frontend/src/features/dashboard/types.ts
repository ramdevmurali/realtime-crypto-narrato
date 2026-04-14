export type SymbolKey = 'btcusdt' | 'ethusdt'

export type SymbolFilter = SymbolKey | 'both'

export type DashboardHealthState = 'live' | 'degraded' | 'stale'

export type TimeWindow = 5 | 15 | 30

export type MetricSnapshot = {
  time: string
  symbol: SymbolKey
  return_1m: number | null
  return_z_ewma_1m: number | null
  vol_z_1m: number | null
  attention: number | null
}

export type MetricSeriesPoint = MetricSnapshot & {
  ts: number
}

export type PriceSeriesPoint = {
  ts: number
  time: string
  symbol: SymbolKey
  price: number
}

export type HeadlineFreshnessPoint = {
  ts: number
  time: string
  ageSec: number
}

export type AlertOverlayPoint = {
  ts: number
  time: string
  symbol: SymbolKey
  price: number
  direction: string
}

export type MarketStatusCard = {
  symbol: SymbolKey
  price: number | null
  return1m: number | null
  returnZ: number | null
  volZ: number | null
  attention: number | null
  freshnessSec: number | null
}
