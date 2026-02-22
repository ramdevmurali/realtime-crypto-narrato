export type ListQuery = {
  limit?: number
  since?: string
}

export type Alert = {
  time: string
  symbol: string
  window: string
  direction: string
  return: number
  threshold: number
  summary: string | null
  headline: string | null
  sentiment: number | null
  headline_age_sec?: number
  headline_fresh?: boolean
}

export type Headline = {
  time: string
  title: string
  url: string | null
  source: string | null
  sentiment: number | null
}

export type PricePoint = {
  time: string
  symbol: string
  price: number
}

export type LatestMetrics = {
  time: string
  symbol: string
  [key: string]: unknown
}

export type StreamPayload<T> = {
  items: T[]
  count: number
}
