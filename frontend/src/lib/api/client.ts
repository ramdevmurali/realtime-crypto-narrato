import { getApiBaseUrl } from '../utils/env'
import type { Alert, Headline, LatestMetrics, ListQuery, PricePoint } from '../types/api'

type PriceQuery = {
  symbol: string
  limit?: number
}

function buildUrl(path: string, params?: Record<string, string | number | undefined>): string {
  const url = new URL(path, `${getApiBaseUrl()}/`)
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === '') {
        continue
      }
      url.searchParams.set(key, String(value))
    }
  }
  return url.toString()
}

async function requestJson<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const response = await fetch(buildUrl(path, params), {
    headers: {
      Accept: 'application/json',
    },
  })

  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${path}`)
  }

  return (await response.json()) as T
}

export function getAlerts(params: ListQuery = {}): Promise<Alert[]> {
  return requestJson<Alert[]>('alerts', params)
}

export function getHeadlines(params: ListQuery = {}): Promise<Headline[]> {
  return requestJson<Headline[]>('headlines', params)
}

export function getPrices(params: PriceQuery): Promise<PricePoint[]> {
  return requestJson<PricePoint[]>('prices', params)
}

export function getLatestMetrics(symbol: string): Promise<LatestMetrics> {
  return requestJson<LatestMetrics>('metrics/latest', { symbol })
}
