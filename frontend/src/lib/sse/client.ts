import { getApiBaseUrl } from '../utils/env'
import type { Alert, Headline, StreamPayload } from '../types/api'

type StreamOptions<T> = {
  limit?: number
  interval?: number
  onMessage: (payload: StreamPayload<T>) => void
  onError?: (error: Event) => void
}

function subscribeStream<T>(path: string, options: StreamOptions<T>): () => void {
  const url = new URL(path, `${getApiBaseUrl()}/`)
  if (options.limit !== undefined) {
    url.searchParams.set('limit', String(options.limit))
  }
  if (options.interval !== undefined) {
    url.searchParams.set('interval', String(options.interval))
  }

  const eventSource = new EventSource(url.toString())

  eventSource.onmessage = (event) => {
    const payload = JSON.parse(event.data) as StreamPayload<T>
    options.onMessage(payload)
  }

  eventSource.onerror = (event) => {
    options.onError?.(event)
  }

  return () => {
    eventSource.close()
  }
}

export function subscribeAlertsStream(options: StreamOptions<Alert>): () => void {
  return subscribeStream<Alert>('alerts/stream', options)
}

export function subscribeHeadlinesStream(options: StreamOptions<Headline>): () => void {
  return subscribeStream<Headline>('headlines/stream', options)
}
