import { getApiBaseUrl } from '../utils/env'
import type { Alert, Headline, StreamPayload } from '../types/api'

type StreamOptions<T> = {
  limit?: number
  interval?: number
  onMessage: (payload: StreamPayload<T>) => void
  onError?: (error: Event) => void
}

function joinPath(base: string, path: string): string {
  const trimmedPath = path.replace(/^\/+/, '')
  if (!base) {
    return `/${trimmedPath}`
  }
  return `${base}/${trimmedPath}`
}

function subscribeStream<T>(path: string, options: StreamOptions<T>): () => void {
  const query = new URLSearchParams()
  if (options.limit !== undefined) {
    query.set('limit', String(options.limit))
  }
  if (options.interval !== undefined) {
    query.set('interval', String(options.interval))
  }

  const baseUrl = joinPath(getApiBaseUrl(), path)
  const streamUrl = query.toString() ? `${baseUrl}?${query.toString()}` : baseUrl
  const eventSource = new EventSource(streamUrl)

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
