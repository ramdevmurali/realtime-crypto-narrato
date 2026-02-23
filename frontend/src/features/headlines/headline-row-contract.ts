import type { Headline } from '../../lib/types'

export const HEADLINE_FRESH_SEC = 900

export const HEADLINE_ROW_FIELDS = [
  'time',
  'title',
  'source',
  'url',
  'sentiment',
] as const

export type HeadlineRowField = (typeof HEADLINE_ROW_FIELDS)[number]

export type HeadlineRowView = {
  time: string
  title: string
  source: string | null
  url: string | null
  sentiment: number | null
}

export function toHeadlineRowView(headline: Headline): HeadlineRowView {
  return {
    time: headline.time.trim(),
    title: headline.title.trim(),
    source: typeof headline.source === 'string' ? headline.source.trim() : null,
    url: typeof headline.url === 'string' ? headline.url.trim() : null,
    sentiment: typeof headline.sentiment === 'number' ? headline.sentiment : null,
  }
}

export function getHeadlineAgeSec(time: string, now: Date = new Date()): number | null {
  const parsedMs = Date.parse(time)
  if (Number.isNaN(parsedMs)) {
    return null
  }
  const ageMs = now.getTime() - parsedMs
  return Math.max(0, Math.floor(ageMs / 1000))
}

export function getHeadlineFreshness(ageSec: number | null): 'fresh' | 'stale' | 'unknown' {
  if (ageSec === null) {
    return 'unknown'
  }
  if (ageSec <= HEADLINE_FRESH_SEC) {
    return 'fresh'
  }
  return 'stale'
}
