import type { Alert } from '../../lib/types'

export const ALERT_ROW_FIELDS = [
  'symbol',
  'window',
  'direction',
  'return',
  'threshold',
  'summary',
  'headline_fresh',
  'headline_age_sec',
  'time',
] as const

export type AlertRowField = (typeof ALERT_ROW_FIELDS)[number]

export type AlertRowView = {
  symbol: string
  window: string
  direction: string
  return: number
  threshold: number
  summary: string | null
  headline_fresh: boolean | null
  headline_age_sec: number | null
  time: string
}

export function toAlertRowView(alert: Alert): AlertRowView {
  return {
    symbol: alert.symbol,
    window: alert.window,
    direction: alert.direction,
    return: alert.return,
    threshold: alert.threshold,
    summary: alert.summary ?? null,
    headline_fresh:
      typeof alert.headline_fresh === 'boolean' ? alert.headline_fresh : null,
    headline_age_sec:
      typeof alert.headline_age_sec === 'number' ? alert.headline_age_sec : null,
    time: alert.time,
  }
}
