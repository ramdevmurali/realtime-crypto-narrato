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

type DirectionBadgeKey = 'up' | 'down' | 'flat' | 'unknown'
type FreshnessBadgeKey = 'true' | 'false' | 'unknown'

const DIRECTION_BADGE_CLASS: Record<DirectionBadgeKey, string> = {
  up: 'bg-emerald-100 text-emerald-700',
  down: 'bg-rose-100 text-rose-700',
  flat: 'bg-slate-100 text-slate-700',
  unknown: 'bg-slate-100 text-slate-700',
}

const FRESHNESS_BADGE_CLASS: Record<FreshnessBadgeKey, string> = {
  true: 'bg-emerald-100 text-emerald-700',
  false: 'bg-amber-100 text-amber-800',
  unknown: 'bg-slate-100 text-slate-700',
}

const KNOWN_DIRECTIONS = new Set<DirectionBadgeKey>(['up', 'down', 'flat'])

export function formatPercent(value: number): string {
  const asPercent = value * 100
  const sign = asPercent >= 0 ? '+' : ''
  return `${sign}${asPercent.toFixed(2)}%`
}

export function formatTime(value: string): string {
  const parsed = Date.parse(value)
  if (Number.isNaN(parsed)) {
    return value
  }
  return new Date(parsed).toLocaleTimeString([], { hour12: false })
}

export function getDirectionBadge(direction: string): string {
  const normalized = direction.trim().toLowerCase()
  if (KNOWN_DIRECTIONS.has(normalized as DirectionBadgeKey)) {
    return DIRECTION_BADGE_CLASS[normalized as DirectionBadgeKey]
  }
  return DIRECTION_BADGE_CLASS.unknown
}

export function getFreshnessBadge(headlineFresh: boolean | null): string {
  if (headlineFresh === true) {
    return FRESHNESS_BADGE_CLASS.true
  }
  if (headlineFresh === false) {
    return FRESHNESS_BADGE_CLASS.false
  }
  return FRESHNESS_BADGE_CLASS.unknown
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
