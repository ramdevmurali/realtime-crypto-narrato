import type { Headline } from '../../lib/types'
import { formatIsoClock } from './ui-utils'

type HeadlinesRailProps = {
  items: Headline[]
  isLoading: boolean
  isError: boolean
  errorMessage: string | null
}

function ageSec(value: string): number | null {
  const ts = Date.parse(value)
  if (Number.isNaN(ts)) {
    return null
  }
  return Math.max(0, Math.floor((Date.now() - ts) / 1000))
}

function freshnessClass(age: number | null): string {
  if (age === null) {
    return 'bg-slate-100 text-slate-600'
  }
  if (age <= 900) {
    return 'bg-emerald-50 text-emerald-700'
  }
  return 'bg-amber-50 text-amber-700'
}

function sentimentLabel(value: number | null): string {
  if (value === null) {
    return 'n/a'
  }
  return value.toFixed(3)
}

export function HeadlinesRail({ items, isLoading, isError, errorMessage }: HeadlinesRailProps) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <header className="mb-3">
        <h2 className="text-sm font-semibold text-slate-900">Headlines</h2>
        <p className="text-xs text-slate-500">Source, freshness and sentiment</p>
      </header>

      {isLoading && <p className="text-sm text-slate-500">Loading headlines...</p>}
      {isError && <p className="text-sm text-rose-700">Failed to load headlines: {errorMessage ?? 'unknown error'}</p>}
      {!isLoading && !isError && items.length === 0 && <p className="text-sm text-slate-500">No headlines in current window.</p>}

      {!isLoading && !isError && items.length > 0 && (
        <ul className="space-y-2">
          {items.slice(0, 8).map((headline) => {
            const age = ageSec(headline.time)
            return (
              <li key={`${headline.time}|${headline.url ?? headline.title}`} className="rounded-xl border border-slate-200 px-3 py-2">
                <p className="truncate text-sm font-medium text-slate-800">{headline.title}</p>
                <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                  <span>{headline.source ?? 'unknown'}</span>
                  <span>{formatIsoClock(headline.time)}</span>
                  <span className={`rounded-full px-2 py-0.5 ${freshnessClass(age)}`}>
                    {age === null ? 'age n/a' : `${age}s`}
                  </span>
                  <span className="rounded-full bg-slate-100 px-2 py-0.5 text-slate-600">
                    sentiment {sentimentLabel(headline.sentiment)}
                  </span>
                  {headline.url && (
                    <a className="text-slate-700 underline-offset-2 hover:underline" href={headline.url} rel="noreferrer noopener" target="_blank">
                      source
                    </a>
                  )}
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
