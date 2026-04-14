import type { MarketStatusCard } from './types'
import { formatNumber, formatPct, formatPrice, symbolLabel } from './ui-utils'

type KPIStackProps = {
  items: MarketStatusCard[]
}

export function KPIStack({ items }: KPIStackProps) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <header className="mb-3">
        <h2 className="text-sm font-semibold text-slate-900">KPI Stack</h2>
        <p className="text-xs text-slate-500">Live per-symbol signal snapshot</p>
      </header>

      <div className="space-y-2">
        {items.map((item) => (
          <article key={item.symbol} className="rounded-xl border border-slate-200 p-3">
            <div className="mb-1 flex items-center justify-between">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{symbolLabel(item.symbol)}</p>
              <p className="text-xs text-slate-500">freshness {item.freshnessSec === null ? 'n/a' : `${item.freshnessSec}s`}</p>
            </div>
            <p className="text-lg font-semibold text-slate-900">{formatPrice(item.price)}</p>
            <p className="mt-1 text-xs text-slate-600">ret 1m {formatPct(item.return1m)} · z {formatNumber(item.returnZ)} · vol z {formatNumber(item.volZ)}</p>
            <p className="text-xs text-slate-500">attention {formatNumber(item.attention, 2)}</p>
          </article>
        ))}
      </div>
    </section>
  )
}
