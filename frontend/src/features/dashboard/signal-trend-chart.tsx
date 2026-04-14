import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { MetricSeriesPoint, SymbolKey } from './types'
import { formatClock, symbolLabel } from './ui-utils'

type SignalTrendChartProps = {
  metricSeriesBySymbol: Record<SymbolKey, MetricSeriesPoint[]>
  selectedSymbols: SymbolKey[]
}

type SeriesField = 'return_z_ewma_1m' | 'vol_z_1m'

type Frame = {
  ts: number
  btcusdt: number | null
  ethusdt: number | null
}

function buildFrames(
  dataBySymbol: Record<SymbolKey, MetricSeriesPoint[]>,
  field: SeriesField
): Frame[] {
  const map = new Map<number, Frame>()

  for (const symbol of ['btcusdt', 'ethusdt'] as const) {
    for (const point of dataBySymbol[symbol]) {
      const value = point[field]
      const existing = map.get(point.ts)
      if (existing) {
        existing[symbol] = value
      } else {
        map.set(point.ts, {
          ts: point.ts,
          btcusdt: symbol === 'btcusdt' ? value : null,
          ethusdt: symbol === 'ethusdt' ? value : null,
        })
      }
    }
  }

  return Array.from(map.values()).sort((a, b) => a.ts - b.ts)
}

function TrendTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ name?: string; value?: number | null }>; label?: number }) {
  if (!active || !payload || payload.length === 0 || typeof label !== 'number') {
    return null
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
      <p className="font-medium text-slate-700">{formatClock(label)}</p>
      {payload
        .filter((item) => typeof item.value === 'number' && Number.isFinite(item.value))
        .map((item) => (
          <p key={item.name} className="text-slate-600">
            {item.name}: {(item.value ?? 0).toFixed(3)}
          </p>
        ))}
    </div>
  )
}

function TrendMiniChart({
  title,
  field,
  dataBySymbol,
  selectedSymbols,
}: {
  title: string
  field: SeriesField
  dataBySymbol: Record<SymbolKey, MetricSeriesPoint[]>
  selectedSymbols: SymbolKey[]
}) {
  const data = buildFrames(dataBySymbol, field)

  if (data.length === 0) {
    return (
      <div className="rounded-xl border border-slate-200 bg-white p-3">
        <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{title}</p>
        <p className="mt-2 text-sm text-slate-500">No signal data in selected window.</p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{title}</p>
      <div className="mt-2 h-36 w-full">
        <ResponsiveContainer>
          <LineChart data={data} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="#e5e7eb" strokeDasharray="2 6" vertical={false} />
            <XAxis
              dataKey="ts"
              tickFormatter={formatClock}
              tickLine={false}
              axisLine={false}
              minTickGap={28}
              stroke="#94a3b8"
              type="number"
              domain={['dataMin', 'dataMax']}
            />
            <YAxis
              tickLine={false}
              axisLine={false}
              width={44}
              stroke="#94a3b8"
              tickFormatter={(value) => Number(value).toFixed(1)}
            />
            <Tooltip content={<TrendTooltip />} />
            <ReferenceLine y={0} stroke="#94a3b8" strokeDasharray="4 4" />

            {selectedSymbols.includes('btcusdt') && (
              <Line
                type="monotone"
                dataKey="btcusdt"
                name={symbolLabel('btcusdt')}
                dot={false}
                stroke="#0f172a"
                strokeWidth={1.6}
                isAnimationActive
                animationDuration={220}
              />
            )}
            {selectedSymbols.includes('ethusdt') && (
              <Line
                type="monotone"
                dataKey="ethusdt"
                name={symbolLabel('ethusdt')}
                dot={false}
                stroke="#475569"
                strokeWidth={1.6}
                isAnimationActive
                animationDuration={220}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

export function SignalTrendChart({ metricSeriesBySymbol, selectedSymbols }: SignalTrendChartProps) {
  return (
    <div className="grid gap-3 md:grid-cols-2">
      <TrendMiniChart
        title="Return EWMA Z"
        field="return_z_ewma_1m"
        dataBySymbol={metricSeriesBySymbol}
        selectedSymbols={selectedSymbols}
      />
      <TrendMiniChart
        title="Volatility Z"
        field="vol_z_1m"
        dataBySymbol={metricSeriesBySymbol}
        selectedSymbols={selectedSymbols}
      />
    </div>
  )
}
