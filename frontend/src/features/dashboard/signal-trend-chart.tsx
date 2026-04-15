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

const SYMBOLS: SymbolKey[] = ['btcusdt', 'ethusdt']
const METRIC_BUCKET_MS = 1000

const Y_DOMAIN_BY_FIELD: Record<SeriesField, [number, number]> = {
  return_z_ewma_1m: [-3.5, 3.5],
  vol_z_1m: [-3, 4],
}

function resolveYDomain(field: SeriesField): [number, number] {
  return Y_DOMAIN_BY_FIELD[field]
}

function toBucketTs(ts: number, bucketMs: number): number {
  return Math.floor(ts / bucketMs) * bucketMs
}

function buildFrames(
  dataBySymbol: Record<SymbolKey, MetricSeriesPoint[]>,
  field: SeriesField,
  selectedSymbols: SymbolKey[]
): Frame[] {
  const selectedSet = new Set(selectedSymbols)
  const sortedBySymbol: Record<SymbolKey, MetricSeriesPoint[]> = {
    btcusdt: [...dataBySymbol.btcusdt].sort((a, b) => a.ts - b.ts),
    ethusdt: [...dataBySymbol.ethusdt].sort((a, b) => a.ts - b.ts),
  }

  let minTs = Number.POSITIVE_INFINITY
  let maxTs = Number.NEGATIVE_INFINITY

  for (const symbol of SYMBOLS) {
    if (!selectedSet.has(symbol)) {
      continue
    }
    const series = sortedBySymbol[symbol]
    if (series.length === 0) {
      continue
    }
    minTs = Math.min(minTs, series[0].ts)
    maxTs = Math.max(maxTs, series[series.length - 1].ts)
  }

  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) {
    return []
  }

  const start = toBucketTs(minTs, METRIC_BUCKET_MS)
  const end = toBucketTs(maxTs, METRIC_BUCKET_MS)
  const pointers: Record<SymbolKey, number> = { btcusdt: 0, ethusdt: 0 }
  const carried: Record<SymbolKey, number | null> = { btcusdt: null, ethusdt: null }
  const frames: Frame[] = []

  for (let bucketTs = start; bucketTs <= end; bucketTs += METRIC_BUCKET_MS) {
    for (const symbol of SYMBOLS) {
      const series = sortedBySymbol[symbol]
      while (pointers[symbol] < series.length) {
        const point = series[pointers[symbol]]
        if (toBucketTs(point.ts, METRIC_BUCKET_MS) > bucketTs) {
          break
        }
        carried[symbol] = point[field]
        pointers[symbol] += 1
      }
    }

    frames.push({
      ts: bucketTs,
      btcusdt: carried.btcusdt,
      ethusdt: carried.ethusdt,
    })
  }

  return frames
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
  const data = buildFrames(dataBySymbol, field, selectedSymbols)
  const yDomain = resolveYDomain(field)

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
              domain={yDomain}
              allowDataOverflow
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
                connectNulls
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
                connectNulls
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
