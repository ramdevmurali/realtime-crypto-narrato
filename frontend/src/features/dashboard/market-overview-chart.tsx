import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { AlertOverlayPoint, PriceSeriesPoint, SymbolKey, TimeWindow } from './types'
import { formatClock, formatPrice, symbolLabel, symbolStroke } from './ui-utils'

type MarketOverviewChartProps = {
  priceSeriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>
  alertOverlay: AlertOverlayPoint[]
  selectedSymbols: SymbolKey[]
  window: TimeWindow
}

type ChartMode = 'raw' | 'normalized'

type PriceFrame = {
  ts: number
  btcusdt: number | null
  ethusdt: number | null
}

type BaselineMap = Partial<Record<SymbolKey, number>>

type AlertDot = {
  x: number
  y: number
  symbol: SymbolKey
  direction: string
}

const SYMBOLS: SymbolKey[] = ['btcusdt', 'ethusdt']
const PRICE_BUCKET_MS = 1000
const NORMALIZED_Y_DOMAIN: Record<TimeWindow, [number, number]> = {
  5: [99.55, 100.45],
  15: [99.6, 100.4],
  30: [99.5, 100.5],
}

function toBucketTs(ts: number, bucketMs: number): number {
  return Math.floor(ts / bucketMs) * bucketMs
}

function buildFrames(
  seriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>,
  selectedSymbols: SymbolKey[],
  bucketMs: number
): PriceFrame[] {
  const selectedSet = new Set(selectedSymbols)

  const sortedBySymbol: Record<SymbolKey, PriceSeriesPoint[]> = {
    btcusdt: [...seriesBySymbol.btcusdt].sort((a, b) => a.ts - b.ts),
    ethusdt: [...seriesBySymbol.ethusdt].sort((a, b) => a.ts - b.ts),
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

  const start = toBucketTs(minTs, bucketMs)
  const end = toBucketTs(maxTs, bucketMs)

  const pointers: Record<SymbolKey, number> = { btcusdt: 0, ethusdt: 0 }
  const carried: Record<SymbolKey, number | null> = { btcusdt: null, ethusdt: null }
  const frames: PriceFrame[] = []

  for (let bucketTs = start; bucketTs <= end; bucketTs += bucketMs) {
    for (const symbol of SYMBOLS) {
      const series = sortedBySymbol[symbol]
      while (pointers[symbol] < series.length) {
        const point = series[pointers[symbol]]
        if (toBucketTs(point.ts, bucketMs) > bucketTs) {
          break
        }
        carried[symbol] = point.price
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

function resolveChartMode(selectedSymbols: SymbolKey[]): ChartMode {
  return selectedSymbols.length === 2 ? 'normalized' : 'raw'
}

function pickBaselines(frames: PriceFrame[]): BaselineMap {
  const baselines: BaselineMap = {}

  for (const symbol of SYMBOLS) {
    const point = frames.find((frame) => {
      const value = frame[symbol]
      return typeof value === 'number' && Number.isFinite(value) && value > 0
    })

    if (point) {
      baselines[symbol] = point[symbol] as number
    }
  }

  return baselines
}

function normalizeValue(value: number | null, baseline: number | undefined): number | null {
  if (value === null || baseline === undefined || baseline <= 0) {
    return null
  }
  return (value / baseline) * 100
}

function transformFrames(frames: PriceFrame[], mode: ChartMode): PriceFrame[] {
  if (mode === 'raw') {
    return frames
  }

  const baselines = pickBaselines(frames)

  return frames.map((frame) => ({
    ts: frame.ts,
    btcusdt: normalizeValue(frame.btcusdt, baselines.btcusdt),
    ethusdt: normalizeValue(frame.ethusdt, baselines.ethusdt),
  }))
}

function resolveNormalizedDomain(window: TimeWindow): [number, number] {
  return NORMALIZED_Y_DOMAIN[window]
}

function buildFrameLookup(data: PriceFrame[]): Record<SymbolKey, Map<number, number | null>> {
  const lookup: Record<SymbolKey, Map<number, number | null>> = {
    btcusdt: new Map<number, number | null>(),
    ethusdt: new Map<number, number | null>(),
  }

  for (const frame of data) {
    lookup.btcusdt.set(frame.ts, frame.btcusdt)
    lookup.ethusdt.set(frame.ts, frame.ethusdt)
  }

  return lookup
}

function buildAlertDots(
  points: AlertOverlayPoint[],
  selectedSymbols: SymbolKey[],
  frameLookup: Record<SymbolKey, Map<number, number | null>>,
  bucketMs: number
): AlertDot[] {
  const selected = new Set(selectedSymbols)

  return points
    .filter((point) => selected.has(point.symbol))
    .flatMap((point) => {
      const bucketTs = toBucketTs(point.ts, bucketMs)
      const yValue = frameLookup[point.symbol].get(bucketTs)
      if (typeof yValue !== 'number' || !Number.isFinite(yValue)) {
        return []
      }

      return [{
        x: bucketTs,
        y: yValue,
        symbol: point.symbol,
        direction: point.direction,
      }]
    })
}

function AlertDotShape(props: {
  cx?: number
  cy?: number
  payload?: AlertDot
}) {
  if (typeof props.cx !== 'number' || typeof props.cy !== 'number' || !props.payload) {
    return null
  }

  const normalized = props.payload.direction.trim().toLowerCase()
  const fill = normalized === 'up' ? '#059669' : normalized === 'down' ? '#e11d48' : '#475569'

  return <circle cx={props.cx} cy={props.cy} fill={fill} r={3} stroke="#ffffff" strokeWidth={1.5} />
}

function labelForSeries(name?: string): string {
  if (name === 'btcusdt') {
    return symbolLabel('btcusdt')
  }
  if (name === 'ethusdt') {
    return symbolLabel('ethusdt')
  }
  return name ?? ''
}

function ChartTooltip({
  active,
  payload,
  label,
  mode,
}: {
  active?: boolean
  payload?: Array<{ name?: string; value?: number | null }>
  label?: number
  mode: ChartMode
}) {
  if (!active || !payload || payload.length === 0 || typeof label !== 'number') {
    return null
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
      <p className="font-medium text-slate-700">{formatClock(label)}</p>
      {payload
        .filter(
          (item) =>
            (item.name === 'btcusdt' || item.name === 'ethusdt') &&
            typeof item.value === 'number' &&
            Number.isFinite(item.value)
        )
        .map((item) => {
          if (mode === 'normalized') {
            const indexValue = item.value ?? 0
            const changePct = indexValue - 100
            return (
              <p key={item.name} className="text-slate-600">
                {labelForSeries(item.name)}: {indexValue.toFixed(2)} ({changePct >= 0 ? '+' : ''}{changePct.toFixed(2)}%)
              </p>
            )
          }

          return (
            <p key={item.name} className="text-slate-600">
              {labelForSeries(item.name)}: {formatPrice(item.value ?? null)}
            </p>
          )
        })}
    </div>
  )
}

export function MarketOverviewChart({
  priceSeriesBySymbol,
  alertOverlay,
  selectedSymbols,
  window,
}: MarketOverviewChartProps) {
  const rawFrames = buildFrames(priceSeriesBySymbol, selectedSymbols, PRICE_BUCKET_MS)
  const chartMode = resolveChartMode(selectedSymbols)
  const data = transformFrames(rawFrames, chartMode)
  const frameLookup = buildFrameLookup(data)
  const normalizedDomain = resolveNormalizedDomain(window)
  const alertDots = buildAlertDots(
    alertOverlay,
    selectedSymbols,
    frameLookup,
    PRICE_BUCKET_MS
  )

  if (data.length === 0) {
    return <p className="text-sm text-slate-500">No price data in selected window.</p>
  }

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer>
        <ComposedChart data={data} margin={{ top: 10, right: 12, left: 8, bottom: 0 }}>
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
            domain={chartMode === 'normalized' ? normalizedDomain : ['auto', 'auto']}
            tickFormatter={(value) =>
              chartMode === 'normalized'
                ? `${Number(value).toFixed(1)}`
                : `$${Number(value).toLocaleString()}`
            }
            stroke="#94a3b8"
            width={80}
          />
          <Tooltip content={<ChartTooltip mode={chartMode} />} />
          <Legend
            iconType="plainline"
            formatter={(value) => (
              <span className="text-xs text-slate-500">{labelForSeries(String(value))}</span>
            )}
          />

          {selectedSymbols.includes('btcusdt') && (
            <Line
              type="monotone"
              dataKey="btcusdt"
              dot={false}
              isAnimationActive
              animationDuration={220}
              name="btcusdt"
              stroke={symbolStroke('btcusdt')}
              strokeWidth={1.8}
            />
          )}

          {selectedSymbols.includes('ethusdt') && (
            <Line
              type="monotone"
              dataKey="ethusdt"
              dot={false}
              isAnimationActive
              animationDuration={220}
              name="ethusdt"
              stroke={symbolStroke('ethusdt')}
              strokeWidth={1.8}
            />
          )}

          <Scatter data={alertDots} legendType="none" shape={<AlertDotShape />} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
