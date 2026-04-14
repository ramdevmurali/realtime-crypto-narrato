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

import type { AlertOverlayPoint, PriceSeriesPoint, SymbolKey } from './types'
import { formatClock, formatPrice, symbolLabel } from './ui-utils'

type MarketOverviewChartProps = {
  priceSeriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>
  alertOverlay: AlertOverlayPoint[]
  selectedSymbols: SymbolKey[]
}

type PriceFrame = {
  ts: number
  btcusdt: number | null
  ethusdt: number | null
}

type AlertDot = {
  x: number
  y: number
  symbol: SymbolKey
  direction: string
}

function buildFrames(seriesBySymbol: Record<SymbolKey, PriceSeriesPoint[]>): PriceFrame[] {
  const frameMap = new Map<number, PriceFrame>()

  for (const symbol of ['btcusdt', 'ethusdt'] as const) {
    for (const point of seriesBySymbol[symbol]) {
      const existing = frameMap.get(point.ts)
      if (existing) {
        existing[symbol] = point.price
      } else {
        frameMap.set(point.ts, {
          ts: point.ts,
          btcusdt: symbol === 'btcusdt' ? point.price : null,
          ethusdt: symbol === 'ethusdt' ? point.price : null,
        })
      }
    }
  }

  return Array.from(frameMap.values()).sort((a, b) => a.ts - b.ts)
}

function buildAlertDots(points: AlertOverlayPoint[], selectedSymbols: SymbolKey[]): AlertDot[] {
  const selected = new Set(selectedSymbols)
  return points
    .filter((point) => selected.has(point.symbol))
    .map((point) => ({
      x: point.ts,
      y: point.price,
      symbol: point.symbol,
      direction: point.direction,
    }))
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

function ChartTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ name?: string; value?: number | null }>; label?: number }) {
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
            {item.name}: {formatPrice(item.value ?? null)}
          </p>
        ))}
    </div>
  )
}

export function MarketOverviewChart({
  priceSeriesBySymbol,
  alertOverlay,
  selectedSymbols,
}: MarketOverviewChartProps) {
  const data = buildFrames(priceSeriesBySymbol)
  const alertDots = buildAlertDots(alertOverlay, selectedSymbols)

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
            tickFormatter={(value) => `$${Number(value).toLocaleString()}`}
            stroke="#94a3b8"
            width={80}
          />
          <Tooltip content={<ChartTooltip />} />
          <Legend
            iconType="plainline"
            formatter={(value) => (
              <span className="text-xs text-slate-500">
                {value === 'btcusdt' ? symbolLabel('btcusdt') : symbolLabel('ethusdt')}
              </span>
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
              stroke="#0f172a"
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
              stroke="#334155"
              strokeWidth={1.8}
            />
          )}

          <Scatter data={alertDots} shape={<AlertDotShape />} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
