import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { HeadlineFreshnessPoint } from './types'
import { formatClock } from './ui-utils'

type HeadlineFreshnessChartProps = {
  points: HeadlineFreshnessPoint[]
}

function FreshnessTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ value?: number }>; label?: number }) {
  if (!active || !payload || payload.length === 0 || typeof label !== 'number') {
    return null
  }

  const value = payload[0]?.value

  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
      <p className="font-medium text-slate-700">{formatClock(label)}</p>
      <p className="text-slate-600">Age: {typeof value === 'number' ? `${Math.round(value)}s` : 'n/a'}</p>
    </div>
  )
}

export function HeadlineFreshnessChart({ points }: HeadlineFreshnessChartProps) {
  if (points.length === 0) {
    return <p className="text-sm text-slate-500">No headline freshness data yet.</p>
  }

  return (
    <div className="h-36 w-full">
      <ResponsiveContainer>
        <AreaChart data={points} margin={{ top: 8, right: 10, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="freshnessFade" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#0f172a" stopOpacity={0.12} />
              <stop offset="95%" stopColor="#0f172a" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="#e5e7eb" strokeDasharray="2 6" vertical={false} />
          <XAxis
            dataKey="ts"
            type="number"
            domain={['dataMin', 'dataMax']}
            tickFormatter={formatClock}
            tickLine={false}
            axisLine={false}
            minTickGap={28}
            stroke="#94a3b8"
          />
          <YAxis
            tickLine={false}
            axisLine={false}
            width={44}
            stroke="#94a3b8"
            tickFormatter={(value) => `${Math.round(Number(value))}s`}
          />
          <Tooltip content={<FreshnessTooltip />} />
          <Area
            type="monotone"
            dataKey="ageSec"
            stroke="#0f172a"
            strokeWidth={1.4}
            fill="url(#freshnessFade)"
            isAnimationActive
            animationDuration={220}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
