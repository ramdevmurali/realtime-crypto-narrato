import type { SymbolFilter, TimeWindow } from './types'

type DashboardControlsProps = {
  symbolFilter: SymbolFilter
  onChangeSymbol: (value: SymbolFilter) => void
  window: TimeWindow
  onChangeWindow: (value: TimeWindow) => void
}

const SYMBOL_OPTIONS: Array<{ value: SymbolFilter; label: string }> = [
  { value: 'both', label: 'Both' },
  { value: 'btcusdt', label: 'BTC' },
  { value: 'ethusdt', label: 'ETH' },
]

const WINDOW_OPTIONS: TimeWindow[] = [5, 15, 30]

function chipClass(active: boolean): string {
  if (active) {
    return 'border-slate-900 bg-slate-900 text-white'
  }
  return 'border-slate-300 bg-white text-slate-600 hover:border-slate-400 hover:text-slate-900'
}

export function DashboardControls({
  symbolFilter,
  onChangeSymbol,
  window,
  onChangeWindow,
}: DashboardControlsProps) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white/80 px-3 py-2 shadow-sm backdrop-blur">
      <div className="flex items-center gap-2">
        <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Symbols</p>
        {SYMBOL_OPTIONS.map((option) => (
          <button
            key={option.value}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${chipClass(symbolFilter === option.value)}`}
            onClick={() => onChangeSymbol(option.value)}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>

      <div className="flex items-center gap-2">
        <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Window</p>
        {WINDOW_OPTIONS.map((option) => (
          <button
            key={option}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${chipClass(window === option)}`}
            onClick={() => onChangeWindow(option)}
            type="button"
          >
            {option}m
          </button>
        ))}
      </div>
    </div>
  )
}
