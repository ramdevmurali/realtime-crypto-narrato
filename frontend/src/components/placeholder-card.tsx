import { type PropsWithChildren } from 'react'

type PlaceholderCardProps = PropsWithChildren<{
  title: string
  description: string
}>

export function PlaceholderCard({ title, description, children }: PlaceholderCardProps) {
  return (
    <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <header className="mb-3">
        <h2 className="text-base font-semibold text-slate-900">{title}</h2>
        <p className="text-sm text-slate-500">{description}</p>
      </header>
      <div className="text-sm text-slate-600">{children}</div>
    </section>
  )
}
