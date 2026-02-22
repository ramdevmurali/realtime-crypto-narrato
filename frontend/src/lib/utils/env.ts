const FALLBACK_API_BASE_URL = 'http://localhost:8000'

export function getApiBaseUrl(): string {
  const configured = import.meta.env.VITE_API_BASE_URL?.trim()
  const baseUrl = configured && configured.length > 0 ? configured : FALLBACK_API_BASE_URL
  return baseUrl.replace(/\/$/, '')
}
