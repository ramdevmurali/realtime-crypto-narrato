const FALLBACK_API_BASE_URL = '/api'

export function getApiBaseUrl(): string {
  const configured = import.meta.env.VITE_API_BASE_URL?.trim()
  const baseUrl = configured && configured.length > 0 ? configured : FALLBACK_API_BASE_URL
  if (baseUrl === '/') {
    return ''
  }
  return baseUrl.replace(/\/+$/, '')
}
