import type { ListenerState } from '../types'

const BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/+$/, '') ||
  'http://127.0.0.1:8000'

type ListResponse<T> = { items?: T[] } | T[]

type ReplayOptions = {
  mode?: 'stored' | 'latest' | 'earliest' | 'since' | 'custom'
  since_minutes?: number
  replay_id_b64?: string
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })

  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`${res.status} ${res.statusText}: ${text}`)
  }

  if (res.status === 204) return undefined as unknown as T
  const txt = await res.text().catch(() => '')
  if (!txt) return undefined as unknown as T
  try {
    return JSON.parse(txt) as T
  } catch {
    return undefined as unknown as T
  }
}

function buildQuery(opts?: ReplayOptions) {
  if (!opts) return ''
  const qs = new URLSearchParams()
  if (opts.mode) qs.set('mode', opts.mode)
  if (opts.since_minutes != null) qs.set('since_minutes', String(opts.since_minutes))
  if (opts.replay_id_b64) qs.set('replay_id_b64', opts.replay_id_b64)
  const s = qs.toString()
  return s ? `?${s}` : ''
}

export async function listListenerStatuses(): Promise<ListenerState[]> {
  const data = await api<ListResponse<ListenerState>>('/listeners')
  return Array.isArray(data) ? data : data.items ?? []
}

export async function startListener(id: number, opts?: ReplayOptions): Promise<any> {
  return api(`/listeners/${id}/start${buildQuery(opts)}`, { method: 'POST' })
}

export async function stopListener(id: number): Promise<ListenerState> {
  return api<ListenerState>(`/listeners/${id}/stop`, { method: 'POST' })
}

export async function restartListener(id: number, opts?: ReplayOptions): Promise<any> {
  return api(`/listeners/${id}/restart${buildQuery(opts)}`, { method: 'POST' })
}

