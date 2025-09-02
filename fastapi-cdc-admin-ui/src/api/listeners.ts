import type { ListenerState } from '../types'

const BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/+$/, '') ||
  'http://127.0.0.1:8000'

type ListResponse<T> = { items?: T[] } | T[]

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

export async function listListenerStatuses(): Promise<ListenerState[]> {
  const data = await api<ListResponse<ListenerState>>('/listeners')
  return Array.isArray(data) ? data : data.items ?? []
}

export async function startListener(id: number): Promise<ListenerState> {
  return api<ListenerState>(`/listeners/${id}/start`, { method: 'POST' })
}

export async function stopListener(id: number): Promise<ListenerState> {
  return api<ListenerState>(`/listeners/${id}/stop`, { method: 'POST' })
}

export async function restartListener(id: number): Promise<ListenerState> {
  return api<ListenerState>(`/listeners/${id}/restart`, { method: 'POST' })
}