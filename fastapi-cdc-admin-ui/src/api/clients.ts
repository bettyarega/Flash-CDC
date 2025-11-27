// src/api/clients.ts
import type { Client } from '../types'
import { getToken, handleUnauthorized } from './session'

export const BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/+$/, '') ||
  'http://127.0.0.1:8000'

type ListResponse<T> = { items?: T[] } | T[]

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken()
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...init,
  })

  if (!res.ok) {
    // Handle 401 Unauthorized - session expired
    if (res.status === 401) {
      handleUnauthorized()
      throw new Error('Session expired. Please sign in again.')
    }
    // Try to parse JSON error response
    const text = await res.text().catch(() => '')
    let errorMessage = text || `${res.status} ${res.statusText}`
    
    try {
      const errorJson = JSON.parse(text)
      
      // Handle FastAPI validation errors (array format)
      if (Array.isArray(errorJson.detail)) {
        const formattedErrors = errorJson.detail.map((err: any) => {
          const field = err.loc && err.loc.length > 1 ? err.loc[err.loc.length - 1] : 'field'
          // Convert snake_case to Title Case
          const fieldName = field
            .split('_')
            .map((word: string) => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ')
          return `${fieldName}: ${err.msg}`
        })
        errorMessage = formattedErrors.join('\n')
      } else if (errorJson.detail) {
        // Handle simple detail string or object
        errorMessage = typeof errorJson.detail === 'string' 
          ? errorJson.detail 
          : errorJson.message || JSON.stringify(errorJson.detail)
      } else if (errorJson.message) {
        errorMessage = errorJson.message
      }
    } catch {
      // If JSON parsing fails, use the text as-is (already set above)
    }
    
    throw new Error(errorMessage)
  }

  // 204 No Content → no body to parse
  if (res.status === 204) {
    return undefined as unknown as T
  }

  // Read as text first; handle truly empty bodies
  const txt = await res.text().catch(() => '')
  if (!txt) {
    return undefined as unknown as T
  }

  // Parse JSON when present
  try {
    return JSON.parse(txt) as T
  } catch {
    // Not JSON (unlikely for our API) — return undefined
    return undefined as unknown as T
  }
}

export async function listClients(): Promise<Client[]> {
  const data = await api<ListResponse<Client>>('/clients/')
  return Array.isArray(data) ? data : data.items ?? []
}

export type ClientStatus = {
  id: number
  client_name: string
  topic_name: string
  is_active: boolean
  webhook_url: string
  listener_status: 'starting' | 'running' | 'stopping' | 'stopped' | 'error'
  listener_running: boolean
  last_error?: string | null
  started_at?: string | null
  last_beat?: string | null
  events_received?: number
  fail_count?: number
}

export async function getClientsStatus(): Promise<ClientStatus[]> {
  const data = await api<{ items: ClientStatus[] }>('/clients/status')
  return data.items ?? []
}

export type CreateClientPayload = Omit<Client, 'id' | 'created_at' | 'updated_at'>

export async function createClient(payload: CreateClientPayload): Promise<Client> {
  return api<Client>('/clients/', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

type UpdatePayload = Partial<CreateClientPayload>

/** strip server-managed fields + empty secrets before sending */
function sanitizeUpdatePayload(p: Partial<Client>): UpdatePayload {
  const {
    id,
    created_at,
    updated_at,
    // allowed fields below
    client_name,
    login_url,
    oauth_grant_type,
    oauth_client_id,
    oauth_client_secret,
    oauth_username,
    oauth_password,
    topic_name,
    webhook_url,
    // pubsub_host,
    flow_batch_size,
    is_active,
  } = p as any

  const out: any = {
    client_name,
    login_url,
    oauth_grant_type,
    oauth_client_id,
    topic_name,
    webhook_url,
    // pubsub_host,
    is_active,
  }

  if (typeof flow_batch_size === 'number') out.flow_batch_size = flow_batch_size

  // only include secrets if non-empty strings
  if (typeof oauth_client_secret === 'string' && oauth_client_secret.trim() !== '') {
    out.oauth_client_secret = oauth_client_secret
  }
  if (typeof oauth_username === 'string') {
    out.oauth_username = oauth_username
  }
  if (typeof oauth_password === 'string' && oauth_password.trim() !== '') {
    out.oauth_password = oauth_password
  }

  // remove undefined keys
  Object.keys(out).forEach((k) => out[k] === undefined && delete out[k])
  return out
}

export async function updateClient(id: number, payload: Partial<Client>): Promise<Client> {
  const body = sanitizeUpdatePayload(payload)
  return api<Client>(`/clients/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  })
}

export async function deleteClient(id: number): Promise<void> {
  await api<void>(`/clients/${id}`, { method: 'DELETE' })
}

// ---- Test Connection API ----
export type TestConnectionPayload = {
  login_url: string
  oauth_grant_type: string
  oauth_client_id: string
  oauth_client_secret: string
  oauth_username?: string | null
  oauth_password?: string | null
  topic_name?: string | null
  pubsub_host?: string | null
  tenant_id?: string | null
  check_topic?: boolean
}

export type TestConnectionResult = {
  ok: boolean
  auth: { ok: boolean; org_id?: string; instance_url?: string; error?: string }
  topic?: { ok: boolean; schema_id?: string; code?: string; error?: string }
}

export async function testConnection(payload: TestConnectionPayload): Promise<TestConnectionResult> {
  return api<TestConnectionResult>('/clients/test-connection', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}
