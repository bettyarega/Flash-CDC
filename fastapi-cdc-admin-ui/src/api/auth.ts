// src/api/auth.ts
import { getToken, setSession, loadSession } from './session'

const BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/+$/, '') ||
  'http://127.0.0.1:8000'

export async function login(email: string, password: string) {
  const form = new URLSearchParams()
  form.set('username', email)
  form.set('password', password)

  const res = await fetch(`${BASE_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: form.toString(),
  })
  if (!res.ok) throw new Error(await res.text())
  const data = await res.json()
  setSession(data.access_token, data.user)
  return data.user as { id: number; email: string; role: 'admin' | 'user' }
}

export function bootSession() {
  loadSession()
}

export async function me() {
  const token = getToken()
  const res = await fetch(`${BASE_URL}/auth/me`, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  })
  if (!res.ok) throw new Error(await res.text())
  return (await res.json()) as { id: number; email: string; role: 'admin' | 'user'; is_active: boolean }
}
