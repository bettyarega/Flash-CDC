// src/api/users.ts
import type { User, UserCreatePayload, UserUpdatePayload } from '../types'
import { api } from './clients' // reuse the same JSON+auth helper

export async function listUsers(): Promise<User[]> {
  return api<User[]>('/auth/users')
}

export async function getUser(id: number): Promise<User> {
  return api<User>(`/auth/users/${id}`)
}

export async function createUser(payload: UserCreatePayload): Promise<User> {
  return api<User>('/auth/users', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function updateUser(id: number, payload: UserUpdatePayload): Promise<User> {
  const body = { ...payload }
  Object.keys(body).forEach(k => (body as any)[k] === undefined && delete (body as any)[k])
  return api<User>(`/auth/users/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  })
}

export async function deleteUser(id: number): Promise<void> {
  await api<void>(`/auth/users/${id}`, { method: 'DELETE' })
}
