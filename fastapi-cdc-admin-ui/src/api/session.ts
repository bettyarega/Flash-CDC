// src/api/session.ts
export type AuthUser = { id: number; email: string; role: 'admin' | 'user' }
let _token: string | null = null
let _user: AuthUser | null = null
let _logoutHandler: (() => void) | null = null

export function setSession(token: string, user: AuthUser) {
  _token = token
  _user = user
  localStorage.setItem('jwt', token)
  localStorage.setItem('user', JSON.stringify(user))
}

export function loadSession() {
  const t = localStorage.getItem('jwt')
  const u = localStorage.getItem('user')
  _token = t
  _user = u ? JSON.parse(u) : null
}

// NEW: convenience alias so imports like `bootSession()` work from this module
export function bootSession() {
  loadSession()
}

export function clearSession() {
  _token = null
  _user = null
  localStorage.removeItem('jwt')
  localStorage.removeItem('user')
}

export function setLogoutHandler(handler: () => void) {
  _logoutHandler = handler
}

export function handleUnauthorized() {
  clearSession()
  if (_logoutHandler) {
    _logoutHandler()
  }
}

export function getToken(): string | null { return _token }
export function getUser(): AuthUser | null { return _user }
export function isAdmin(): boolean { return _user?.role === 'admin' }
export function isLoggedIn(): boolean { return !!_token }
