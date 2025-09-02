// src/App.tsx
import { useEffect, useMemo, useState } from 'react'
import type { Client, ListenerState, User } from './types'
import {
  listClients,
  createClient,
  updateClient,
  deleteClient,
} from './api/clients'
import {
  listListenerStatuses,
  startListener,
  stopListener,
  restartListener,
} from './api/listeners'
import ClientForm from './components/ClientForm'
import Login from './pages/Login'
import { loadSession, isLoggedIn, isAdmin, clearSession } from './api/session'

import { listUsers, createUser, updateUser, deleteUser } from './api/users'
import UserForm from './components/UserForm'

function StatusBadge({ state }: { state?: ListenerState }) {
  const s = state?.status ?? 'stopped'
  const cls =
    s === 'running'
      ? 'bg-green-100 text-green-800'
      : s === 'starting'
      ? 'bg-blue-100 text-blue-800'
      : s === 'stopping'
      ? 'bg-amber-100 text-amber-800'
      : s === 'error'
      ? 'bg-red-100 text-red-800'
      : 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {s}
    </span>
  )
}

export default function App() {
  const [authed, setAuthed] = useState(false)

  useEffect(() => {
    loadSession()
    setAuthed(isLoggedIn())
  }, [])

  return authed ? (
    <AuthedApp onLogout={() => { clearSession(); setAuthed(false) }} />
  ) : (
    <Login onSuccess={() => setAuthed(true)} />
  )
}

function AuthedApp({ onLogout }: { onLogout: () => void }) {
  const [view, setView] = useState<'clients' | 'users'>('clients')

  return (
    <div className="max-w-7xl mx-auto p-6 flex flex-col gap-6">
      <header className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">Flash Admin</h1>
          <nav className="flex gap-2">
            <button
              onClick={() => setView('clients')}
              className={`px-2 py-1 rounded border ${view === 'clients' ? 'bg-black text-white' : ''}`}
            >
              Clients
            </button>
            {isAdmin() && (
              <button
                onClick={() => setView('users')}
                className={`px-2 py-1 rounded border ${view === 'users' ? 'bg-black text-white' : ''}`}
              >
                Users
              </button>
            )}
          </nav>
        </div>
        <div className="flex gap-2 items-center">
          <button className="px-3 py-2 rounded border" onClick={onLogout}>
            Sign out
          </button>
        </div>
      </header>

      {view === 'clients' ? <ClientsPanel /> : <UsersPanel />}
    </div>
  )
}

/** Existing clients dashboard preserved */
function ClientsPanel() {
  const [clients, setClients] = useState<Client[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [showCreate, setShowCreate] = useState(false)
  const [editClientId, setEditClientId] = useState<number | null>(null)

  const [listenerMap, setListenerMap] = useState<Record<number, ListenerState>>({})
  const [busyRow, setBusyRow] = useState<number | null>(null)

  async function refreshClients() {
    setLoading(true)
    setError(null)
    try {
      const data = await listClients()
      setClients(data)
    } catch (e: any) {
      setError(e.message ?? String(e))
    } finally {
      setLoading(false)
    }
  }

  async function refreshStatuses() {
    try {
      const items = await listListenerStatuses()
      const map: Record<number, ListenerState> = {}
      for (const s of items) map[s.client_id] = s
      setListenerMap(map)
    } catch {}
  }

  // visibility-aware polling
  useEffect(() => {
    let timer: any
    async function tick() {
      try {
        await refreshClients()
        await refreshStatuses()
      } finally {
        const visible = document.visibilityState === 'visible'
        const next = visible ? 60000 : 3600000
        timer = setTimeout(tick, next)
      }
    }
    function onVisChange() {
      clearTimeout(timer)
      tick()
    }
    document.addEventListener('visibilitychange', onVisChange)
    tick()
    return () => {
      document.removeEventListener('visibilitychange', onVisChange)
      clearTimeout(timer)
    }
  }, [])

  async function handleCreate(values: Partial<Client>) {
    await createClient(values as any)
    setShowCreate(false)
    await refreshClients()
    await refreshStatuses()
  }

  async function handleEditSave(values: Partial<Client>) {
    if (!editClientId) return
    await updateClient(editClientId, values as any)
    setEditClientId(null)
    await refreshClients()
    await refreshStatuses()
  }

  async function handleDelete(client: Client) {
    const ok = window.confirm(
      `Delete "${client.client_name}"?\n\nThis will stop its listener and remove the configuration.`
    )
    if (!ok) return
    try {
      await deleteClient(client.id!)
      await refreshClients()
      await refreshStatuses()
    } catch (e: any) {
      alert(e.message ?? String(e))
    }
  }

  async function handleStartListener(id: number) {
    setBusyRow(id)
    try {
      await startListener(id)
      await refreshStatuses()
    } catch (e: any) {
      alert(e.message ?? String(e))
    } finally {
      setBusyRow(prev => (prev === id ? null : prev))
    }
  }

  async function handleStopListener(id: number) {
    setBusyRow(id)
    try {
      await stopListener(id)
      await refreshStatuses()
    } catch (e: any) {
      alert(e.message ?? String(e))
    } finally {
      setBusyRow(prev => (prev === id ? null : prev))
    }
  }

  async function handleRestartListener(id: number) {
    setBusyRow(id)
    try {
      await restartListener(id)
      await refreshStatuses()
    } catch (e: any) {
      alert(e.message ?? String(e))
    } finally {
      setBusyRow(prev => (prev === id ? null : prev))
    }
  }

  const editing = useMemo(
    () => clients.find(c => c.id === editClientId) || null,
    [clients, editClientId]
  )

  return (
    <>
      {error && <div className="p-3 rounded bg-red-50 text-red-700 border border-red-200">{error}</div>}

      {loading ? (
        <p>Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded border border-neutral-200 bg-white">
          <div className="flex justify-end p-3">
            <button
              onClick={() => setShowCreate(true)}
              className="px-3 py-2 rounded bg-green-600 text-white"
            >
              + Add Client
            </button>
          </div>
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="text-left border-b bg-neutral-50">
                <th className="p-2">ID</th>
                <th className="p-2">Name</th>
                <th className="p-2">Topic</th>
                <th className="p-2">Active</th>
                <th className="p-2">Listener</th>
                <th className="p-2">Listener Controls</th>
                <th className="p-2">Edit/Delete</th>
              </tr>
            </thead>
            <tbody>
              {clients.map((c) => {
                const st = listenerMap[c.id!]
                const disabled = busyRow === c.id
                return (
                  <tr key={c.id} className="border-b">
                    <td className="p-2">{c.id}</td>
                    <td className="p-2">{c.client_name}</td>
                    <td className="p-2">{c.topic_name}</td>
                    <td className="p-2">{c.is_active ? 'Yes' : 'No'}</td>
                    <td className="p-2">
                      <div className="flex items-center gap-2">
                        <StatusBadge state={st} />
                        {st?.last_error ? <span title={st.last_error}>⚠️</span> : null}
                      </div>
                    </td>
                    <td className="p-2">
                      <div className="flex items-center gap-2">
                        {st?.status === 'running' ? (
                          <button
                            className="px-2 py-1 rounded border"
                            disabled={disabled}
                            onClick={() => handleStopListener(c.id!)}
                          >
                            {disabled ? 'Stopping…' : 'Stop'}
                          </button>
                        ) : (
                          <button
                            className="px-2 py-1 rounded border"
                            disabled={disabled}
                            onClick={() => handleStartListener(c.id!)}
                          >
                            {disabled ? 'Starting…' : 'Start'}
                          </button>
                        )}
                        <button
                          className="px-2 py-1 rounded border"
                          disabled={disabled}
                          onClick={() => handleRestartListener(c.id!)}
                        >
                          {disabled ? 'Restarting…' : 'Restart'}
                        </button>
                      </div>
                    </td>
                    <td className="p-2 whitespace-nowrap">
                      <div className="flex gap-2">
                        <button className="px-2 py-1 rounded border" onClick={() => setEditClientId(c.id!)}>
                          Edit
                        </button>
                        {isAdmin() && (
                          <button
                            className="px-2 py-1 rounded border border-red-300 text-red-700"
                            onClick={() => handleDelete(c)}
                          >
                            Delete
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
              {clients.length === 0 && (
                <tr>
                  <td colSpan={7} className="p-4 text-center text-gray-500">
                    No clients yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Create modal */}
      {showCreate && (
        <Modal onClose={() => setShowCreate(false)}>
          <ClientForm mode="create" onSubmit={handleCreate} onCancel={() => setShowCreate(false)} />
        </Modal>
      )}
      {/* Edit modal */}
      {editing && (
        <Modal onClose={() => setEditClientId(null)}>
          <ClientForm
            mode="edit"
            initial={editing}
            onSubmit={handleEditSave}
            onCancel={() => setEditClientId(null)}
          />
        </Modal>
      )}
    </>
  )
}

/** New Users admin panel */
function UsersPanel() {
  const [rows, setRows] = useState<User[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [showCreate, setShowCreate] = useState(false)
  const [editId, setEditId] = useState<number | null>(null)

  async function refresh() {
    setLoading(true)
    setError(null)
    try {
      setRows(await listUsers())
    } catch (e: any) {
      setError(e.message ?? String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
  }, [])

  async function handleCreate(values: any) {
    await createUser(values)
    setShowCreate(false)
    await refresh()
  }

  const editing = useMemo(() => rows.find(u => u.id === editId) || null, [rows, editId])

  async function handleEdit(values: any) {
    if (!editId) return
    await updateUser(editId, values)
    setEditId(null)
    await refresh()
  }

  async function handleDeleteUser(user: User) {
    if (!confirm(`Delete user ${user.email}?`)) return
    await deleteUser(user.id)
    await refresh()
  }

  return (
    <>
      {error && <div className="p-3 rounded bg-red-50 text-red-700 border border-red-200">{error}</div>}

      {loading ? (
        <p>Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded border border-neutral-200 bg-white">
          <div className="flex justify-end p-3">
            <button
              onClick={() => setShowCreate(true)}
              className="px-3 py-2 rounded bg-green-600 text-white"
            >
              + New User
            </button>
          </div>
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="text-left border-b bg-neutral-50">
                <th className="p-2">ID</th>
                <th className="p-2">Email</th>
                <th className="p-2">Role</th>
                <th className="p-2">Active</th>
                <th className="p-2">Created</th>
                <th className="p-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((u) => (
                <tr key={u.id} className="border-b">
                  <td className="p-2">{u.id}</td>
                  <td className="p-2">{u.email}</td>
                  <td className="p-2">{u.role}</td>
                  <td className="p-2">{u.is_active ? 'Yes' : 'No'}</td>
                  <td className="p-2">{new Date(u.created_at).toLocaleString()}</td>
                  <td className="p-2 whitespace-nowrap">
                    <div className="flex gap-2">
                      <button className="px-2 py-1 rounded border" onClick={() => setEditId(u.id)}>
                        Edit
                      </button>
                      <button
                        className="px-2 py-1 rounded border border-red-300 text-red-700"
                        onClick={() => handleDeleteUser(u)}
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="p-4 text-center text-gray-500">
                    No users yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {showCreate && (
        <Modal onClose={() => setShowCreate(false)}>
          <UserForm mode="create" onSubmit={handleCreate} onCancel={() => setShowCreate(false)} />
        </Modal>
      )}

      {editing && (
        <Modal onClose={() => setEditId(null)}>
          <UserForm mode="edit" initial={editing} onSubmit={handleEdit} onCancel={() => setEditId(null)} />
        </Modal>
      )}
    </>
  )
}

function Modal({ children, onClose }: { children: React.ReactNode; onClose: () => void }) {
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center p-4">
      <div className="bg-white rounded shadow max-w-3xl w-full p-5 relative">
        <button onClick={onClose} className="absolute right-3 top-3 px-2 rounded border" aria-label="Close">
          ✕
        </button>
        {children}
      </div>
    </div>
  )
}
