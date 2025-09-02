// src/components/UserForm.tsx
import { useState } from 'react'
import type { Role, User } from '../types'

type Props =
  | { mode: 'create'; initial?: Partial<User>; onSubmit: (values: any) => Promise<void> | void; onCancel: () => void }
  | { mode: 'edit'; initial: User; onSubmit: (values: any) => Promise<void> | void; onCancel: () => void }

export default function UserForm(props: Props) {
  const isEdit = props.mode === 'edit'
  const [email, setEmail] = useState(props.initial?.email ?? '')
  const [password, setPassword] = useState('')
  const [role, setRole] = useState<Role>((props.initial?.role as Role) ?? 'user')
  const [isActive, setIsActive] = useState<boolean>(props.initial?.is_active ?? true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    try {
      if (isEdit) {
        const payload: any = { role, is_active: isActive }
        if (password.trim()) payload.password = password
        await props.onSubmit(payload)
      } else {
        await props.onSubmit({ email, password, role, is_active: isActive })
      }
    } catch (err: any) {
      setError(err.message ?? String(err))
    } finally {
      setSaving(false)
    }
  }

  return (
    <form onSubmit={onSubmit} className="flex flex-col gap-3">
      <h3 className="text-lg font-semibold">{isEdit ? 'Edit User' : 'Create User'}</h3>
      {error && <div className="p-2 rounded bg-red-50 border border-red-200 text-red-700">{error}</div>}

      <label className="flex flex-col gap-1">
        <span className="text-sm text-neutral-700">Email</span>
        <input
          type="email"
          className="border rounded px-2 py-1"
          value={email}
          onChange={e => setEmail(e.target.value)}
          required
          disabled={isEdit}
        />
      </label>

      <label className="flex flex-col gap-1">
        <span className="text-sm text-neutral-700">{isEdit ? 'New Password (optional)' : 'Password'}</span>
        <input
          type="password"
          className="border rounded px-2 py-1"
          value={password}
          onChange={e => setPassword(e.target.value)}
          placeholder={isEdit ? 'Leave blank to keep current password' : ''}
          {...(isEdit ? {} : { required: true })}
        />
      </label>

      <label className="flex flex-col gap-1">
        <span className="text-sm text-neutral-700">Role</span>
        <select
          className="border rounded px-2 py-1"
          value={role}
          onChange={e => setRole(e.target.value as Role)}
        >
          <option value="user">User</option>
          <option value="admin">Admin</option>
        </select>
      </label>

      <label className="inline-flex items-center gap-2">
        <input type="checkbox" checked={isActive} onChange={e => setIsActive(e.target.checked)} />
        <span className="text-sm">Active</span>
      </label>

      <div className="flex gap-2 justify-end mt-2">
        <button type="button" className="px-3 py-1 rounded border" onClick={props.onCancel}>
          Cancel
        </button>
        <button
          type="submit"
          className="px-3 py-1 rounded bg-blue-600 text-white disabled:opacity-60"
          disabled={saving}
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </form>
  )
}
