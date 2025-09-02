import { useState } from 'react'
import { login } from '../api/auth'

export default function Login({ onSuccess }: { onSuccess: () => void }) {
  const [email, setEmail] = useState('')
  const [pw, setPw] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setErr(null)
    try {
      await login(email, pw)
      onSuccess()
    } catch (e: any) {
      setErr(e.message ?? String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={submit} className="max-w-sm mx-auto mt-24 space-y-3">
      <h1 className="text-xl font-semibold">Sign in</h1>
      {err && <div className="text-red-700 bg-red-50 border border-red-200 p-2 rounded">{err}</div>}
      <input className="w-full border rounded px-3 py-2" placeholder="Email" value={email} onChange={e => setEmail(e.target.value)} />
      <input className="w-full border rounded px-3 py-2" placeholder="Password" type="password" value={pw} onChange={e => setPw(e.target.value)} />
      <button className="w-full bg-black text-white rounded px-3 py-2" disabled={loading}>{loading ? 'Signing in…' : 'Sign in'}</button>
    </form>
  )
}
