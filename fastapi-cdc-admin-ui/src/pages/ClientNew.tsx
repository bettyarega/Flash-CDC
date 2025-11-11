import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { createClient, type CreateClientPayload } from '../api/clients'

type Grant = 'password' | 'client_credentials'

const defaults: CreateClientPayload = {
  client_name: '',
  login_url: '',
  oauth_grant_type: 'password',
  oauth_client_id: '',
  oauth_client_secret: '',
  oauth_username: '',
  oauth_password: '',
  topic_name: '/data/OpportunityChangeEvent',
  webhook_url: '',
  // pubsub_host: 'api.pubsub.salesforce.com:7443',
  tenant_id: '',
  flow_batch_size: 100,
  is_active: true,
}

export default function ClientNew() {
  const [form, setForm] = useState<CreateClientPayload>({ ...defaults })
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()

  const update = (k: keyof CreateClientPayload, v: any) =>
    setForm((f) => ({ ...f, [k]: v }))

  const validate = (): string | null => {
    if (!form.client_name.trim()) return 'Client name is required.'
    if (!/^https?:\/\//.test(form.login_url)) return 'Login URL must be http(s).'
    if (!form.oauth_client_id) return 'OAuth client id is required.'
    if (!form.oauth_client_secret) return 'OAuth client secret is required.'
    // Both password and client_credentials require username/password for Salesforce
    if (form.oauth_grant_type === 'password' || form.oauth_grant_type === 'client_credentials') {
      if (!form.oauth_username) return 'Username is required for this grant type.'
      if (!form.oauth_password) return 'Password is required for this grant type.'
    }
    if (!form.topic_name.startsWith('/data/')) return 'Topic should start with /data/.'
    if (!/^https?:\/\//.test(form.webhook_url)) return 'Webhook URL must be http(s).'
    if (form.flow_batch_size <= 0) return 'Flow batch size must be > 0.'
    return null
  }

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const msg = validate()
    if (msg) {
      setError(msg)
      return
    }
    try {
      setSaving(true)
      setError(null)
      // empty strings -> undefined for nullable fields
      const payload: CreateClientPayload = {
        ...form,
        // Both password and client_credentials require username/password for Salesforce
        oauth_username: (form.oauth_grant_type === 'password' || form.oauth_grant_type === 'client_credentials') 
          ? form.oauth_username 
          : undefined,
        oauth_password: (form.oauth_grant_type === 'password' || form.oauth_grant_type === 'client_credentials') 
          ? form.oauth_password 
          : undefined,
        // pubsub_host: form.pubsub_host?.trim() || undefined,
        tenant_id: form.tenant_id?.trim() || undefined,
      }
      await createClient(payload)
      // backend should auto-start listener; we just go back to list
      navigate('/', { replace: true })
    } catch (e: any) {
      setError(e?.message ?? String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="max-w-3xl">
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-xl font-semibold">New Client</h1>
        <Link to="/" className="text-sm text-blue-600 hover:underline">← Back</Link>
      </div>

      {error && (
        <div className="mb-4 rounded border border-red-200 bg-red-50 p-3 text-red-700">
          {error}
        </div>
      )}

      <form onSubmit={onSubmit} className="space-y-5">
        <section className="rounded border bg-white p-4 space-y-4">
          <h2 className="font-medium">Basics</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <label className="block">
              <span className="text-sm">Client Name</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                value={form.client_name}
                onChange={(e) => update('client_name', e.target.value)} />
            </label>

            <label className="block">
              <span className="text-sm">Active</span><br/>
              <input type="checkbox" className="mt-2"
                checked={form.is_active}
                onChange={(e) => update('is_active', e.target.checked)} />
            </label>

            <label className="block md:col-span-2">
              <span className="text-sm">Topic Name</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                placeholder="/data/OpportunityChangeEvent"
                value={form.topic_name}
                onChange={(e) => update('topic_name', e.target.value)} />
            </label>

            <label className="block md:col-span-2">
              <span className="text-sm">Webhook URL</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                placeholder="https://example.com/webhook"
                value={form.webhook_url}
                onChange={(e) => update('webhook_url', e.target.value)} />
            </label>
          </div>
        </section>

        <section className="rounded border bg-white p-4 space-y-4">
          <h2 className="font-medium">Salesforce OAuth</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <label className="block">
              <span className="text-sm">Login URL</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                value={form.login_url}
                onChange={(e) => update('login_url', e.target.value)} />
              {form.oauth_grant_type === 'client_credentials' ? (
                <p className="text-xs text-gray-500 mt-1">Enter your Salesforce org URL (e.g., yourdomain.my.salesforce.com or yourdomain--sandbox.sandbox.my.salesforce.com)</p>
              ) : (
                <p className="text-xs text-gray-500 mt-1">Use: https://login.salesforce.com</p>
              )}
            </label>

            <label className="block">
              <span className="text-sm">Grant Type</span>
              <select className="mt-1 w-full rounded border px-3 py-2"
                value={form.oauth_grant_type}
                onChange={(e) => update('oauth_grant_type', e.target.value as Grant)}>
                <option value="password">password</option>
                <option value="client_credentials">client_credentials</option>
              </select>
            </label>

            <label className="block">
              <span className="text-sm">Client ID</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                value={form.oauth_client_id}
                onChange={(e) => update('oauth_client_id', e.target.value)} />
            </label>

            <label className="block">
              <span className="text-sm">Client Secret</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                value={form.oauth_client_secret}
                onChange={(e) => update('oauth_client_secret', e.target.value)} />
            </label>

            {(form.oauth_grant_type === 'password' || form.oauth_grant_type === 'client_credentials') && (
              <>
                <label className="block">
                  <span className="text-sm">Username</span>
                  <input className="mt-1 w-full rounded border px-3 py-2"
                    value={form.oauth_username ?? ''}
                    onChange={(e) => update('oauth_username', e.target.value)} />
                </label>

                <label className="block">
                  <span className="text-sm">Password (+ token if needed)</span>
                  <input className="mt-1 w-full rounded border px-3 py-2"
                    type="password"
                    value={form.oauth_password ?? ''}
                    onChange={(e) => update('oauth_password', e.target.value)} />
                </label>
              </>
            )}
          </div>
        </section>

        <section className="rounded border bg-white p-4 space-y-4">
          <h2 className="font-medium">Advanced</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* <label className="block">
              <span className="text-sm">Pub/Sub Host</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                placeholder="api.pubsub.salesforce.com:7443"
                value={form.pubsub_host ?? ''}
                onChange={(e) => update('pubsub_host', e.target.value)} />
            </label> */}

            <label className="block">
              <span className="text-sm">Tenant ID (optional)</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                value={form.tenant_id ?? ''}
                onChange={(e) => update('tenant_id', e.target.value)} />
            </label>

            <label className="block">
              <span className="text-sm">Flow batch size</span>
              <input className="mt-1 w-full rounded border px-3 py-2"
                type="number"
                min={1}
                value={form.flow_batch_size}
                onChange={(e) => update('flow_batch_size', Number(e.target.value))} />
            </label>
          </div>
        </section>

        <div className="flex items-center gap-3">
          <button
            type="submit"
            disabled={saving}
            className="rounded bg-black px-4 py-2 text-white text-sm disabled:opacity-50"
          >
            {saving ? 'Saving…' : 'Create Client'}
          </button>
          <Link to="/" className="text-sm text-neutral-600 hover:underline">Cancel</Link>
        </div>
      </form>
    </div>
  )
}
