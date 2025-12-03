// src/components/ClientForm.tsx
import { useEffect, useState } from 'react'
import type { Client } from '../types'
import { testConnection, type TestConnectionPayload, type TestConnectionResult } from '../api/clients'

type Props = {
  mode: 'create' | 'edit'
  initial?: Partial<Client>
  onSubmit: (values: Partial<Client>) => Promise<void> | void
  onCancel: () => void
}

export default function ClientForm({ mode, initial, onSubmit, onCancel }: Props) {
  const [values, setValues] = useState<Partial<Client>>({
    client_name: '',
    login_url: '',
    oauth_grant_type: 'password',
    oauth_client_id: '',
    oauth_client_secret: '',
    oauth_username: '',
    oauth_password: '',
    topic_name: '',
    webhook_url: '',
    // pubsub_host: 'api.pubsub.salesforce.com:7443',
    tenant_id: '',          // NEW: present in DB
    flow_batch_size: 100,
    is_active: true,
    ...initial,             // first paint will use whatever parent already has
  })

  // Hydrate once when edit modal opens and the parent finishes fetching full row (w/ secrets)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => {
    if (mode === 'edit' && initial && !hydrated) {
      setValues(v => ({ ...v, ...initial }))
      setHydrated(true)
    }
  }, [mode, initial, hydrated])
  
  // Toggle field visibility (edit mode only)
  function toggleReveal(fieldName: string) {
    setRevealedFields(prev => {
      const next = new Set(prev)
      if (next.has(fieldName)) {
        next.delete(fieldName)
      } else {
        next.add(fieldName)
      }
      return next
    })
  }
  
  // Check if a field should be masked (edit mode + has value + not revealed)
  function isMasked(fieldName: string, value: string | undefined): boolean {
    if (mode !== 'edit') return false
    if (!value || value === '') return false
    return !revealedFields.has(fieldName)
  }

  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null)
  const [testError, setTestError] = useState<string | null>(null)
  
  // Track which sensitive fields are revealed (edit mode only)
  const [revealedFields, setRevealedFields] = useState<Set<string>>(new Set())

  const isPasswordGrant = (values.oauth_grant_type ?? 'password') === 'password'
  // Both password and client_credentials require username/password for Salesforce
  const showUsernamePassword = isPasswordGrant || values.oauth_grant_type === 'client_credentials'

  function onChange<K extends keyof Client>(key: K, val: any) {
    setValues(v => ({ ...v, [key]: val }))
  }

  function onGrantTypeChange(v: string) {
    // Both grant types require username/password, so don't clear them
    setValues(s => ({ ...s, oauth_grant_type: v as any }))
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    setError(null)
    try {
      await onSubmit(values)
    } catch (err: any) {
      setError(err?.message ?? String(err))
      throw err // Re-throw so parent can handle if needed
    } finally {
      setSaving(false)
    }
  }

  async function handleTestConnection() {
    setTesting(true)
    setTestError(null)
    setTestResult(null)
    try {
      const payload: TestConnectionPayload = {
        login_url: values.login_url!,
        oauth_grant_type: values.oauth_grant_type!,
        oauth_client_id: values.oauth_client_id!,
        oauth_client_secret: values.oauth_client_secret ?? '',
        oauth_username: values.oauth_username ?? undefined,
        oauth_password: values.oauth_password ?? undefined,
        topic_name: values.topic_name ?? undefined,
        // pubsub_host: values.pubsub_host ?? undefined,
        tenant_id: (values as any).tenant_id ?? undefined, // NEW
        check_topic: !!values.topic_name,
      }
      const res = await testConnection(payload)
      setTestResult(res)
      if (!res.ok) {
        setTestError(formatTestError(res))
      }
    } catch (e: any) {
      setTestError(e?.message ?? String(e))
    } finally {
      setTesting(false)
    }
  }

  function formatTestError(res: TestConnectionResult): string {
    if (!res.auth?.ok) return `Auth failed: ${res.auth?.error ?? 'unknown error'}`
    if (res.topic && !res.topic.ok) {
      const code = res.topic.code ? `[${res.topic.code}] ` : ''
      return `Topic check failed: ${code}${res.topic.error ?? 'Unknown'}`
    }
    return 'Unknown error'
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <h2 className="text-lg font-semibold">{mode === 'create' ? 'New Client' : 'Edit Client'}</h2>

      {error && (
        <div className="rounded border border-red-300 bg-red-50 p-3 text-red-700">
          <div className="font-medium mb-1">Error:</div>
          <div className="text-sm whitespace-pre-line">{error}</div>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <TextInput label="Client Name" value={values.client_name ?? ''} onChange={v => onChange('client_name', v)} required />

        <div>
          <TextInput label="Login URL" value={values.login_url ?? ''} onChange={v => onChange('login_url', v)} required />
          {values.oauth_grant_type === 'client_credentials' ? (
            <p className="text-xs text-gray-500 mt-1">Enter your Salesforce org URL (e.g., yourdomain.my.salesforce.com or yourdomain--sandbox.sandbox.my.salesforce.com)</p>
          ) : (
            <p className="text-xs text-gray-500 mt-1">Use: https://login.salesforce.com</p>
          )}
        </div>

        <Select
          label="OAuth Grant Type"
          value={values.oauth_grant_type ?? 'password'}
          onChange={onGrantTypeChange}
          options={[
            { value: 'password', label: 'password' },
            { value: 'client_credentials', label: 'client_credentials' },
          ]}
        />

        <MaskedTextInput
          label="Client ID"
          value={values.oauth_client_id ?? ''}
          onChange={v => onChange('oauth_client_id', v)}
          isMasked={isMasked('oauth_client_id', values.oauth_client_id ?? undefined)}
          onToggleReveal={() => toggleReveal('oauth_client_id')}
          fieldName="oauth_client_id"
          required
        />

        <MaskedTextInput
          label="Client Secret"
          value={values.oauth_client_secret ?? ''}
          onChange={v => onChange('oauth_client_secret', v)}
          isMasked={isMasked('oauth_client_secret', values.oauth_client_secret)}
          onToggleReveal={() => toggleReveal('oauth_client_secret')}
          fieldName="oauth_client_secret"
        />

        {showUsernamePassword && (
          <>
            <MaskedTextInput
              label="Username"
              value={values.oauth_username ?? ''}
              onChange={v => onChange('oauth_username', v)}
              isMasked={isMasked('oauth_username', values.oauth_username ?? undefined)}
              onToggleReveal={() => toggleReveal('oauth_username')}
              fieldName="oauth_username"
            />
            <MaskedTextInput
              label="Password"
              value={values.oauth_password ?? ''}
              onChange={v => onChange('oauth_password', v)}
              isMasked={isMasked('oauth_password', values.oauth_password ?? undefined)}
              onToggleReveal={() => toggleReveal('oauth_password')}
              fieldName="oauth_password"
            />
          </>
        )}

        <TextInput
          label="Topic Name"
          value={values.topic_name ?? ''}
          onChange={v => onChange('topic_name', v)}
          placeholder="/data/OpportunityChangeEvent"
        />

        <TextInput label="Webhook URL" value={values.webhook_url ?? ''} onChange={v => onChange('webhook_url', v)} />

        {/* <TextInput
          label="Pub/Sub Host"
          value={values.pubsub_host ?? ''}
          onChange={v => onChange('pubsub_host', v)}
          placeholder="api.pubsub.salesforce.com:7443"
        /> */}

        {/* NEW: Tenant Id (optional) */}
        {/* <TextInput
          label="Tenant ID (Org Id)"
          value={(values as any).tenant_id ?? ''}
          onChange={v => onChange('tenant_id' as any, v)}
          placeholder="00Dxxxxxxxxxxxx"
        /> */}

        {/* <NumberInput
          label="Flow Batch Size"
          value={values.flow_batch_size ?? 100}
          onChange={v => onChange('flow_batch_size', v)}
          min={1}
        /> */}

        {/* NEW: Active toggle */}
        <Checkbox
          label="Active"
          checked={!!values.is_active}
          onChange={(v) => onChange('is_active', v)}
        />
      </div>

      <div className="flex items-center gap-3 pt-2">
        <button type="button" onClick={handleTestConnection} disabled={testing} className="px-3 py-2 rounded border">
          {testing ? 'Testing…' : 'Test Connection'}
        </button>
        <button type="submit" disabled={saving} className="px-3 py-2 rounded bg-black text-white">
          {saving ? 'Saving…' : mode === 'create' ? 'Save' : 'Save changes'}
        </button>
        <button type="button" onClick={onCancel} className="px-3 py-2 rounded border">
          Cancel
        </button>
      </div>

      {/* Inline result */}
      {testResult && (
        <div
          className={`mt-2 rounded border p-3 ${
            testResult.ok ? 'border-green-300 bg-green-50 text-green-700' : 'border-red-300 bg-red-50 text-red-700'
          }`}
        >
          <div className="font-medium mb-1">{testResult.ok ? 'Test passed' : 'Test failed'}</div>
          <ul className="text-sm space-y-1">
            <li>
              <span className="font-semibold">Auth:</span>{' '}
              {testResult.auth.ok
                ? `OK (org=${testResult.auth.org_id ?? '—'}, instance=${testResult.auth.instance_url ?? '—'})`
                : `ERROR: ${testResult.auth.error ?? 'unknown'}`}
            </li>
            {values.topic_name && (
              <li>
                <span className="font-semibold">Topic:</span>{' '}
                {testResult.topic?.ok
                  ? `OK (schema=${testResult.topic?.schema_id ?? '—'})`
                  : `ERROR${testResult.topic?.code ? ` [${testResult.topic.code}]` : ''}: ${
                      testResult.topic?.error ?? 'unknown'
                    }`}
              </li>
            )}
          </ul>
        </div>
      )}

      {testError && <div className="mt-2 rounded border border-red-300 bg-red-50 p-3 text-red-700">{testError}</div>}
    </form>
  )
}

function TextInput(props: {
  label: string
  value: string
  onChange: (v: string) => void
  type?: string
  required?: boolean
  placeholder?: string
  autoComplete?: string
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-sm text-neutral-600">{props.label}</span>
      <input
        type={props.type ?? 'text'}
        className="rounded border px-3 py-2"
        value={props.value}
        required={props.required}
        placeholder={props.placeholder}
        autoComplete={props.autoComplete}
        onChange={(e) => props.onChange(e.target.value)}
      />
    </label>
  )
}

function NumberInput(props: { label: string; value: number; onChange: (v: number) => void; min?: number }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-sm text-neutral-600">{props.label}</span>
      <input
        type="number"
        min={props.min}
        className="rounded border px-3 py-2"
        value={props.value}
        onChange={(e) => props.onChange(Number(e.target.value))}
      />
    </label>
  )
}

function Select(props: { label: string; value: string; onChange: (v: string) => void; options: { value: string; label: string }[] }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-sm text-neutral-600">{props.label}</span>
      <select className="rounded border px-3 py-2" value={props.value} onChange={(e) => props.onChange(e.target.value)}>
        {props.options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </label>
  )
}

function Checkbox(props: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="flex items-center gap-2">
      <input
        type="checkbox"
        checked={props.checked}
        onChange={(e) => props.onChange(e.target.checked)}
        className="h-4 w-4"
      />
      <span className="text-sm text-neutral-600">{props.label}</span>
    </label>
  )
}

// Eye icon for show
function EyeIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className={className}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M2.036 12.322a1.012 1.012 0 0 1 0-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178Z"
      />
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z"
      />
    </svg>
  )
}

// Eye slash icon for hide
function EyeSlashIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className={className}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M3.98 8.223A10.477 10.477 0 0 0 1.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.451 10.451 0 0 1 12 4.5c4.756 0 8.773 3.162 10.065 7.498a10.522 10.522 0 0 1-4.293 5.774M6.228 6.228 3 3m3.228 3.228 3.65 3.65m7.894 7.894L21 21m-3.228-3.228-3.65-3.65m0 0a3 3 0 1 0-4.243-4.243m4.242 4.242L9.88 9.88"
      />
    </svg>
  )
}

function MaskedTextInput(props: {
  label: string
  value: string
  onChange: (v: string) => void
  isMasked: boolean
  onToggleReveal: () => void
  fieldName: string
  required?: boolean
  placeholder?: string
}) {
  // When masked, use password type (browser will show dots)
  // When revealed, use text type (shows actual value)
  const inputType = props.isMasked ? 'password' : 'text'

  return (
    <label className="flex flex-col gap-1">
      <span className="text-sm text-neutral-600">{props.label}</span>
      <div className="relative">
        <input
          type={inputType}
          className="rounded border px-3 py-2 pr-10 w-full"
          value={props.value}
          onChange={(e) => props.onChange(e.target.value)}
          required={props.required}
          placeholder={props.placeholder}
          autoComplete="off"
        />
        {props.value && props.value !== '' && (
          <button
            type="button"
            onClick={props.onToggleReveal}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-700 focus:outline-none p-1"
            title={props.isMasked ? 'Show' : 'Hide'}
          >
            {props.isMasked ? (
              <EyeIcon className="w-5 h-5" />
            ) : (
              <EyeSlashIcon className="w-5 h-5" />
            )}
          </button>
        )}
      </div>
    </label>
  )
}
