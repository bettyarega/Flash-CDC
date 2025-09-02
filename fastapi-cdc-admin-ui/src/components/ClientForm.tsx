// src/components/ClientForm.tsx
import { useMemo, useState } from 'react'
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
    login_url: 'https://login.salesforce.com',
    oauth_grant_type: 'password',
    oauth_client_id: '',
    oauth_client_secret: '',
    oauth_username: '',
    oauth_password: '',
    topic_name: '',
    webhook_url: '',
    pubsub_host: 'api.pubsub.salesforce.com:7443',
    flow_batch_size: 100,
    is_active: true,
    ...initial,
  })

  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null)
  const [testError, setTestError] = useState<string | null>(null)

  const isPasswordGrant = (values.oauth_grant_type ?? 'password') === 'password'

  function onChange<K extends keyof Client>(key: K, val: any) {
    setValues(v => ({ ...v, [key]: val }))
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSaving(true)
    try {
      await onSubmit(values)
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
        pubsub_host: values.pubsub_host ?? undefined,
        tenant_id: (values as any).tenant_id ?? undefined,
        check_topic: !!values.topic_name, // only check topic if user filled one
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

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <TextInput label="Client Name" value={values.client_name ?? ''} onChange={v => onChange('client_name', v)} required />

        <TextInput label="Login URL" value={values.login_url ?? ''} onChange={v => onChange('login_url', v)} required />

        <Select
          label="OAuth Grant Type"
          value={values.oauth_grant_type ?? 'password'}
          onChange={v => onChange('oauth_grant_type', v)}
          options={[
            { value: 'password', label: 'password' },
            { value: 'client_credentials', label: 'client_credentials' },
          ]}
        />

        <TextInput label="Client ID" value={values.oauth_client_id ?? ''} onChange={v => onChange('oauth_client_id', v)} required />

        {/* CHANGED: plain text (no masking) */}
        <TextInput
          label="Client Secret"
          type="text"
          autoComplete="off"
          value={values.oauth_client_secret ?? ''}
          onChange={v => onChange('oauth_client_secret', v)}
        />

        {isPasswordGrant && (
          <>
            <TextInput label="Username" value={values.oauth_username ?? ''} onChange={v => onChange('oauth_username', v)} />

            {/* CHANGED: plain text (no masking) */}
            <TextInput
              label="Password"
              type="text"
              autoComplete="off"
              value={values.oauth_password ?? ''}
              onChange={v => onChange('oauth_password', v)}
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

        <TextInput
          label="Pub/Sub Host"
          value={values.pubsub_host ?? ''}
          onChange={v => onChange('pubsub_host', v)}
          placeholder="api.pubsub.salesforce.com:7443"
        />

        <NumberInput label="Flow Batch Size" value={values.flow_batch_size ?? 100} onChange={v => onChange('flow_batch_size', v)} min={1} />
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
