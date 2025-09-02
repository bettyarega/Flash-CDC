import { useEffect, useState } from 'react'
import { listClients } from '../api/clients'
import type { Client } from '../types'
import { Link } from 'react-router-dom'

export default function ClientsList() {
  const [rows, setRows] = useState<Client[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        setLoading(true)
        setRows(await listClients())
      } catch (e: any) {
        setError(e?.message ?? String(e))
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  return (
    <>
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Clients</h1>
          <p className="text-sm text-neutral-500">Configured Salesforce listeners</p>
        </div>
        <Link to="/clients/new" className="rounded bg-black px-3 py-2 text-white text-sm">+ New Client</Link>
      </div>

      {loading && <div className="text-neutral-600">Loading…</div>}
      {error && <div className="rounded border border-red-200 bg-red-50 p-3 text-red-700">{error}</div>}

      {!loading && !error && rows.length === 0 && (
        <div className="rounded border border-neutral-200 bg-white p-6 text-neutral-600">
          No clients yet. Click “New Client” to add one.
        </div>
      )}

      {!loading && !error && rows.length > 0 && (
        <div className="overflow-x-auto rounded border border-neutral-200 bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-neutral-100 text-neutral-700">
              <tr>
                <th className="px-3 py-2 text-left">ID</th>
                <th className="px-3 py-2 text-left">Name</th>
                <th className="px-3 py-2 text-left">Topic</th>
                <th className="px-3 py-2 text-left">Webhook</th>
                <th className="px-3 py-2 text-left">Active</th>
                <th className="px-3 py-2 text-left">Flow</th>
                <th className="px-3 py-2 text-left">Updated</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((c) => (
                <tr key={c.id} className="border-t">
                  <td className="px-3 py-2">{c.id}</td>
                  <td className="px-3 py-2">{c.client_name}</td>
                  <td className="px-3 py-2">{c.topic_name}</td>
                  <td className="px-3 py-2 truncate max-w-[280px]">
                    <a className="text-blue-600 hover:underline" href={c.webhook_url} target="_blank">
                      {c.webhook_url}
                    </a>
                  </td>
                  <td className="px-3 py-2">
                    <span className={c.is_active ? 'text-green-700' : 'text-neutral-500'}>
                      {c.is_active ? 'Yes' : 'No'}
                    </span>
                  </td>
                  <td className="px-3 py-2">{c.flow_batch_size}</td>
                  <td className="px-3 py-2">{c.updated_at ? new Date(c.updated_at).toLocaleString() : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}
