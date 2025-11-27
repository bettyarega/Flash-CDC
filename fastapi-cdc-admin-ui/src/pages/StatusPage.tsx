import { useEffect, useState } from 'react'
import { getClientsStatus, type ClientStatus } from '../api/clients'
import { isLoggedIn } from '../api/session'

function StatusBadge({ status }: { status: string }) {
  const cls =
    status === 'running'
      ? 'bg-green-100 text-green-800'
      : status === 'starting'
      ? 'bg-blue-100 text-blue-800'
      : status === 'stopping'
      ? 'bg-amber-100 text-amber-800'
      : status === 'error'
      ? 'bg-red-100 text-red-800'
      : 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {status}
    </span>
  )
}

export default function StatusPage() {
  const [statuses, setStatuses] = useState<ClientStatus[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function refreshStatuses() {
    setLoading(true)
    setError(null)
    try {
      const data = await getClientsStatus()
      setStatuses(data)
    } catch (e: any) {
      setError(e.message ?? String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (!isLoggedIn()) return
    refreshStatuses()
    
    // Auto-refresh every 30 seconds
    const interval = setInterval(refreshStatuses, 30000)
    return () => clearInterval(interval)
  }, [])

  function formatDate(dateStr: string | null | undefined): string {
    if (!dateStr) return '—'
    try {
      return new Date(dateStr).toLocaleString()
    } catch {
      return dateStr
    }
  }

  return (
    <>
      {error && <div className="p-3 rounded bg-red-50 text-red-700 border border-red-200">{error}</div>}

      {loading ? (
        <p>Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded border border-neutral-200 bg-white">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="text-left border-b bg-neutral-50">
                <th className="p-2">ID</th>
                <th className="p-2">Client Name</th>
                <th className="p-2">Topic</th>
                <th className="p-2">Active</th>
                <th className="p-2">Listener Status</th>
                <th className="p-2">Running</th>
                {/* <th className="p-2">Events Received</th> */}
                {/* <th className="p-2">Last Error</th> */}
                <th className="p-2">Started At</th>
                {/* <th className="p-2">Last Activity</th> */}
                <th className="p-2">Fail Count</th>
              </tr>
            </thead>
            <tbody>
              {statuses.map((status) => (
                <tr key={status.id} className="border-b">
                  <td className="p-2">{status.id}</td>
                  <td className="p-2">{status.client_name}</td>
                  <td className="p-2">{status.topic_name}</td>
                  <td className="p-2">{status.is_active ? 'Yes' : 'No'}</td>
                  <td className="p-2">
                    <StatusBadge status={status.listener_status} />
                  </td>
                  <td className="p-2">{status.listener_running ? <span className="text-green-600">✓</span> : '—'}</td>
                  {/* <td className="p-2">{status.events_received ?? 0}</td> */}
                  {/* <td className="p-2">
                    {status.last_error ? (
                      <span className="text-red-600 text-xs" title={status.last_error}>
                        {status.last_error.length > 50 
                          ? status.last_error.substring(0, 50) + '...' 
                          : status.last_error}
                      </span>
                    ) : (
                      <span className="text-gray-400">—</span>
                    )}
                  </td> */}
                  <td className="p-2">{formatDate(status.started_at)}</td>
                  {/* <td className="p-2 text-xs text-gray-600">
                    {formatDate(status.last_beat)}
                  </td> */}
                  <td className="p-2">
                    {status.fail_count && status.fail_count > 0 ? (
                      <span className="text-red-600">{status.fail_count}</span>
                    ) : (
                      <span>0</span>
                    )}
                  </td>
                </tr>
              ))}
              {statuses.length === 0 && (
                <tr>
                  <td colSpan={8} className="p-4 text-center text-gray-500">
                    No clients found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

