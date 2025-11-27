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
    <div className="max-w-7xl mx-auto p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold mb-2">Client Status Monitor</h1>
        <p className="text-sm text-gray-600">Real-time monitoring of client listeners and event processing</p>
      </div>

      {error && (
        <div className="mb-4 rounded border border-red-200 bg-red-50 p-3 text-red-700">
          {error}
        </div>
      )}

      {loading ? (
        <p>Loading status...</p>
      ) : (
        <div className="overflow-x-auto rounded border border-neutral-200 bg-white">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="text-left border-b bg-neutral-50">
                <th className="p-3">ID</th>
                <th className="p-3">Client Name</th>
                <th className="p-3">Topic</th>
                <th className="p-3">Active</th>
                <th className="p-3">Listener Status</th>
                <th className="p-3">Running</th>
                {/* <th className="p-3">Events Received</th> */}
                {/* <th className="p-3">Last Error</th> */}
                <th className="p-3">Started At</th>
                {/* <th className="p-3">Last Activity</th> */}
                <th className="p-3">Fail Count</th>
              </tr>
            </thead>
            <tbody>
              {statuses.map((status) => (
                <tr key={status.id} className="border-b hover:bg-gray-50">
                  <td className="p-3">{status.id}</td>
                  <td className="p-3 font-medium">{status.client_name}</td>
                  <td className="p-3 text-gray-600">{status.topic_name}</td>
                  <td className="p-3">
                    {status.is_active ? (
                      <span className="text-green-600">Yes</span>
                    ) : (
                      <span className="text-gray-400">No</span>
                    )}
                  </td>
                  <td className="p-3">
                    <StatusBadge status={status.listener_status} />
                  </td>
                  <td className="p-3">
                    {status.listener_running ? (
                      <span className="text-green-600">✓</span>
                    ) : (
                      <span className="text-gray-400">—</span>
                    )}
                  </td>
                  {/* <td className="p-3">{status.events_received ?? 0}</td> */}
                  {/* <td className="p-3">
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
                  <td className="p-3 text-xs text-gray-600">
                    {formatDate(status.started_at)}
                  </td>
                  {/* <td className="p-3 text-xs text-gray-600">
                    {formatDate(status.last_beat)}
                  </td> */}
                  <td className="p-3">
                    {status.fail_count && status.fail_count > 0 ? (
                      <span className="text-red-600">{status.fail_count}</span>
                    ) : (
                      <span className="text-gray-400">0</span>
                    )}
                  </td>
                </tr>
              ))}
              {statuses.length === 0 && (
                <tr>
                  <td colSpan={11} className="p-4 text-center text-gray-500">
                    No clients found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      <div className="mt-4 text-xs text-gray-500">
        Status updates automatically every 30 seconds. Last updated: {new Date().toLocaleTimeString()}
      </div>
    </div>
  )
}

