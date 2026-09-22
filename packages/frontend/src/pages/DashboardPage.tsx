import { Card, CardBody, CardHeader, Divider, Spinner } from '@heroui/react'
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { listRevertJobs } from '../api/reverts'
import type { RevertJob } from '../api/reverts'
import { listWalTransactions } from '../api/wal'
import { StatusBadge } from '../components/StatusBadge'

export function DashboardPage() {
  const navigate = useNavigate()
  const [walCount, setWalCount] = useState<number | null>(null)
  const [recentReverts, setRecentReverts] = useState<RevertJob[] | null>(null)
  const [revertTotal, setRevertTotal] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      setLoading(true)
      // Each request settles independently — a missing WAL database should not
      // blank out the whole dashboard.
      const [wal, reverts] = await Promise.allSettled([
        listWalTransactions(1, 1),
        listRevertJobs(1, 5),
      ])
      if (wal.status === 'fulfilled') setWalCount(wal.value.total_count)
      if (reverts.status === 'fulfilled') {
        setRecentReverts(reverts.value.data)
        setRevertTotal(reverts.value.total_count)
      }
      setLoading(false)
    }
    void load()
  }, [])

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <Spinner />
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-2xl font-bold">Dashboard</h1>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <Card isPressable onPress={() => navigate('/wal')}>
          <CardBody>
            <p className="text-xs text-default-500">WAL transactions</p>
            <p className="text-3xl font-bold">{walCount ?? '—'}</p>
          </CardBody>
        </Card>
        <Card isPressable onPress={() => navigate('/reverts')}>
          <CardBody>
            <p className="text-xs text-default-500">Revert jobs</p>
            <p className="text-3xl font-bold">{revertTotal ?? '—'}</p>
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <h2 className="text-lg font-semibold">Recent revert jobs</h2>
        </CardHeader>
        <Divider />
        <CardBody>
          {recentReverts && recentReverts.length > 0 ? (
            <div className="flex flex-col gap-2">
              {recentReverts.map((job) => (
                <button
                  key={job.id}
                  onClick={() => navigate(`/reverts/${job.id}`)}
                  className="flex items-center justify-between rounded-md bg-default-100 p-3 text-left hover:bg-default-200"
                >
                  <div className="flex items-center gap-3">
                    <StatusBadge status={job.status} />
                    <span className="text-sm">
                      {new Date(job.input_from).toLocaleString()} →{' '}
                      {new Date(job.input_to).toLocaleString()}
                    </span>
                  </div>
                  <span className="text-sm text-default-500">
                    {job.completed_count + job.failed_count}/{job.total_changes}
                  </span>
                </button>
              ))}
            </div>
          ) : (
            <p className="text-sm text-default-500">No revert jobs yet</p>
          )}
        </CardBody>
      </Card>
    </div>
  )
}
