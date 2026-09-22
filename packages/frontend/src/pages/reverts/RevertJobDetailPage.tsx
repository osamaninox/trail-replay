import {
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  Divider,
  Progress,
  Spinner,
} from '@heroui/react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ApiError } from '../../api/client'
import { cancelRevertJob, getRevertJob } from '../../api/reverts'
import type { RevertJob } from '../../api/reverts'
import { StatusBadge } from '../../components/StatusBadge'

const ACTIVE_STATUSES = new Set(['pending', 'in_progress', 'cancelling'])
const POLL_INTERVAL_MS = 2000

export function RevertJobDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [job, setJob] = useState<RevertJob | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [cancelling, setCancelling] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    if (!id) return
    try {
      const updated = await getRevertJob(id)
      setJob(updated)
      if (!ACTIVE_STATUSES.has(updated.status) && pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to load revert job')
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    } finally {
      setLoading(false)
    }
  }, [id])

  useEffect(() => {
    void load()
    pollRef.current = setInterval(() => void load(), POLL_INTERVAL_MS)
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [load])

  async function onCancel() {
    if (!id) return
    setCancelling(true)
    setError('')
    try {
      setJob(await cancelRevertJob(id))
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to cancel revert job')
    } finally {
      setCancelling(false)
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <Spinner />
      </div>
    )
  }
  if (!job) {
    return <p className="text-danger">{error || 'Revert job not found'}</p>
  }

  const processed = job.completed_count + job.failed_count
  const progressValue = job.total_changes > 0 ? (processed / job.total_changes) * 100 : 0
  const isActive = ACTIVE_STATUSES.has(job.status)
  const canCancel = job.status === 'pending' || job.status === 'in_progress'

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Revert Job</h1>
          <p className="text-sm text-default-500">ID: {job.id}</p>
        </div>
        <div className="flex items-center gap-3">
          <StatusBadge status={job.status} />
          <Button variant="light" onPress={() => navigate('/reverts')}>
            Back
          </Button>
        </div>
      </div>

      {error && <p className="text-sm text-danger">{error}</p>}

      {job.last_error && (
        <Alert color="danger" title="Last error">
          {job.last_error}
        </Alert>
      )}

      <Card>
        <CardHeader>
          <h2 className="text-lg font-semibold">Progress</h2>
        </CardHeader>
        <Divider />
        <CardBody className="gap-4">
          <Progress
            value={progressValue}
            color={job.status === 'failed' ? 'danger' : 'primary'}
            label={`${processed} of ${job.total_changes} changes processed`}
            showValueLabel
            formatOptions={{ style: 'percent', maximumFractionDigits: 0 }}
            isIndeterminate={isActive && job.total_changes === 0}
          />
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <div>
              <p className="text-xs text-default-500">Total changes</p>
              <p className="text-lg font-semibold">{job.total_changes}</p>
            </div>
            <div>
              <p className="text-xs text-default-500">Completed</p>
              <p className="text-lg font-semibold text-success">{job.completed_count}</p>
            </div>
            <div>
              <p className="text-xs text-default-500">Failed</p>
              <p className="text-lg font-semibold text-danger">{job.failed_count}</p>
            </div>
            <div>
              <p className="text-xs text-default-500">Status</p>
              <p className="text-lg font-semibold">{job.status.replace('_', ' ')}</p>
            </div>
          </div>
          {canCancel && (
            <Button
              color="danger"
              variant="flat"
              onPress={onCancel}
              isLoading={cancelling}
              className="self-start"
            >
              Cancel job
            </Button>
          )}
        </CardBody>
      </Card>

      <Card>
        <CardHeader>
          <h2 className="text-lg font-semibold">Details</h2>
        </CardHeader>
        <Divider />
        <CardBody>
          <dl className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div>
              <dt className="text-xs text-default-500">From</dt>
              <dd className="text-sm">{new Date(job.input_from).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-xs text-default-500">To</dt>
              <dd className="text-sm">{new Date(job.input_to).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-xs text-default-500">Created</dt>
              <dd className="text-sm">{new Date(job.created_at).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-xs text-default-500">Updated</dt>
              <dd className="text-sm">{new Date(job.updated_at).toLocaleString()}</dd>
            </div>
            {job.completed_at && (
              <div>
                <dt className="text-xs text-default-500">Completed at</dt>
                <dd className="text-sm">{new Date(job.completed_at).toLocaleString()}</dd>
              </div>
            )}
          </dl>
        </CardBody>
      </Card>
    </div>
  )
}
