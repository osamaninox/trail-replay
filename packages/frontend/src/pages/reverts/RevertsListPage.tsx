import {
  Button,
  Input,
  Modal,
  ModalBody,
  ModalContent,
  ModalFooter,
  ModalHeader,
  Pagination,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
  useDisclosure,
} from '@heroui/react'
import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ApiError } from '../../api/client'
import { createRevertJob, listRevertJobs } from '../../api/reverts'
import type { Paginated } from '../../api/wal'
import type { RevertJob } from '../../api/reverts'
import { StatusBadge } from '../../components/StatusBadge'

const PAGE_SIZE = 20

export function RevertsListPage() {
  const navigate = useNavigate()
  const [result, setResult] = useState<Paginated<RevertJob> | null>(null)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState('')
  const { isOpen, onOpen, onClose } = useDisclosure()

  const load = useCallback(async (p: number) => {
    setLoading(true)
    setError('')
    try {
      setResult(await listRevertJobs(p, PAGE_SIZE))
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to load revert jobs')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(page)
  }, [load, page])

  async function onCreate() {
    setCreateError('')
    const fromDate = new Date(from)
    const toDate = new Date(to)
    if (Number.isNaN(fromDate.getTime()) || Number.isNaN(toDate.getTime())) {
      setCreateError('Both from and to dates are required')
      return
    }
    if (fromDate >= toDate) {
      setCreateError('From must be before to')
      return
    }
    setCreating(true)
    try {
      const job = await createRevertJob(fromDate.toISOString(), toDate.toISOString())
      onClose()
      setFrom('')
      setTo('')
      navigate(`/reverts/${job.id}`)
    } catch (err) {
      setCreateError(err instanceof ApiError ? err.message : 'Failed to create revert job')
      setCreating(false)
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Revert Jobs</h1>
        <Button color="primary" onPress={onOpen}>
          New revert job
        </Button>
      </div>

      {error && <p className="text-sm text-danger">{error}</p>}

      <Table aria-label="Revert jobs">
        <TableHeader>
          <TableColumn>STATUS</TableColumn>
          <TableColumn>FROM</TableColumn>
          <TableColumn>TO</TableColumn>
          <TableColumn>PROGRESS</TableColumn>
          <TableColumn>CREATED</TableColumn>
        </TableHeader>
        <TableBody
          items={result?.data ?? []}
          isLoading={loading}
          loadingContent={<Spinner size="sm" />}
          emptyContent="No revert jobs yet"
        >
          {(job) => (
            <TableRow
              key={job.id}
              className="cursor-pointer"
              onClick={() => navigate(`/reverts/${job.id}`)}
            >
              <TableCell>
                <StatusBadge status={job.status} />
              </TableCell>
              <TableCell>{new Date(job.input_from).toLocaleString()}</TableCell>
              <TableCell>{new Date(job.input_to).toLocaleString()}</TableCell>
              <TableCell>
                {job.completed_count + job.failed_count}/{job.total_changes}
              </TableCell>
              <TableCell>{new Date(job.created_at).toLocaleString()}</TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      {result && result.total_pages > 1 && (
        <div className="flex justify-center">
          <Pagination page={page} total={result.total_pages} onChange={setPage} showControls />
        </div>
      )}

      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalContent>
          <ModalHeader>Create revert job</ModalHeader>
          <ModalBody className="gap-4">
            <Input
              label="From"
              type="datetime-local"
              value={from}
              onValueChange={setFrom}
              isRequired
              autoFocus
            />
            <Input
              label="To"
              type="datetime-local"
              value={to}
              onValueChange={setTo}
              isRequired
            />
            {createError && <p className="text-sm text-danger">{createError}</p>}
          </ModalBody>
          <ModalFooter>
            <Button variant="light" onPress={onClose}>
              Cancel
            </Button>
            <Button
              color="primary"
              onPress={onCreate}
              isLoading={creating}
              isDisabled={!from || !to}
            >
              Create
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </div>
  )
}
