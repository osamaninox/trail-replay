import { Chip } from '@heroui/react'
import type { RevertJobStatus } from '../api/reverts'

const STATUS_COLORS: Record<
  RevertJobStatus,
  'default' | 'primary' | 'success' | 'danger' | 'warning'
> = {
  pending: 'default',
  in_progress: 'primary',
  completed: 'success',
  failed: 'danger',
  cancelling: 'warning',
  cancelled: 'warning',
}

export function StatusBadge({ status }: { status: RevertJobStatus }) {
  return (
    <Chip size="sm" variant="flat" color={STATUS_COLORS[status] ?? 'default'}>
      {status.replace('_', ' ')}
    </Chip>
  )
}
