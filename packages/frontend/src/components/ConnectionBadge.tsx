import { Chip } from '@heroui/react'
import type { ConnectionTestResult } from '../api/databases'

export function ConnectionBadge({ result }: { result: ConnectionTestResult | undefined }) {
  if (!result) {
    return (
      <Chip size="sm" variant="flat" color="default">
        not tested
      </Chip>
    )
  }
  if (result.success) {
    return (
      <Chip size="sm" variant="flat" color="success">
        connected ({result.latency_ms}ms)
      </Chip>
    )
  }
  return (
    <Chip size="sm" variant="flat" color="danger">
      unreachable
    </Chip>
  )
}
