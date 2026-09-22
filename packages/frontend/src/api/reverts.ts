import { apiFetch } from './client'
import type { Paginated } from './wal'

export type RevertJobStatus =
  | 'pending'
  | 'in_progress'
  | 'completed'
  | 'failed'
  | 'cancelling'
  | 'cancelled'

export interface RevertJob {
  id: string
  status: RevertJobStatus
  input_from: string
  input_to: string
  total_changes: number
  completed_count: number
  failed_count: number
  last_error?: string
  created_at: string
  updated_at: string
  completed_at?: string
}

export function createRevertJob(from: string, to: string): Promise<RevertJob> {
  return apiFetch<RevertJob>('/reverts/jobs', {
    method: 'POST',
    body: { from, to },
  })
}

export function listRevertJobs(page = 1, pageSize = 20): Promise<Paginated<RevertJob>> {
  return apiFetch<Paginated<RevertJob>>(`/reverts/jobs?page=${page}&page_size=${pageSize}`)
}

export function getRevertJob(id: string): Promise<RevertJob> {
  return apiFetch<RevertJob>(`/reverts/jobs/${id}`)
}

export function cancelRevertJob(id: string): Promise<RevertJob> {
  return apiFetch<RevertJob>(`/reverts/jobs/${id}`, { method: 'DELETE' })
}
