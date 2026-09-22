import { apiFetch } from './client'

export interface Paginated<T> {
  data: T[]
  page: number
  page_size: number
  total_count: number
  total_pages: number
}

export interface WalTransaction {
  id: number
  source_slot: string
  source_db: string
  xid: number
  commit_lsn?: string
  commit_ts: string
  change_count: number
  ingested_at: string
  changes: WalChange[]
}

export interface WalChange {
  id: number
  transaction_id: number
  change_seq_in_txn: number
  schema_name: string
  table_name: string
  op: 'I' | 'U' | 'D'
  changed_columns: string[]
  forward_dml_sql: string
  reverse_dml_sql: string
  undo_status: string
  created_at: string
}

export function listWalTransactions(
  page = 1,
  pageSize = 20,
): Promise<Paginated<WalTransaction>> {
  return apiFetch<Paginated<WalTransaction>>(
    `/wal/transactions?page=${page}&page_size=${pageSize}`,
  )
}
