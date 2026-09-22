import {
  Accordion,
  AccordionItem,
  Chip,
  Pagination,
  Snippet,
  Spinner,
} from '@heroui/react'
import { useCallback, useEffect, useState } from 'react'
import { ApiError } from '../api/client'
import { listWalTransactions } from '../api/wal'
import type { Paginated, WalTransaction } from '../api/wal'

const OP_COLORS: Record<string, 'success' | 'primary' | 'danger'> = {
  I: 'success',
  U: 'primary',
  D: 'danger',
}

const OP_LABELS: Record<string, string> = {
  I: 'INSERT',
  U: 'UPDATE',
  D: 'DELETE',
}

const PAGE_SIZE = 20

export function WalTransactionsPage() {
  const [result, setResult] = useState<Paginated<WalTransaction> | null>(null)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const load = useCallback(async (p: number) => {
    setLoading(true)
    setError('')
    try {
      setResult(await listWalTransactions(p, PAGE_SIZE))
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to load WAL transactions')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(page)
  }, [load, page])

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">WAL Transactions</h1>
        {result && (
          <span className="text-sm text-default-500">{result.total_count} total</span>
        )}
      </div>

      {error && <p className="text-sm text-danger">{error}</p>}

      {loading && !result ? (
        <div className="flex justify-center py-12">
          <Spinner />
        </div>
      ) : !result || result.data.length === 0 ? (
        <p className="text-sm text-default-500">No WAL transactions</p>
      ) : (
        <>
          <Accordion variant="splitted">
            {result.data.map((tx) => (
              <AccordionItem
                key={tx.id}
                title={
                  <div className="flex items-center gap-3">
                    <span className="font-medium">
                      {tx.source_db} / xid {tx.xid}
                    </span>
                    <Chip size="sm" variant="flat" color="default">
                      {tx.change_count} change(s)
                    </Chip>
                    <span className="text-sm text-default-400">
                      {new Date(tx.commit_ts).toLocaleString()}
                    </span>
                  </div>
                }
              >
                <div className="flex flex-col gap-3">
                  {tx.changes.map((change) => (
                    <div
                      key={change.id}
                      className="flex flex-col gap-2 rounded-md bg-default-100 p-3"
                    >
                      <div className="flex items-center gap-3">
                        <Chip
                          size="sm"
                          variant="flat"
                          color={OP_COLORS[change.op] ?? 'default'}
                        >
                          {OP_LABELS[change.op] ?? change.op}
                        </Chip>
                        <span className="text-sm font-medium">
                          {change.schema_name}.{change.table_name}
                        </span>
                        <Chip size="sm" variant="bordered" color="default">
                          {change.undo_status}
                        </Chip>
                      </div>
                      {change.changed_columns.length > 0 && (
                        <p className="text-xs text-default-500">
                          Columns: {change.changed_columns.join(', ')}
                        </p>
                      )}
                      <Snippet
                        size="sm"
                        variant="bordered"
                        hideSymbol
                        className="w-full"
                        codeString={change.forward_dml_sql}
                      >
                        <span className="whitespace-pre-wrap break-all">
                          {change.forward_dml_sql}
                        </span>
                      </Snippet>
                    </div>
                  ))}
                  {tx.changes.length === 0 && (
                    <p className="text-sm text-default-500">No changes recorded</p>
                  )}
                </div>
              </AccordionItem>
            ))}
          </Accordion>

          {result.total_pages > 1 && (
            <div className="flex justify-center">
              <Pagination
                page={page}
                total={result.total_pages}
                onChange={setPage}
                showControls
              />
            </div>
          )}
        </>
      )}
    </div>
  )
}
