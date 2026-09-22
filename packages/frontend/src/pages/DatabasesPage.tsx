import {
  Button,
  Input,
  Modal,
  ModalBody,
  ModalContent,
  ModalFooter,
  ModalHeader,
  Select,
  SelectItem,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableColumn,
  TableHeader,
  TableRow,
  Tooltip,
  useDisclosure,
} from '@heroui/react'
import { useCallback, useEffect, useState } from 'react'
import { ApiError } from '../api/client'
import {
  createDatabase,
  deleteDatabase,
  listDatabases,
  testDatabaseConnection,
  updateDatabase,
} from '../api/databases'
import type {
  ConnectionTestResult,
  SourceDatabase,
  SourceDatabaseInput,
} from '../api/databases'
import { ConnectionBadge } from '../components/ConnectionBadge'

const SSL_MODES = ['disable', 'require', 'verify-ca', 'verify-full'] as const

const EMPTY_FORM: SourceDatabaseInput = {
  name: '',
  host: '',
  port: 5432,
  dbname: '',
  username: '',
  password: '',
  sslmode: 'disable',
}

export function DatabasesPage() {
  const [databases, setDatabases] = useState<SourceDatabase[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const [editing, setEditing] = useState<SourceDatabase | null>(null)
  const [form, setForm] = useState<SourceDatabaseInput>(EMPTY_FORM)
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState('')
  const { isOpen, onOpen, onClose } = useDisclosure()

  const [testResults, setTestResults] = useState<Record<string, ConnectionTestResult>>({})
  const [testing, setTesting] = useState<Record<string, boolean>>({})
  const [deleting, setDeleting] = useState<Record<string, boolean>>({})

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      setDatabases(await listDatabases())
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to load databases')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  function openCreate() {
    setEditing(null)
    setForm(EMPTY_FORM)
    setFormError('')
    onOpen()
  }

  function openEdit(db: SourceDatabase) {
    setEditing(db)
    setForm({
      name: db.name,
      host: db.host,
      port: db.port,
      dbname: db.dbname,
      username: db.username,
      password: '',
      sslmode: db.sslmode,
    })
    setFormError('')
    onOpen()
  }

  async function onSave() {
    setSaving(true)
    setFormError('')
    try {
      if (editing) {
        // Omit password when unchanged so the existing one is kept.
        const input = form.password ? form : { ...form, password: undefined }
        await updateDatabase(editing.id, input)
      } else {
        await createDatabase(form)
      }
      onClose()
      await load()
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : 'Failed to save database')
    } finally {
      setSaving(false)
    }
  }

  async function onTest(db: SourceDatabase) {
    setTesting((prev) => ({ ...prev, [db.id]: true }))
    try {
      const result = await testDatabaseConnection(db.id)
      setTestResults((prev) => ({ ...prev, [db.id]: result }))
    } catch (err) {
      setTestResults((prev) => ({
        ...prev,
        [db.id]: {
          success: false,
          latency_ms: 0,
          error: err instanceof ApiError ? err.message : 'Test failed',
        },
      }))
    } finally {
      setTesting((prev) => ({ ...prev, [db.id]: false }))
    }
  }

  async function onDelete(db: SourceDatabase) {
    setDeleting((prev) => ({ ...prev, [db.id]: true }))
    setError('')
    try {
      await deleteDatabase(db.id)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to delete database')
    } finally {
      setDeleting((prev) => ({ ...prev, [db.id]: false }))
    }
  }

  const formValid =
    form.name && form.host && form.dbname && form.username && (editing !== null || form.password)

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Source Databases</h1>
        <Button color="primary" onPress={openCreate}>
          Add database
        </Button>
      </div>

      {error && <p className="text-sm text-danger">{error}</p>}

      <Table aria-label="Source databases">
        <TableHeader>
          <TableColumn>NAME</TableColumn>
          <TableColumn>HOST</TableColumn>
          <TableColumn>DATABASE</TableColumn>
          <TableColumn>USER</TableColumn>
          <TableColumn>SSL</TableColumn>
          <TableColumn>CONNECTION</TableColumn>
          <TableColumn>ACTIONS</TableColumn>
        </TableHeader>
        <TableBody
          items={databases}
          isLoading={loading}
          loadingContent={<Spinner size="sm" />}
          emptyContent="No source databases configured"
        >
          {(db) => (
            <TableRow key={db.id}>
              <TableCell>{db.name}</TableCell>
              <TableCell>
                {db.host}:{db.port}
              </TableCell>
              <TableCell>{db.dbname}</TableCell>
              <TableCell>{db.username}</TableCell>
              <TableCell>{db.sslmode}</TableCell>
              <TableCell>
                {testing[db.id] ? (
                  <Spinner size="sm" />
                ) : (
                  <ConnectionBadge result={testResults[db.id]} />
                )}
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  <Tooltip content="Test connection">
                    <Button size="sm" variant="flat" onPress={() => void onTest(db)}>
                      Test
                    </Button>
                  </Tooltip>
                  <Button size="sm" variant="flat" onPress={() => openEdit(db)}>
                    Edit
                  </Button>
                  <Button
                    size="sm"
                    color="danger"
                    variant="flat"
                    isLoading={deleting[db.id]}
                    onPress={() => void onDelete(db)}
                  >
                    Delete
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalContent>
          <ModalHeader>{editing ? 'Edit database' : 'Add database'}</ModalHeader>
          <ModalBody className="gap-4">
            <Input
              label="Name"
              value={form.name}
              onValueChange={(v) => setForm({ ...form, name: v })}
              isRequired
              autoFocus
            />
            <div className="flex gap-3">
              <Input
                label="Host"
                value={form.host}
                onValueChange={(v) => setForm({ ...form, host: v })}
                isRequired
                className="flex-1"
              />
              <Input
                label="Port"
                type="number"
                value={String(form.port)}
                onValueChange={(v) => setForm({ ...form, port: Number.parseInt(v, 10) || 5432 })}
                className="w-28"
              />
            </div>
            <Input
              label="Database name"
              value={form.dbname}
              onValueChange={(v) => setForm({ ...form, dbname: v })}
              isRequired
            />
            <Input
              label="Username"
              value={form.username}
              onValueChange={(v) => setForm({ ...form, username: v })}
              isRequired
            />
            <Input
              label={editing ? 'Password (leave blank to keep current)' : 'Password'}
              type="password"
              value={form.password ?? ''}
              onValueChange={(v) => setForm({ ...form, password: v })}
              isRequired={!editing}
            />
            <Select
              label="SSL mode"
              selectedKeys={[form.sslmode]}
              onChange={(e) => setForm({ ...form, sslmode: e.target.value })}
            >
              {SSL_MODES.map((mode) => (
                <SelectItem key={mode}>{mode}</SelectItem>
              ))}
            </Select>
            {formError && <p className="text-sm text-danger">{formError}</p>}
          </ModalBody>
          <ModalFooter>
            <Button variant="light" onPress={onClose}>
              Cancel
            </Button>
            <Button color="primary" onPress={onSave} isLoading={saving} isDisabled={!formValid}>
              {editing ? 'Save changes' : 'Add database'}
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </div>
  )
}
