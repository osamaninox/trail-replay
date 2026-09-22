import { apiFetch } from './client'

export interface SourceDatabase {
  id: string
  name: string
  host: string
  port: number
  dbname: string
  username: string
  sslmode: string
  created_at: string
  updated_at: string
}

export interface SourceDatabaseInput {
  name: string
  host: string
  port: number
  dbname: string
  username: string
  password?: string
  sslmode: string
}

export interface ConnectionTestResult {
  success: boolean
  latency_ms: number
  error?: string
}

export function listDatabases(): Promise<SourceDatabase[]> {
  return apiFetch<SourceDatabase[]>('/databases')
}

export function createDatabase(input: SourceDatabaseInput): Promise<SourceDatabase> {
  return apiFetch<SourceDatabase>('/databases', { method: 'POST', body: input })
}

export function getDatabase(id: string): Promise<SourceDatabase> {
  return apiFetch<SourceDatabase>(`/databases/${id}`)
}

export function updateDatabase(
  id: string,
  input: SourceDatabaseInput,
): Promise<SourceDatabase> {
  return apiFetch<SourceDatabase>(`/databases/${id}`, { method: 'PUT', body: input })
}

export function deleteDatabase(id: string): Promise<void> {
  return apiFetch<void>(`/databases/${id}`, { method: 'DELETE' })
}

export function testDatabaseConnection(id: string): Promise<ConnectionTestResult> {
  return apiFetch<ConnectionTestResult>(`/databases/${id}/test`, { method: 'POST' })
}
