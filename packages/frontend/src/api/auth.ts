import { apiFetch } from './client'

export interface User {
  id: string
  email: string
}

export interface AuthResult {
  token: string
  user: User
}

export function login(email: string, password: string): Promise<AuthResult> {
  return apiFetch<AuthResult>('/auth/login', {
    method: 'POST',
    body: { email, password },
  })
}

export function signup(email: string, password: string): Promise<AuthResult> {
  return apiFetch<AuthResult>('/auth/signup', {
    method: 'POST',
    body: { email, password },
  })
}
