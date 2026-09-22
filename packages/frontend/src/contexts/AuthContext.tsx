import { createContext, useContext, useState } from 'react'
import type { ReactNode } from 'react'
import * as authApi from '../api/auth'
import type { User } from '../api/auth'
import { clearToken, getToken, setToken } from '../api/client'

const USER_KEY = 'trail_replay_user'

interface AuthContextValue {
  user: User | null
  isAuthenticated: boolean
  login: (email: string, password: string) => Promise<void>
  signup: (email: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

function loadUser(): User | null {
  if (!getToken()) {
    return null
  }
  const raw = localStorage.getItem(USER_KEY)
  if (!raw) {
    return null
  }
  try {
    return JSON.parse(raw) as User
  } catch {
    return null
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(loadUser)

  async function handleAuth(fn: () => Promise<authApi.AuthResult>) {
    const result = await fn()
    setToken(result.token)
    localStorage.setItem(USER_KEY, JSON.stringify(result.user))
    setUser(result.user)
  }

  const value: AuthContextValue = {
    user,
    isAuthenticated: user !== null,
    login: (email, password) => handleAuth(() => authApi.login(email, password)),
    signup: (email, password) => handleAuth(() => authApi.signup(email, password)),
    logout: () => {
      clearToken()
      localStorage.removeItem(USER_KEY)
      setUser(null)
    },
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return ctx
}
