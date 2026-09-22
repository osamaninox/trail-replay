import { Button, Card, CardBody, CardFooter, CardHeader, Divider, Input, Link } from '@heroui/react'
import { useState } from 'react'
import type { FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { ApiError } from '../api/client'
import { useAuth } from '../contexts/AuthContext'

export function LoginPage() {
  const { login } = useAuth()
  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await login(email, password)
      navigate('/')
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <Card className="w-full max-w-sm">
        <CardHeader className="flex flex-col items-start gap-1">
          <h1 className="text-xl font-bold">Trail Replay</h1>
          <p className="text-sm text-default-500">Sign in to your account</p>
        </CardHeader>
        <Divider />
        <form onSubmit={onSubmit}>
          <CardBody className="gap-4">
            <Input
              label="Email"
              type="email"
              value={email}
              onValueChange={setEmail}
              isRequired
              autoFocus
            />
            <Input
              label="Password"
              type="password"
              value={password}
              onValueChange={setPassword}
              isRequired
            />
            {error && <p className="text-sm text-danger">{error}</p>}
            <Button type="submit" color="primary" isLoading={loading} fullWidth>
              Sign in
            </Button>
          </CardBody>
        </form>
        <Divider />
        <CardFooter className="justify-center">
          <p className="text-sm text-default-500">
            No account? <Link href="/signup">Sign up</Link>
          </p>
        </CardFooter>
      </Card>
    </div>
  )
}
