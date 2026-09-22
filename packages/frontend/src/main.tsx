import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { HeroUIProvider } from '@heroui/react'
import { BrowserRouter, useNavigate } from 'react-router-dom'
import { AuthProvider } from './contexts/AuthContext'
import App from './App'
import './index.css'

function Providers({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()
  return (
    <HeroUIProvider navigate={navigate}>
      <AuthProvider>{children}</AuthProvider>
    </HeroUIProvider>
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Providers>
        <App />
      </Providers>
    </BrowserRouter>
  </StrictMode>,
)
