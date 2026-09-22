import {
  Button,
  Dropdown,
  DropdownItem,
  DropdownMenu,
  DropdownTrigger,
  Listbox,
  ListboxItem,
  Navbar,
  NavbarBrand,
  NavbarContent,
  NavbarItem,
  NavbarMenu,
  NavbarMenuItem,
  NavbarMenuToggle,
} from '@heroui/react'
import { useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

const NAV_ITEMS = [
  { path: '/', label: 'Dashboard' },
  { path: '/wal', label: 'WAL Transactions' },
  { path: '/reverts', label: 'Reverts' },
  { path: '/databases', label: 'Databases' },
]

export function AppLayout() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const [menuOpen, setMenuOpen] = useState(false)

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Navbar isMenuOpen={menuOpen} onMenuOpenChange={setMenuOpen} maxWidth="xl">
        <NavbarContent>
          <NavbarMenuToggle className="sm:hidden" />
          <NavbarBrand>
            <span className="font-bold text-lg">Trail Replay</span>
          </NavbarBrand>
        </NavbarContent>
        <NavbarContent className="hidden sm:flex gap-4" justify="center">
          {NAV_ITEMS.map((item) => (
            <NavbarItem key={item.path} isActive={location.pathname === item.path}>
              <Button
                variant={location.pathname === item.path ? 'flat' : 'light'}
                size="sm"
                onPress={() => navigate(item.path)}
              >
                {item.label}
              </Button>
            </NavbarItem>
          ))}
        </NavbarContent>
        <NavbarContent justify="end">
          <NavbarItem>
            <Dropdown>
              <DropdownTrigger>
                <Button variant="flat" size="sm">
                  {user?.email}
                </Button>
              </DropdownTrigger>
              <DropdownMenu
                aria-label="User menu"
                onAction={(key) => {
                  if (key === 'logout') {
                    logout()
                    navigate('/login')
                  }
                }}
              >
                <DropdownItem key="logout" color="danger">
                  Log out
                </DropdownItem>
              </DropdownMenu>
            </Dropdown>
          </NavbarItem>
        </NavbarContent>
        <NavbarMenu>
          {NAV_ITEMS.map((item) => (
            <NavbarMenuItem key={item.path}>
              <Button
                fullWidth
                variant="light"
                onPress={() => {
                  setMenuOpen(false)
                  navigate(item.path)
                }}
              >
                {item.label}
              </Button>
            </NavbarMenuItem>
          ))}
        </NavbarMenu>
      </Navbar>

      <div className="flex w-full max-w-7xl mx-auto gap-6 px-6 py-6">
        <aside className="hidden md:block w-52 shrink-0">
          <Listbox
            aria-label="Navigation"
            selectedKeys={[location.pathname]}
            onAction={(key) => navigate(String(key))}
          >
            {NAV_ITEMS.map((item) => (
              <ListboxItem key={item.path}>{item.label}</ListboxItem>
            ))}
          </Listbox>
        </aside>
        <main className="flex-1 min-w-0">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
