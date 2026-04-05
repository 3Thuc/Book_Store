import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from '../components/ui/sidebar';
import {
  LayoutDashboard,
  Users,
  BookOpen,
  Tag,
  ShoppingCart,
  Package,
  Percent,
  BarChart3,
  LogOut,
  Home,
  Settings,
  Building2,
  UserPen,
} from 'lucide-react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Avatar, AvatarFallback } from '../components/ui/avatar';
import { Badge } from '../components/ui/badge';
import { ThemeToggle } from '../components/ThemeToggle';
import { useAuth } from '../context/AuthContext';
import { AdminProvider } from '../features/admin/AdminContext';

// Import admin components from features/admin
import { UserManagement } from '../features/admin/UserManagement';
import { BookManagement } from '../features/admin/BookManagement';
import { CategoryManagement } from '../features/admin/CategoryManagement';
import { OrderManagement } from '../features/admin/OrderManagement';
import { PublisherManagement } from '../features/admin/PublisherManagement';
import { InventoryManagement } from '../features/admin/InventoryManagement';
import { PromotionManagement } from '../features/admin/PromotionManagement';
import { Statistics } from '../features/admin/Statistics';
import { NotificationsDropdown } from '../features/admin/NotificationsDropdown';
import { AuthorManagement } from '../features/admin';

type MenuItem = {
  id: string;
  label: string;
  icon: React.ReactNode;
  component: React.ReactNode;
  badge?: number;
  roles?: string[]; // Add role-based access control
};

const AdminPageContent: React.FC = () => {
  const [activeMenu, setActiveMenu] = useState('statistics');
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  // All menu items with role restrictions
  const allMenuItems: MenuItem[] = [
    {
      id: 'statistics',
      label: 'Dashboard',
      icon: <LayoutDashboard className="h-5 w-5" />,
      component: <Statistics />,
      roles: ['admin', 'staff'],
    },
    {
      id: 'users',
      label: 'Quản lý người dùng',
      icon: <Users className="h-5 w-5" />,
      component: <UserManagement />,
      roles: ['admin'],
    },
    {
      id: 'books',
      label: 'Quản lý sách',
      icon: <BookOpen className="h-5 w-5" />,
      component: <BookManagement />,
      roles: ['admin', 'staff'],
    },
    {
      id: 'categories',
      label: 'Quản lý danh mục',
      icon: <Tag className="h-5 w-5" />,
      component: <CategoryManagement />,
      roles: ['admin'], // Admin only
    },
    {
      id: 'orders',
      label: 'Quản lý đơn hàng',
      icon: <ShoppingCart className="h-5 w-5" />,
      component: <OrderManagement />,
      roles: ['admin', 'staff'], // Both can access
    },
    {
      id: 'publishers',
      label: 'Quản lý nhà xuất bản',
      icon: <Building2 className="h-5 w-5" />,
      component: <PublisherManagement />,
      roles: ['admin', 'staff'],
    },
        {
      id: "authors",
      label: "Quản lý tác giả",
      icon: <UserPen className="h-5 w-5" />,
      component: <AuthorManagement />,
      roles: ["admin", 'staff'],
    },
    {
      id: 'inventory',
      label: 'Quản lý kho',
      icon: <Package className="h-5 w-5" />,
      component: <InventoryManagement />,
      roles: ['admin', 'staff'], // Both can access
    },
    {
      id: 'promotions',
      label: 'Quản lý khuyến mãi',
      icon: <Percent className="h-5 w-5" />,
      component: <PromotionManagement />,
      roles: ['admin', 'staff'], // Both can access
    },

  ];

  // Filter menu items based on user role
  const menuItems = allMenuItems.filter(item =>
    !item.roles || item.roles.includes(user?.role || 'customer')
  );

  const handleBackToHome = () => {
    navigate('/');
  };

  const handleLogout = () => {
    logout();
    toast.success('Đăng xuất thành công!', {
      description: 'Hẹn gặp lại bạn.'
    });

    // Small delay to show toast before navigation
    setTimeout(() => {
      navigate('/');
    }, 500);
  };

  const activeMenuItem = menuItems.find(item => item.id === activeMenu);

  return (
    <SidebarProvider>
      {/* Enhanced Sidebar */}
      <Sidebar className="border-r bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        <SidebarContent>
          {/* Logo & Branding */}
          <div className="px-6 py-5 border-b">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary shadow-lg">
                <LayoutDashboard className="h-5 w-5 text-primary-foreground" />
              </div>
              <div>
                <h2 className="font-bold text-foreground">Admin Panel</h2>
                <p className="text-xs text-muted-foreground">BookStore Management</p>
              </div>
            </div>
          </div>

          {/* User Profile Card */}
          {user && (
            <div className="px-4 py-4">
              <Card className="p-4 bg-gradient-to-br from-primary/5 to-secondary/10 shadow-sm">
                <div className="flex items-center gap-3">
                  <Avatar className="h-11 w-11 border-2 border-background shadow-md">
                    <AvatarFallback className="bg-primary text-primary-foreground">
                      {user.fullName?.charAt(0)?.toUpperCase() || 'A'}
                    </AvatarFallback>
                  </Avatar>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold text-foreground truncate">{user.fullName}</p>
                    <p className="text-xs text-muted-foreground truncate">{user.email}</p>
                  </div>
                  <Badge variant="secondary" className="capitalize">
                    {user.role === 'admin' ? 'Admin' : user.role === 'staff' ? 'Nhân viên' : 'Khách hàng'}
                  </Badge>
                </div>
              </Card>
            </div>
          )}

          {/* Navigation Menu */}
          <SidebarGroup className="px-3">
            <SidebarGroupLabel className="px-3 text-xs uppercase tracking-wider text-foreground/60 font-semibold mb-2">
              Chức năng
            </SidebarGroupLabel>
            <SidebarGroupContent className="mt-2">
              <SidebarMenu className="space-y-1">
                {menuItems.map((item) => (
                  <SidebarMenuItem key={item.id}>
                    <SidebarMenuButton
                      onClick={() => setActiveMenu(item.id)}
                      isActive={activeMenu === item.id}
                      className={`
                          w-full relative group transition-all duration-200
                          ${activeMenu === item.id
                          ? 'bg-background text-foreground shadow-md hover:shadow-lg border border-border'
                          : 'hover:bg-muted/50 text-foreground/70 hover:text-foreground'
                        }
                          rounded-lg px-3 py-2.5
                        `}
                    >
                      <div className={`
                          ${activeMenu === item.id ? 'text-foreground' : 'text-foreground/60 group-hover:text-foreground'}
                          transition-colors
                        `}>
                        {item.icon}
                      </div>
                      <span className="flex-1 text-left">{item.label}</span>
                      {item.badge && (
                        <Badge
                          variant="destructive"
                          className="h-5 px-2 text-xs"
                        >
                          {item.badge}
                        </Badge>
                      )}
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          {/* Bottom Actions */}
          <div className="mt-auto border-t p-4 space-y-2">
            <Button
              variant="ghost"
              className="w-full justify-start gap-3 hover:bg-accent transition-colors"
              onClick={handleBackToHome}
            >
              <Home className="h-4 w-4" />
              <span>Về trang chủ</span>
            </Button>
            <Button
              variant="ghost"
              className="w-full justify-start gap-3 text-destructive hover:text-destructive hover:bg-destructive/10 transition-colors"
              onClick={handleLogout}
            >
              <LogOut className="h-4 w-4" />
              <span>Đăng xuất</span>
            </Button>
          </div>
        </SidebarContent>
      </Sidebar>

      {/* Main Content Area */}
      <div className="flex min-h-screen w-full bg-gradient-to-br from-primary/5 to-secondary/10">
        <div className="flex-1 flex flex-col min-w-0 w-full">
          {/* Enhanced Top Bar */}
          <header className="sticky top-0 z-20 flex h-16 items-center gap-4 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80 px-6 shadow-sm">
            <SidebarTrigger className="lg:hidden" />

            <div className="flex-1 flex items-center gap-4">
              {/* Page Title */}
              <div className="flex items-center gap-3">
                <div className="hidden sm:flex h-10 w-10 items-center justify-center rounded-lg bg-secondary">
                  {activeMenuItem?.icon}
                </div>
                <div>
                  <h1 className="text-lg font-bold text-foreground">{activeMenuItem?.label}</h1>
                  <p className="text-xs text-muted-foreground hidden sm:block">Quản lý và theo dõi hệ thống</p>
                </div>
              </div>
            </div>

            {/* Top Bar Actions */}
            <div className="flex items-center gap-2">
              {/* Theme Toggle */}
              <ThemeToggle />

              {/* Notifications */}
              <NotificationsDropdown
                onNavigateToOrders={() => setActiveMenu('orders')}
                onNavigateToInventory={() => setActiveMenu('inventory')}
              />

            </div>
          </header>

          {/* Content Area with Gradient Background */}
          <main className="flex-1 p-6 overflow-y-auto">
            <div className="mx-auto max-w-7xl">
              {activeMenuItem?.component}
            </div>
          </main>

          {/* Footer */}
          <footer className="border-t bg-background/50 backdrop-blur-sm px-6 py-4">
            <div className="flex items-center justify-between text-sm text-muted-foreground">
              <p>© 2025 BookStore. All rights reserved.</p>
              <div className="flex items-center gap-4">
                <a href="#" onClick={(e) => e.preventDefault()} className="hover:text-foreground transition-colors">Trợ giúp</a>
                <a href="#" onClick={(e) => e.preventDefault()} className="hover:text-foreground transition-colors">Điều khoản</a>
                <a href="#" onClick={(e) => e.preventDefault()} className="hover:text-foreground transition-colors">Bảo mật</a>
              </div>
            </div>
          </footer>
        </div>
      </div>
    </SidebarProvider>
  );
};

export const AdminPage: React.FC = () => {
  return (
    <AdminProvider>
      <AdminPageContent />
    </AdminProvider>
  );
};