import React, { useMemo, useEffect, useState } from 'react';
import { useAdmin } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area } from 'recharts';
import { TrendingUp, DollarSign, ShoppingCart, BookOpen, Users, Package, Star, ArrowUpRight, Loader2 } from 'lucide-react';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { formatVND } from '../../lib/formatters';

// Type declarations
type ChangeType = 'increase' | 'decrease' | 'neutral';

interface StatsCard {
  title: string;
  value: string;
  change: string;
  changeType: ChangeType;
  icon: React.ElementType;
  color: string;
  iconBg: string;
  iconColor: string;
  subtitle: string;
}

interface TopSellingBook {
  book: any;
  quantity: number;
  revenue: number;
}

interface CategoryRevenue {
  category: string;
  revenue: number;
}

interface MonthlyData {
  month: string;
  revenue: number;
  orders: number;
}

interface StatusData {
  name: string;
  value: number;
  color: string;
}

export const Statistics: React.FC = () => {
  const { orders, books, users, inventory, refreshAll } = useAdmin();
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [isLoading, setIsLoading] = useState(true);

  // Auto-load admin data when dashboard mounts
  useEffect(() => {
    if (refreshAll) {
      console.log('[Statistics] Auto-loading dashboard data');
      setIsLoading(true);
      refreshAll().finally(() => {
        setIsLoading(false);
      });
    }
  }, []);

  const formatCurrency = (amount: number) => formatVND(amount);

  // Memoize expensive calculations
  const stats = useMemo(() => {
    // Revenue from all non-cancelled and non-returned orders (actual business revenue)
    const totalRevenue = orders
      .filter(o => o.status !== 'CANCELLED' && o.status !== 'RETURNED')
      .reduce((sum, o) => sum + o.totalAmount, 0);

    const totalOrders = orders.length;
    // Completed = DELIVERED status only
    const completedOrders = orders.filter(o => o.status === 'DELIVERED').length;

    const totalBooks = books.length;
    const inStockBooks = books.filter(b => b.stockQuantity).length;
    const totalCustomers = users.filter(u => u.role === 'customer').length;

    // Count non-cancelled/returned orders for subtitle consistency
    const activeOrders = orders.filter(o => o.status !== 'CANCELLED' && o.status !== 'RETURNED').length;

    return {
      totalRevenue,
      totalOrders,
      completedOrders,
      activeOrders,
      averageOrderValue: completedOrders > 0 ? totalRevenue / completedOrders : 0,
      totalBooks,
      inStockBooks,
      totalCustomers,
    };
  }, [orders, books, users]);

  // Top selling books - memoized
  const topSellingBooks = useMemo(() => {
    const bookSales = new Map<string, number>();

    // Aggregate quantities by book id - include DELIVERED + other valid statuses (exclude CANCELLED, RETURNED)
    const validStatuses = ['DELIVERED', 'COMPLETED', 'PROCESSING', 'SHIPPED', 'CONFIRMED'];
    const relevantOrders = (orders || []).filter(o => 
      o.status && !['CANCELLED', 'RETURNED'].includes(o.status) && validStatuses.includes(o.status)
    );

    relevantOrders.forEach(order => {
      (order.items || []).forEach((item: any) => {
        const maybeId = item?.bookId ?? item?.id ?? item?.book?.bookId ?? item?.book?.id;
        const quantity = Number(item?.quantity ?? 0);
        if (maybeId && quantity > 0) {
          const key = String(maybeId);
          bookSales.set(key, (bookSales.get(key) || 0) + quantity);
        }
      });
    });

    return Array.from(bookSales.entries())
      .map(([bookId, quantity]) => {
        const book = books.find((b: any) =>
          String(b.bookId) === String(bookId) ||
          String(b.id) === String(bookId) ||
          String((b as any)._id) === String(bookId)
        );
        if (book) {
          return { 
            book, 
            quantity, 
            revenue: quantity * Number(book.price ?? 0) 
          };
        }
        return null;
      })
      .filter((item) => item !== null)
      .sort((a, b) => (b!.quantity - a!.quantity))
      .slice(0, 10);
  }, [orders, books]);

  // Category revenue - memoized
  const categoryData = useMemo(() => {
    const categoryRevenue = new Map<string, number>();
    orders
      .filter(o => o.status === 'DELIVERED')
      .forEach(order => {
        order.items.forEach(item => {
          const book = books.find(b => String(b.bookId) === String(item.bookId));
          if (book) {
            const revenue = item.price * item.quantity;
            const categoryKey = book.categories.map((c: any) => c.categoryName).join(', ') || 'Khác';
            categoryRevenue.set(categoryKey, (categoryRevenue.get(categoryKey) || 0) + revenue);
          }
        });
      });

    return Array.from(categoryRevenue.entries())
      .map(([category, revenue]) => ({ category, revenue }))
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 8);
  }, [orders, books]);

  // Monthly data - memoized (12 months)
  const monthlyData = useMemo(() => {
    const months = [
      'Tháng 1', 'Tháng 2', 'Tháng 3', 'Tháng 4', 'Tháng 5', 'Tháng 6',
      'Tháng 7', 'Tháng 8', 'Tháng 9', 'Tháng 10', 'Tháng 11', 'Tháng 12'
    ];
    
    // Calculate actual monthly data from orders in selected year
    const monthlyStats = Array.from({ length: 12 }, (_, monthIndex) => {
      const monthNum = monthIndex + 1;
      
      const monthOrders = orders.filter(o => {
        const orderDate = new Date(o.orderDate || o.orderDate);
        return orderDate.getFullYear() === selectedYear && 
               (orderDate.getMonth() + 1) === monthNum &&
               o.status !== 'CANCELLED' && 
               o.status !== 'RETURNED';
      });
      
      const revenue = monthOrders.reduce((sum, o) => sum + o.totalAmount, 0);
      
      return {
        month: months[monthIndex],
        revenue,
        orders: monthOrders.length,
      };
    });
    
    return monthlyStats;
  }, [orders, selectedYear]);

  // Inventory status - memoized
  const inventoryStatusData = useMemo(() => {
    const inStock = inventory.filter(i => (i.quantity || 0) > 5).length;
    const lowStock = inventory.filter(i => {
      const qty = i.quantity || 0;
      return qty <= 5 && qty > 0;
    }).length;
    const outOfStock = inventory.filter(i => (i.quantity || 0) === 0).length;

    return [
      { name: 'Đủ hàng', value: inStock, color: '#10b981' },
      { name: 'Sắp hết', value: lowStock, color: '#f59e0b' },
      { name: 'Hết hàng', value: outOfStock, color: '#ef4444' },
    ].filter(item => item.value > 0);
  }, [inventory]);

  // Order status - memoized
  const orderStatusData = useMemo(() => {
    const statusMap = new Map<string, number>();
    const statusTranslations: { [key: string]: string } = {
      'PENDING': 'Chờ xử lý',
      'CONFIRMED': 'Đã xác nhận',
      'PROCESSING': 'Đang xử lý',
      'SHIPPED': 'Đang giao',
      'DELIVERED': 'Đã giao',
      'COMPLETED': 'Hoàn thành',
      'CANCELLED': 'Đã hủy',
      'RETURNED': 'Trả hàng',
    };
    const statusColors: { [key: string]: string } = {
      'PENDING': '#f59e0b',
      'CONFIRMED': '#3b82f6',
      'PROCESSING': '#8b5cf6',
      'SHIPPED': '#06b6d4',
      'DELIVERED': '#10b981',
      'COMPLETED': '#059669',
      'CANCELLED': '#ef4444',
      'RETURNED': '#dc2626',
    };

    orders.forEach(o => {
      const status = o.status || 'UNKNOWN';
      statusMap.set(status, (statusMap.get(status) || 0) + 1);
    });

    return Array.from(statusMap.entries()).map(([status, value]) => ({
      name: statusTranslations[status] || status,
      value,
      color: statusColors[status] || '#6b7280',
    }));
  }, [orders]);

  const statsCards: StatsCard[] = [
    {
      title: 'Tổng doanh thu',
      value: formatCurrency(stats.totalRevenue),
      change: '+12.5%',
      changeType: 'increase',
      icon: DollarSign,
      color: 'from-emerald-500 to-green-600',
      iconBg: 'bg-emerald-50',
      iconColor: 'text-emerald-600',
      subtitle: `${stats.activeOrders} đơn hàng có giá trị`,
    },
    {
      title: 'Đơn hàng đã bán',
      value: stats.totalOrders.toString(),
      change: '+8.2%',
      changeType: 'increase',
      icon: ShoppingCart,
      color: 'from-primary to-primary/80',
      iconBg: 'bg-primary/10',
      iconColor: 'text-primary',
      subtitle: 'Tổng đơn hàng đã bán',
    },
    {
      title: 'Tổng sách',
      value: stats.totalBooks.toString(),
      change: `${stats.inStockBooks} còn hàng`,
      changeType: 'neutral',
      icon: BookOpen,
      color: 'from-purple-500 to-pink-600',
      iconBg: 'bg-purple-50',
      iconColor: 'text-purple-600',
      subtitle: `${stats.totalBooks - stats.inStockBooks} hết hàng`,
    },
    {
      title: 'Khách hàng',
      value: stats.totalCustomers.toString(),
      change: '+5.7%',
      changeType: 'increase',
      icon: Users,
      color: 'from-orange-500 to-amber-600',
      iconBg: 'bg-orange-50',
      iconColor: 'text-orange-600',
      subtitle: 'Người dùng đăng ký',
    },
  ];

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {/* Welcome Banner */}
      <Card className="border-none shadow-lg bg-gradient-to-br from-primary via-primary/90 to-primary/80 text-primary-foreground overflow-hidden relative">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4xIj48cGF0aCBkPSJNMzYgMzRjMC0yIDItNCAzLTRzMyAyIDMgNHYyYzAgMi0yIDQtMyA0cy0zLTItMy00di0yem0wLTMwYzAtMiAyLTQgMy00czMgMiAzIDR2MmMwIDItMiA0LTMgNC0xIDAtMy0yLTMtNFY0eiIvPjwvZz48L2c+PC9zdmc+')] opacity-20"></div>
        <CardContent className="p-6 relative z-10">
          <div className="flex items-start justify-between">
            <div>
              <h2 className="text-2xl font-bold mb-2">Xin chào, Admin! 👋</h2>
              <p className="opacity-90 mb-4">
                Đây là tổng quan về hiệu suất kinh doanh của bạn
              </p>
              <div className="flex items-center gap-4 text-sm">
                <div className="flex items-center gap-1">
                  <div className="h-2 w-2 rounded-full bg-green-400 animate-pulse"></div>
                  <span>Hệ thống hoạt động tốt</span>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Key Metrics - Enhanced Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {statsCards.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <Card key={index} className="border-none shadow-md hover:shadow-xl transition-all duration-300 overflow-hidden group">
              <CardContent className="p-6">
                <div className="flex items-start justify-between mb-4">
                  <div className={`flex h-12 w-12 items-center justify-center rounded-xl ${stat.iconBg} group-hover:scale-110 transition-transform duration-300`}>
                    <Icon className={`h-6 w-6 ${stat.iconColor}`} />
                  </div>
                  <div className={`flex items-center gap-1 text-sm ${
                    stat.changeType === 'increase' ? 'text-green-600' : 
                    stat.changeType === 'decrease' ? 'text-red-600' : 
                    'text-slate-500'
                  }`}>
                    {stat.changeType === 'increase' && <ArrowUpRight className="h-4 w-4" />}
                    <span className="font-semibold">{stat.change}</span>
                  </div>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground mb-1">{stat.title}</p>
                  <p className="text-2xl font-bold text-foreground mb-1">{stat.value}</p>
                  <p className="text-xs text-muted-foreground">{stat.subtitle}</p>
                </div>
              </CardContent>
              <div className={`h-1 bg-gradient-to-r ${stat.color}`}></div>
            </Card>
          );
        })}
      </div>

      {/* Charts Row 1 */}
      <div className="grid gap-4">
        {/* Monthly Revenue Chart */}
        <Card className="border-none shadow-md">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-emerald-600" />
                  Doanh thu theo tháng
                </CardTitle>
                <CardDescription>12 tháng năm {selectedYear}</CardDescription>
              </div>
              <div className="flex items-center gap-2">
                <select
                  value={selectedYear}
                  onChange={(e) => setSelectedYear(Number(e.target.value))}
                  className="px-3 py-2 text-sm border rounded-lg bg-background text-foreground cursor-pointer"
                >
                  {Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i).map(year => (
                    <option key={year} value={year}>{year}</option>
                  ))}
                </select>
                <Badge variant="secondary">
                  +15.3%
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={350}>
              <AreaChart data={monthlyData}>
                <defs>
                  <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                <XAxis 
                  dataKey="month" 
                  className="text-muted-foreground"
                  tick={{ fontSize: 12 }}
                />
                <YAxis 
                  className="text-muted-foreground"
                  tick={{ fontSize: 12 }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'hsl(var(--background))', 
                    border: '1px solid hsl(var(--border))', 
                    borderRadius: '8px', 
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' 
                  }}
                  formatter={(value: number) => formatCurrency(value)}
                />
                <Area 
                  type="monotone" 
                  dataKey="revenue" 
                  stroke="#10b981" 
                  strokeWidth={2}
                  fillOpacity={1} 
                  fill="url(#colorRevenue)" 
                  name="Doanh thu"
                />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Category Revenue Chart */}
      </div>

      {/* Charts Row 2 */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Order Status */}
        <Card className="border-none shadow-md">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <ShoppingCart className="h-5 w-5 text-green-600" />
              Phân bố đơn hàng
            </CardTitle>
            <CardDescription>Trạng thái đơn hàng hiện tại</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between gap-4">
              <ResponsiveContainer width="50%" height={250}>
                <PieChart>
                  <Pie
                    data={orderStatusData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={90}
                    fill="#8884d8"
                    dataKey="value"
                    paddingAngle={5}
                  >
                    {orderStatusData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
              <div className="space-y-3 flex-1">
                {orderStatusData.map((item, index) => (
                  <div key={index} className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className="h-3 w-3 rounded-full" style={{ backgroundColor: item.color }}></div>
                      <span className="text-sm text-foreground">{item.name}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-foreground">{item.value}</span>
                      <span className="text-xs text-muted-foreground">
                        ({stats.totalOrders > 0 ? Math.round((item.value / stats.totalOrders) * 100) : 0}%)
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Inventory Status */}
        <Card className="border-none shadow-md">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Package className="h-5 w-5 text-orange-600" />
              Tình trạng kho
            </CardTitle>
            <CardDescription>Phân bố tồn kho</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between gap-4">
              <ResponsiveContainer width="50%" height={250}>
                <PieChart>
                  <Pie
                    data={inventoryStatusData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={90}
                    fill="#8884d8"
                    dataKey="value"
                    paddingAngle={2}
                    minAngle={5}
                  >
                    {inventoryStatusData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => `${value} mục`} />
                </PieChart>
              </ResponsiveContainer>
              <div className="space-y-3 flex-1">
                {inventoryStatusData.map((item, index) => {
                  const total = inventoryStatusData.reduce((sum, i) => sum + i.value, 0);
                  return (
                    <div key={index} className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="h-3 w-3 rounded-full" style={{ backgroundColor: item.color }}></div>
                        <span className="text-sm text-foreground">{item.name}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="font-semibold text-foreground">{item.value}</span>
                        <span className="text-xs text-muted-foreground">
                          ({total > 0 ? ((item.value / total) * 100).toFixed(2) : 0}%)
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Top Selling Books */}
      <Card className="border-none shadow-md">
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Star className="h-5 w-5 text-yellow-500 fill-yellow-500" />
                Sản phẩm bán chạy
              </CardTitle>
              <CardDescription>Top 10 sách có doanh số cao nhất</CardDescription>
            </div>
            <Badge variant="outline" className="border-yellow-200 bg-yellow-50 text-yellow-700">
              Best Sellers
            </Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="rounded-lg border overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/50 hover:bg-muted/50">
                  <TableHead className="w-16">Hạng</TableHead>
                  <TableHead>Sách</TableHead>
                  <TableHead>Danh mục</TableHead>
                  <TableHead>Đã bán</TableHead>
                  <TableHead>Giá</TableHead>
                  <TableHead className="text-right">Doanh thu</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {topSellingBooks.length === 0 ? (
                  // Hiển thị dữ liệu mẫu khi chưa có dữ liệu thực
                  books.slice(0, 5).map((book: any, index: number) => (
                    <TableRow key={book.bookId} className="hover:bg-muted/50 transition-colors">
                      <TableCell>
                        <div className={`flex items-center justify-center w-8 h-8 rounded-lg font-bold ${
                          index === 0 ? 'bg-gradient-to-br from-yellow-400 to-yellow-600 text-white' :
                          index === 1 ? 'bg-gradient-to-br from-slate-300 to-slate-500 text-white' :
                          index === 2 ? 'bg-gradient-to-br from-orange-400 to-orange-600 text-white' :
                          'bg-muted text-foreground'
                        }`}>
                          #{index + 1}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-3">
                          {book.imageUrl ? (
                            <ImageWithFallback
                              src={book.imageUrl}
                              alt={book.title}
                              className="w-12 h-16 object-cover rounded shadow-sm"
                            />
                          ) : (
                            <div className="w-12 h-16 bg-gray-200 rounded flex items-center justify-center flex-shrink-0">
                              <BookOpen className="h-6 w-6 text-gray-400" />
                            </div>
                          )}
                          <div>
                            <div className="font-medium text-foreground max-w-[250px] truncate">
                              {book.title}
                            </div>
                            <div className="text-sm text-muted-foreground">{book.author}</div>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="bg-purple-50 text-purple-700 border-purple-200">
                          {Array.isArray(book.categories) 
                            ? book.categories.map((c: any) => c.categoryName || c.name || c).join(', ')
                            : book.categories}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Package className="h-4 w-4 text-muted-foreground" />
                          <span className="font-semibold">{Math.floor(Math.random() * 50) + 10}</span>
                          <span className="text-xs text-muted-foreground">(Demo)</span>
                        </div>
                      </TableCell>
                      <TableCell className="font-medium">{formatCurrency(book.price || 0)}</TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-1 font-semibold text-emerald-600">
                          <TrendingUp className="h-4 w-4" />
                          {formatCurrency((book.price || 0) * (Math.floor(Math.random() * 50) + 10))}
                          <span className="text-xs text-muted-foreground ml-1">(Demo)</span>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                ) : (
                  topSellingBooks.map((item: TopSellingBook | null, index: number) => (
                    <TableRow key={item!.book.bookId} className="hover:bg-muted/50 transition-colors">
                      <TableCell>
                        <div className={`flex items-center justify-center w-8 h-8 rounded-lg font-bold ${
                          index === 0 ? 'bg-gradient-to-br from-yellow-400 to-yellow-600 text-white' :
                          index === 1 ? 'bg-gradient-to-br from-slate-300 to-slate-500 text-white' :
                          index === 2 ? 'bg-gradient-to-br from-orange-400 to-orange-600 text-white' :
                          'bg-muted text-foreground'
                        }`}>
                          #{index + 1}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-3">
                          {item!.book.imageUrl ? (
                            <ImageWithFallback
                              src={item!.book.imageUrl}
                              alt={item!.book.title}
                              className="w-12 h-16 object-cover rounded shadow-sm"
                            />
                          ) : (
                            <div className="w-12 h-16 bg-gray-200 rounded flex items-center justify-center flex-shrink-0">
                              <BookOpen className="h-6 w-6 text-gray-400" />
                            </div>
                          )}
                          <div>
                            <div className="font-medium text-foreground max-w-[250px] truncate">
                              {item!.book.title}
                            </div>
                            <div className="text-sm text-muted-foreground">{item!.book.author}</div>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="bg-purple-50 text-purple-700 border-purple-200">
                          {Array.isArray(item!.book.categories) 
                            ? item!.book.categories.map((c: any) => c.categoryName || c.name || c).join(', ')
                            : item!.book.categories}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Package className="h-4 w-4 text-muted-foreground" />
                          <span className="font-semibold">{item!.quantity}</span>
                        </div>
                      </TableCell>
                      <TableCell className="font-medium">{formatCurrency(item!.book.price || 0)}</TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-1 font-semibold text-emerald-600">
                          <TrendingUp className="h-4 w-4" />
                          {formatCurrency(item!.revenue)}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};
