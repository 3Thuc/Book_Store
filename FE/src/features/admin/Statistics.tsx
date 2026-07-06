import React, { useMemo, useEffect, useState, useRef } from 'react';
import { useAdmin } from './AdminContext';
import { useDashboardFilter } from './DashboardFilterContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area } from 'recharts';
import { TrendingUp, DollarSign, ShoppingCart, BookOpen, Users, Package, Star, ArrowUpRight, Loader2 } from 'lucide-react';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { formatVND } from '../../lib/formatters';
import adminService from '../../services/adminService';

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

const POLL_INTERVAL_MS = 60_000; // 60 giây

export const Statistics: React.FC = () => {
  const { orders, books, users, inventory, isInventoryLoading, loadInventory, refreshAll } = useAdmin();
  const { filter, inPeriod, filterLabel, reset } = useDashboardFilter();
  const [isLoading, setIsLoading] = useState(true);
  const [isPolling, setIsPolling] = useState(false); // true khi đang poll ngầm
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const refreshAllRef = React.useRef(refreshAll);
  refreshAllRef.current = refreshAll;

  // Doanh thu chính xác từ API /admin/order-statistics (SUM(total_amount) từ DB)
  // Đồng nhất với trang Quản lý đơn hàng — tránh recompute sai từ orders[]
  const [apiTotalRevenue, setApiTotalRevenue] = useState<number | null>(null);
  const apiRevenueLoadedRef = useRef(false);

  useEffect(() => {
    const fetchRevenue = async () => {
      try {
        const res = await adminService.getOrderStats();
        const data = res?.result ?? res?.data ?? res;
        if (data && typeof data.totalRevenue === 'number') {
          setApiTotalRevenue(data.totalRevenue);
          apiRevenueLoadedRef.current = true;
        }
      } catch {
        // fallback: dùng giá trị tính từ orders[] (không chính xác với đơn cũ)
      }
    };
    fetchRevenue();
  }, [orders]); // re-fetch khi orders thay đổi để bắt kịp đơn mới

  // O(1) book lookup
  const bookMapById = useMemo(
    () => new Map(books.map(b => [String(b.bookId), b])),
    [books]
  );

  // Luôn refresh khi vào Dashboard để đảm bảo số liệu mới nhất
  useEffect(() => {
    // Đảm bảo inventory được load (lazy) ngay khi vào Dashboard
    // để chart "Tình trạng kho" không phải chờ user mở tab Quản lý kho
    if (loadInventory) loadInventory();

    if (refreshAllRef.current) {
      setIsLoading(true);
      refreshAllRef.current().then(() => setLastUpdated(new Date())).finally(() => setIsLoading(false));
    } else {
      setIsLoading(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Auto-poll mỗi 60s — dừng khi tab ẩn để tiết kiệm tài nguyên
  useEffect(() => {
    let handle: ReturnType<typeof setInterval> | null = null;
    let canceled = false;

    const poll = async () => {
      if (canceled || document.hidden || !refreshAllRef.current) return;
      setIsPolling(true);
      try {
        await refreshAllRef.current();
        if (!canceled) setLastUpdated(new Date());
      } catch {
        // Silent fail — không làm hỏng UI
      } finally {
        if (!canceled) setIsPolling(false);
      }
    };

    handle = setInterval(poll, POLL_INTERVAL_MS);
    return () => { canceled = true; if (handle) clearInterval(handle); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Reset filter về "Tất cả năm" mỗi khi vào Dashboard
  useEffect(() => {
    reset();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const formatCurrency = (amount: number) => formatVND(amount);

  // ─── Stats ────────────────────────────────────────────────────────────────
  const stats = useMemo(() => {
    const yr = filter.year; const mo = filter.month; const dy = filter.day;
    const periodOrders = orders.filter(o => {
      const d = new Date(o.orderDate);
      if (yr !== 0 && d.getFullYear()    !== yr) return false;
      if (mo !== 0 && (d.getMonth() + 1) !== mo) return false;
      if (dy !== 0 && d.getDate()        !== dy) return false;
      return true;
    });
    const computedRevenue = periodOrders.filter(o => {
      const s = String(o.status || '').toUpperCase();
      return s !== 'CANCELLED' && s !== 'RETURNED' && s !== 'FAILED';
    }).reduce((s, o) => s + o.totalAmount, 0);
    // Khi xem "Tất cả năm", dùng API totalRevenue (chính xác, đồng nhất với Quản lý đơn hàng)
    // Khi lọc theo năm/tháng/ngày, fallback về giá trị tính từ orders[]
    const isGlobalView = yr === 0 && mo === 0 && dy === 0;
    const totalRevenue = isGlobalView && apiTotalRevenue !== null ? apiTotalRevenue : computedRevenue;
    
    // Doanh thu thực nhận (delivered)
    const deliveredRevenue = periodOrders.filter(o => {
      const s = String(o.status || '').toUpperCase();
      return s === 'DELIVERED';
    }).reduce((s, o) => s + o.totalAmount, 0);

    // Đang thực hiện (pending, processing, shipped, confirmed)
    const pendingRevenue = periodOrders.filter(o => {
      const s = String(o.status || '').toUpperCase();
      return ['PENDING', 'PROCESSING', 'SHIPPED', 'CONFIRMED'].includes(s);
    }).reduce((s, o) => s + o.totalAmount, 0);

    const totalOrders     = periodOrders.length;
    const completedOrders = periodOrders.filter(o => String(o.status || '').toUpperCase() === 'DELIVERED').length;
    const activeOrders    = periodOrders.filter(o => {
      const s = String(o.status || '').toUpperCase();
      return s !== 'CANCELLED' && s !== 'RETURNED' && s !== 'FAILED';
    }).length;
    const pendingOrders = periodOrders.filter(o => String(o.status || '').toUpperCase() === 'PENDING').length;
    
    // Use inventory for more accurate active books count
    const totalBooks      = inventory && inventory.length > 0 ? inventory.length : books.length;
    const inStockBooks    = inventory && inventory.length > 0 
                              ? inventory.filter(b => Number(b.availableQuantity ?? b.stockQuantity ?? 0) > 0).length 
                              : books.filter(b => b.stockQuantity).length;
                              
    const totalCustomers  = users.filter(u => u.role === 'customer').length;
    return { totalRevenue, deliveredRevenue, pendingRevenue, totalOrders, completedOrders, activeOrders, pendingOrders,
             averageOrderValue: completedOrders > 0 ? totalRevenue / completedOrders : 0,
             totalBooks, inStockBooks, totalCustomers };
  }, [orders, books, users, filter.year, filter.month, filter.day, apiTotalRevenue]);

  // ─── Top selling books ────────────────────────────────────────────────────
  const topSellingBooks = useMemo(() => {
    const yr = filter.year; const mo = filter.month; const dy = filter.day;
    const validStatuses = ['DELIVERED', 'COMPLETED', 'PROCESSING', 'SHIPPED', 'CONFIRMED'];
    const bookSales = new Map<string, number>();
    (orders || []).filter(o => {
      if (!o.status || ['CANCELLED','RETURNED'].includes(o.status)) return false;
      if (!validStatuses.includes(o.status)) return false;
      const d = new Date(o.orderDate);
      if (yr !== 0 && d.getFullYear()    !== yr) return false;
      if (mo !== 0 && (d.getMonth() + 1) !== mo) return false;
      if (dy !== 0 && d.getDate()        !== dy) return false;
      return true;
    }).forEach(order => {
      (order.items || []).forEach((item: any) => {
        const id  = item?.bookId ?? item?.id ?? item?.book?.bookId ?? item?.book?.id;
        const qty = Number(item?.quantity ?? 0);
        if (id && qty > 0) bookSales.set(String(id), (bookSales.get(String(id)) || 0) + qty);
      });
    });
    return Array.from(bookSales.entries())
      .map(([bookId, quantity]) => {
        const book = bookMapById.get(bookId);
        return book ? { book, quantity, revenue: quantity * Number(book.price ?? 0) } : null;
      })
      .filter(x => x !== null)
      .sort((a, b) => b!.quantity - a!.quantity)
      .slice(0, 10);
  }, [orders, bookMapById, filter.year, filter.month, filter.day]);

  // ─── Category revenue ────────────────────────────────────────────────────
  const categoryData = useMemo(() => {
    const yr = filter.year; const mo = filter.month; const dy = filter.day;
    const categoryRevenue = new Map<string, number>();
    orders.filter(o => {
      if (o.status !== 'DELIVERED') return false;
      const d = new Date(o.orderDate);
      if (yr !== 0 && d.getFullYear()    !== yr) return false;
      if (mo !== 0 && (d.getMonth() + 1) !== mo) return false;
      if (dy !== 0 && d.getDate()        !== dy) return false;
      return true;
    }).forEach(order => {
      order.items.forEach(item => {
        const book = bookMapById.get(String(item.bookId));
        if (book) {
          const key = book.categories.map((c: any) => c.categoryName).join(', ') || 'Khác';
          categoryRevenue.set(key, (categoryRevenue.get(key) || 0) + item.price * item.quantity);
        }
      });
    });
    return Array.from(categoryRevenue.entries())
      .map(([category, revenue]) => ({ category, revenue }))
      .sort((a, b) => b.revenue - a.revenue).slice(0, 8);
  }, [orders, bookMapById, filter.year, filter.month, filter.day]);

  // ─── Chart data ──────────────────────────────────────────────────────────
  const chartData = useMemo(() => {
    const yr = filter.year; const mo = filter.month; const dy = filter.day;
    const valid = (o: any) => o.status !== 'CANCELLED' && o.status !== 'RETURNED' && o.status !== 'FAILED';
    if (yr === 0) {
      const cur = new Date().getFullYear();
      return Array.from({ length: 5 }, (_, i) => cur - 4 + i).map(y => {
        const yo = orders.filter(o => new Date(o.orderDate).getFullYear() === y && valid(o));
        return { label: `${y}`, revenue: yo.reduce((s, o) => s + o.totalAmount, 0), orders: yo.length };
      });
    }
    if (mo === 0) {
      const months = ['T1','T2','T3','T4','T5','T6','T7','T8','T9','T10','T11','T12'];
      return months.map((label, i) => {
        const list = orders.filter(o => { const d = new Date(o.orderDate); return d.getFullYear() === yr && (d.getMonth()+1) === i+1 && valid(o); });
        return { label, revenue: list.reduce((s, o) => s + o.totalAmount, 0), orders: list.length };
      });
    }
    if (dy === 0) {
      const days = new Date(yr, mo, 0).getDate();
      return Array.from({ length: days }, (_, i) => i + 1).map(day => {
        const list = orders.filter(o => { const d = new Date(o.orderDate); return d.getFullYear() === yr && (d.getMonth()+1) === mo && d.getDate() === day && valid(o); });
        return { label: `${day}`, revenue: list.reduce((s, o) => s + o.totalAmount, 0), orders: list.length };
      });
    }
    return Array.from({ length: 24 }, (_, h) => {
      const list = orders.filter(o => { const d = new Date(o.orderDate); return d.getFullYear() === yr && (d.getMonth()+1) === mo && d.getDate() === dy && d.getHours() === h && valid(o); });
      return { label: `${h}h`, revenue: list.reduce((s, o) => s + o.totalAmount, 0), orders: list.length };
    });
  }, [orders, filter.year, filter.month, filter.day]);

  // ─── Inventory (real-time, no date filter) ────────────────────────────────
  const inventoryStatusData = useMemo(() => {
    if (!inventory || inventory.length === 0) return [];
    
    // Inventory array items have availableQuantity or stockQuantity
    const getAvailable = (b: any) => Number(b.availableQuantity ?? b.stockQuantity ?? 0);
    
    const inStock    = inventory.filter(b => getAvailable(b) > 5).length;
    const lowStock   = inventory.filter(b => { const q = getAvailable(b); return q >= 1 && q <= 5; }).length;
    const outOfStock = inventory.filter(b => getAvailable(b) <= 0).length;
    
    return [
      { name: 'Đủ hàng',  value: inStock,    color: '#10b981' },
      { name: 'Sắp hết',  value: lowStock,   color: '#f59e0b' },
      { name: 'Hết hàng', value: outOfStock, color: '#ef4444' },
    ].filter(x => x.value > 0);
  }, [inventory]);

  // ─── Order status ─────────────────────────────────────────────────────────
  const orderStatusData = useMemo(() => {
    const yr = filter.year; const mo = filter.month; const dy = filter.day;
    const statusMap = new Map<string, number>();
    const T: Record<string,string> = { PENDING:'Chờ xử lý', CONFIRMED:'Đã xác nhận', PROCESSING:'Đang xử lý', SHIPPED:'Đang giao', DELIVERED:'Đã giao', COMPLETED:'Hoàn thành', CANCELLED:'Đã hủy', RETURNED:'Trả hàng', CANCEL_REQUESTED:'Yêu cầu hủy', RETURN_REQUESTED:'Yêu cầu trả', FAILED:'Thất bại' };
    const C: Record<string,string> = { PENDING:'#f59e0b', CONFIRMED:'#3b82f6', PROCESSING:'#8b5cf6', SHIPPED:'#06b6d4', DELIVERED:'#10b981', COMPLETED:'#059669', CANCELLED:'#ef4444', RETURNED:'#dc2626', CANCEL_REQUESTED:'#6b7280', RETURN_REQUESTED:'#6b7280', FAILED:'#1f2937' };
    orders.filter(o => {
      const d = new Date(o.orderDate);
      if (yr !== 0 && d.getFullYear()    !== yr) return false;
      if (mo !== 0 && (d.getMonth() + 1) !== mo) return false;
      if (dy !== 0 && d.getDate()        !== dy) return false;
      return true;
    }).forEach(o => {
      const s = o.status || 'UNKNOWN';
      statusMap.set(s, (statusMap.get(s) || 0) + 1);
    });
    return Array.from(statusMap.entries()).map(([s, value]) => ({ name: T[s] || s, value, color: C[s] || '#6b7280' }));
  }, [orders, filter.year, filter.month, filter.day]);

  const statsCards: StatsCard[] = [
    {
      title: 'Doanh thu thực nhận',
      value: formatCurrency(stats.deliveredRevenue),
      change: '+12.5%',
      changeType: 'increase',
      icon: DollarSign,
      color: 'from-emerald-500 to-green-600',
      iconBg: 'bg-emerald-50',
      iconColor: 'text-emerald-600',
      subtitle: 'Từ đơn hàng đã giao',
    },
    {
      title: 'Tổng đơn hàng',
      value: stats.totalOrders.toString(),
      change: '+8.2%',
      changeType: 'increase',
      icon: ShoppingCart,
      color: 'from-primary to-primary/80',
      iconBg: 'bg-primary/10',
      iconColor: 'text-primary',
      subtitle: `+${stats.pendingOrders} đơn chờ xử lý`,
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
              <p className="opacity-90 mb-4">Đây là tổng quan về hiệu suất kinh doanh của bạn</p>
              <div className="flex items-center gap-4 text-sm flex-wrap">
                <div className="flex items-center gap-1">
                  <div className="h-2 w-2 rounded-full bg-green-400 animate-pulse"></div>
                  <span>Hệ thống hoạt động tốt</span>
                </div>
                {/* Live update indicator */}
                {isPolling ? (
                  <div className="flex items-center gap-1.5 opacity-80">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    <span className="text-xs">Đang cập nhật...</span>
                  </div>
                ) : lastUpdated ? (
                  <div className="flex items-center gap-1.5 opacity-70">
                    <div className="h-1.5 w-1.5 rounded-full bg-white/80"></div>
                    <span className="text-xs">
                      Cập nhật lúc {lastUpdated.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                    </span>
                  </div>
                ) : null}
              </div>
            </div>
            {/* Manual refresh button */}
            <button
              onClick={() => {
                if (refreshAllRef.current && !isPolling && !isLoading) {
                  setIsPolling(true);
                  refreshAllRef.current().then(() => setLastUpdated(new Date())).finally(() => setIsPolling(false));
                }
              }}
              title="Làm mới số liệu"
              className="flex items-center gap-1.5 text-xs bg-white/15 hover:bg-white/25 transition-colors rounded-lg px-3 py-2 cursor-pointer"
              disabled={isPolling || isLoading}
            >
              <Loader2 className={`h-3.5 w-3.5 ${(isPolling || isLoading) ? 'animate-spin' : ''}`} />
              <span>Làm mới</span>
            </button>
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
                  {filter.year === 0
                    ? 'Doanh thu theo năm (5 năm gần nhất)'
                    : filter.month === 0
                      ? `Doanh thu theo tháng — Năm ${filter.year}`
                      : filter.day === 0
                        ? `Doanh thu theo ngày — Tháng ${filter.month}/${filter.year}`
                        : `Doanh thu theo giờ — Ngày ${filter.day}/${filter.month}/${filter.year}`}
                </CardTitle>
                <CardDescription>
                  {filterLabel}
                </CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={350}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                <XAxis
                  dataKey="label"
                  className="text-muted-foreground"
                  tick={{ fontSize: filter.month === 0 ? 12 : 10 }}
                  label={filter.month > 0 ? { value: `Ngày (Tháng ${filter.month}/${filter.year})`, position: 'insideBottom', offset: -2, fontSize: 11 } : undefined}
                />
                <YAxis
                  className="text-muted-foreground"
                  tick={{ fontSize: 11 }}
                  tickFormatter={(value: number) => {
                    if (value === 0) return '0';
                    if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
                    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
                    if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
                    return value.toString();
                  }}
                  width={55}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(var(--background))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'
                  }}
                  formatter={(value: number, name: string) =>
                    name === 'revenue' ? [formatCurrency(value), 'Doanh thu'] :
                    [value, 'Đơn hàng']
                  }
                  labelFormatter={(label) =>
                    filter.day > 0 ? `${label}` :
                    filter.month > 0 ? `Ngày ${label}` : `Tháng ${label.replace('T','')}`
                  }
                />
                <Area
                  type="monotone"
                  dataKey="revenue"
                  stroke="#10b981"
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#colorRevenue)"
                  name="revenue"
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
            <CardDescription>Trạng thái đơn hàng ({stats.totalOrders.toLocaleString()} đơn)</CardDescription>
          </CardHeader>
          <CardContent>
            {stats.totalOrders === 0 ? (
              <div className="flex items-center justify-center h-[250px] text-muted-foreground text-sm">
                Chưa có dữ liệu đơn hàng
              </div>
            ) : (
              <div className="flex flex-col gap-5">
                {/* Donut + Center Label */}
                <div className="flex items-center gap-6">
                  <div className="relative flex-shrink-0">
                    <ResponsiveContainer width={240} height={240}>
                      <PieChart>
                        <Pie
                          data={orderStatusData}
                          cx="50%" cy="50%"
                          innerRadius={80} outerRadius={110}
                          dataKey="value"
                          paddingAngle={3}
                          minAngle={4}
                          strokeWidth={0}
                        >
                          {orderStatusData.map((entry, i) => (
                            <Cell key={i} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip
                          formatter={(v: number) => [`${v.toLocaleString()} đơn hàng`, '']}
                          contentStyle={{ borderRadius: 8, fontSize: 12 }}
                        />
                      </PieChart>
                    </ResponsiveContainer>
                    {/* Center label */}
                    <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                      <span className="text-2xl font-bold text-foreground leading-none">
                        {stats.totalOrders.toLocaleString()}
                      </span>
                      <span className="text-[10px] text-muted-foreground mt-0.5">đơn hàng</span>
                    </div>
                  </div>

                  {/* Legend */}
                  <div className="flex-1 space-y-3">
                    {orderStatusData
                      .sort((a, b) => b.value - a.value) // Sort desc by value to look better
                      .map((item, i) => {
                        const pct = stats.totalOrders > 0 ? (item.value / stats.totalOrders) * 100 : 0;
                        return (
                          <div key={i} className="space-y-1.5">
                            <div className="flex items-center justify-between text-sm">
                              <div className="flex items-center gap-2">
                                <span className="inline-block h-2.5 w-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: item.color }} />
                                <span className="text-foreground font-medium">{item.name}</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <span className="font-semibold text-foreground">{item.value.toLocaleString()}</span>
                                <span className="text-xs text-muted-foreground">({pct.toFixed(1)}%)</span>
                              </div>
                            </div>
                            {/* Progress bar */}
                            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
                              <div
                                className="h-full rounded-full transition-all duration-700"
                                style={{ width: `${pct}%`, backgroundColor: item.color }}
                              />
                            </div>
                          </div>
                        );
                    })}
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Inventory Status */}
        <Card className="border-none shadow-md">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Package className="h-5 w-5 text-orange-600" />
              Tình trạng kho
            </CardTitle>
            <CardDescription>Phân bố tồn kho ({(inventory?.length || 0).toLocaleString()} đầu sách)</CardDescription>
          </CardHeader>
          <CardContent>
            {isInventoryLoading && (!inventory || inventory.length === 0) ? (
              <div className="flex items-center justify-center h-[250px] text-muted-foreground text-sm">
                <div className="text-center space-y-2">
                  <div className="h-6 w-6 border-2 border-primary border-t-transparent rounded-full animate-spin mx-auto" />
                  <p>Đang tải dữ liệu kho...</p>
                </div>
              </div>
            ) : (!inventory || inventory.length === 0) ? (
              <div className="flex items-center justify-center h-[250px] text-muted-foreground text-sm">
                Chưa có dữ liệu tồn kho
              </div>
            ) : (() => {
              const total = inventoryStatusData.reduce((s, i) => s + i.value, 0);
              return (
                <div className="flex flex-col gap-5">
                  {/* Donut + Center Label */}
                  <div className="flex items-center gap-6">
                    <div className="relative flex-shrink-0">
                      <ResponsiveContainer width={240} height={240}>
                        <PieChart>
                          <Pie
                            data={inventoryStatusData}
                            cx="50%" cy="50%"
                            innerRadius={80} outerRadius={110}
                            dataKey="value"
                            paddingAngle={3}
                            minAngle={4}
                            strokeWidth={0}
                          >
                            {inventoryStatusData.map((entry, i) => (
                              <Cell key={i} fill={entry.color} />
                            ))}
                          </Pie>
                          <Tooltip
                            formatter={(v: number) => [`${v.toLocaleString()} đầu sách`, '']}
                            contentStyle={{ borderRadius: 8, fontSize: 12 }}
                          />
                        </PieChart>
                      </ResponsiveContainer>
                      {/* Center label */}
                      <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                        <span className="text-2xl font-bold text-foreground leading-none">
                          {total.toLocaleString()}
                        </span>
                        <span className="text-[10px] text-muted-foreground mt-0.5">đầu sách</span>
                      </div>
                    </div>

                    {/* Legend */}
                    <div className="flex-1 space-y-3">
                      {inventoryStatusData.map((item, i) => {
                        const pct = total > 0 ? (item.value / total) * 100 : 0;
                        return (
                          <div key={i} className="space-y-1.5">
                            <div className="flex items-center justify-between text-sm">
                              <div className="flex items-center gap-2">
                                <span className="inline-block h-2.5 w-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: item.color }} />
                                <span className="text-foreground font-medium">{item.name}</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <span className="font-semibold text-foreground">{item.value.toLocaleString()}</span>
                                <span className="text-xs text-muted-foreground">({pct.toFixed(1)}%)</span>
                              </div>
                            </div>
                            {/* Progress bar */}
                            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
                              <div
                                className="h-full rounded-full transition-all duration-700"
                                style={{ width: `${pct}%`, backgroundColor: item.color }}
                              />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  {/* Footer note */}
                  <div 
                    className="border-t border-border flex flex-wrap items-center justify-center text-[11px] text-muted-foreground"
                    style={{ marginTop: '20px', paddingTop: '16px', gap: '24px' }}
                  >
                    <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: '#10b981' }}></span> Đủ hàng: &gt; 5</span>
                    <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: '#f59e0b' }}></span> Sắp hết: 1 - 5</span>
                    <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: '#ef4444' }}></span> Hết hàng: 0</span>
                  </div>
                </div>
              );
            })()}
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
