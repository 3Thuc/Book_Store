import React, { useState } from 'react';
import { useAdmin, Promotion } from './AdminContext';
import { Order, OrderStatus } from '../../types/order';
import { OrderWorkflowService } from '../../utils/orderWorkflowService';
import adminService from '../../services/adminService';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import {
  ShoppingCart,
  Package,
  CheckCircle,
  XCircle,
  Clock,
  CreditCard,
  PackageCheck,
  Truck,
  Home,
  RotateCcw,
  ChevronRight,
  Eye,
  ArrowRight,
  Loader2,
  Search
} from 'lucide-react';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { Separator } from '../../components/ui/separator';
import { formatVND } from '../../lib/formatters';
import PaginationControls from '../../components/admin/PaginationControls';
import { useEffect, useRef } from 'react';
import { toast } from 'sonner';

export const OrderManagement: React.FC = () => {
  const { orders, updateOrderStatus, refreshOrders } = useAdmin();
  const { promotions } = useAdmin();
  const [filterStatus, setFilterStatus] = useState<'all' | OrderStatus>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedOrder, setSelectedOrder] = useState<Order | null>(null);
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [promoCode, setPromoCode] = useState<string>('');
  const [appliedPromo, setAppliedPromo] = useState<Promotion | null>(null);
  const [discountAmount, setDiscountAmount] = useState<number>(0);
  const [promoError, setPromoError] = useState<string>('');
  const [updatingOrderId, setUpdatingOrderId] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);
  const [selectedOrderIds, setSelectedOrderIds] = useState<Set<string>>(new Set());
  const [isInitialLoading, setIsInitialLoading] = useState<boolean>(orders.length === 0);
  const [filterStartDate, setFilterStartDate] = useState<string>('');
  const [filterEndDate, setFilterEndDate] = useState<string>('');

  // Fetch orders on mount
  useEffect(() => {
    refreshOrders().finally(() => setIsInitialLoading(false));
  }, []);

  // Lắng nghe sự kiện từ Chatbot (Optimistic Update)
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail as {
        source?: string; role?: string;
        orderId?: number | null; newStatus?: string | null;
      };

      if (detail.orderId && detail.newStatus) {
        const id = String(detail.orderId);
        const newSt = detail.newStatus as OrderStatus;

        // Cập nhật trạng thái thông qua context
        updateOrderStatus(id, newSt);

        const statusLabels: Record<string, string> = {
          pending: 'Chờ xử lý', processing: 'Đang xử lý',
          shipped: 'Đang giao hàng', delivered: 'Đã giao',
          cancelled: 'Đã hủy', cancel_requested: 'Yêu cầu hủy',
          failed: 'Giao thất bại', returned: 'Đã hoàn hàng',
          return_requested: 'Yêu cầu hoàn',
        };
        const statusLabel = statusLabels[detail.newStatus] ?? detail.newStatus;

        // 🔔 Hiển thị toast thành công ngay lập tức
        toast.success(`✅ Đơn #${id} → ${statusLabel}`, {
          description: 'Cập nhật từ Staff AI Chatbot',
          duration: 4000,
        });
      } else if (detail.source === 'chatbot') {
        toast.info('🤖 Staff AI đã cập nhật dữ liệu', { duration: 3000 });
      }

      // Delay 2s để Java cache kịp evict trước khi fetch lại
      setTimeout(() => {
        refreshOrders();
      }, 2000);
    };

    window.addEventListener('bookstore:data-changed', handler);
    return () => window.removeEventListener('bookstore:data-changed', handler);
  }, []);

  const handleViewDetails = (order: Order) => {
    setSelectedOrder(order);
    setPromoCode('');
    setAppliedPromo(null);
    setDiscountAmount(0);
    setPromoError('');
    setDetailDialogOpen(true);
  };

  // Reset to page 1 and clear selection when filters change
  useEffect(() => {
    setCurrentPage(1);
    setSelectedOrderIds(new Set());
  }, [filterStatus, searchQuery, filterStartDate, filterEndDate]);

  // Clear selection when changing page
  useEffect(() => {
    setSelectedOrderIds(new Set());
  }, [currentPage]);

  // Base filtering (applied search and date query filters, but not status filter)
  const baseOrders = React.useMemo(() => {
    return orders.filter(order => {
      // 1. Search query Filter
      if (searchQuery.trim()) {
        const query = searchQuery.trim().toLowerCase();
        const customerName = (order.customerName || '').toLowerCase();
        const orderId = String(order.id).toLowerCase();
        const customerPhone = (order.customerPhone || '').toLowerCase();
        if (
          !customerName.includes(query) &&
          !orderId.includes(query) &&
          !customerPhone.includes(query)
        ) {
          return false;
        }
      }

      // 2. Date Filter
      if (filterStartDate) {
        const start = new Date(filterStartDate);
        start.setHours(0, 0, 0, 0);
        const orderTime = new Date(order.orderDate);
        if (isNaN(orderTime.getTime()) || orderTime < start) {
          return false;
        }
      }

      if (filterEndDate) {
        const end = new Date(filterEndDate);
        end.setHours(23, 59, 59, 999);
        const orderTime = new Date(order.orderDate);
        if (isNaN(orderTime.getTime()) || orderTime > end) {
          return false;
        }
      }

      return true;
    });
  }, [orders, searchQuery, filterStartDate, filterEndDate]);

  // Client-side filtering (applying status filter to baseOrders)
  const filteredOrders = React.useMemo(() => {
    if (filterStatus === 'all') return baseOrders;
    return baseOrders.filter(order => String(order.status).toUpperCase() === String(filterStatus).toUpperCase());
  }, [baseOrders, filterStatus]);

  // Calculate filtered orders count and revenue
  const filteredTotalItems = filteredOrders.length;

  const filteredRevenue = React.useMemo(() => {
    return filteredOrders
      .filter(o => {
        const s = String(o.status || '').toUpperCase();
        return s !== 'CANCELLED' && s !== 'RETURNED' && s !== 'FAILED';
      })
      .reduce((sum, o) => sum + o.totalAmount, 0);
  }, [filteredOrders]);

  // Client-side pagination
  const totalPages = Math.ceil(filteredTotalItems / pageSize) || 1;

  const pagedOrders = React.useMemo(() => {
    const startIndex = (currentPage - 1) * pageSize;
    return filteredOrders.slice(startIndex, startIndex + pageSize);
  }, [filteredOrders, currentPage, pageSize]);

  const formatCurrency = (amount: number) => formatVND(amount);

  const formatDate = (date: Date | string) => {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(dateObj.getTime())) {
      return 'N/A';
    }
    return new Intl.DateTimeFormat('vi-VN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }).format(dateObj);
  };

  const getStatusConfig = (status: OrderStatus) => {
    const configs: Record<string, any> = {
      PENDING: {
        label: 'Chờ xử lý',
        variant: 'secondary' as const,
        icon: Clock,
        color: 'text-yellow-600 bg-yellow-100'
      },
      PROCESSING: {
        label: 'Đang xử lý',
        variant: 'default' as const,
        icon: Package,
        color: 'text-blue-600 bg-blue-100'
      },
      SHIPPED: {
        label: 'Đang giao hàng',
        variant: 'default' as const,
        icon: Truck,
        color: 'text-indigo-600 bg-indigo-100'
      },
      DELIVERED: {
        label: 'Đã giao hàng',
        variant: 'default' as const,
        icon: Home,
        color: 'text-green-700 bg-green-200'
      },
      CANCEL_REQUESTED: {
        label: 'Yêu cầu hủy',
        variant: 'secondary' as const,
        icon: XCircle,
        color: 'text-orange-600 bg-orange-100'
      },
      CANCELLED: {
        label: 'Đã hủy',
        variant: 'destructive' as const,
        icon: XCircle,
        color: 'text-red-600 bg-red-100'
      },
      RETURN_REQUESTED: {
        label: 'Yêu cầu trả hàng',
        variant: 'secondary' as const,
        icon: RotateCcw,
        color: 'text-orange-600 bg-orange-100'
      },
      RETURNED: {
        label: 'Đã trả hàng',
        variant: 'destructive' as const,
        icon: RotateCcw,
        color: 'text-orange-700 bg-orange-200'
      },
      FAILED: {
        label: 'Thất bại',
        variant: 'destructive' as const,
        icon: XCircle,
        color: 'text-red-700 bg-red-200'
      },
    };
    return configs[String(status).toUpperCase() as OrderStatus] || {
      label: status,
      variant: 'secondary' as const,
      icon: Clock,
      color: 'text-gray-600 bg-gray-100'
    };
  };

  const getStatusBadge = (status: OrderStatus) => {
    const config = getStatusConfig(status);
    const Icon = config.icon;
    return (
      <Badge variant={config.variant} className="gap-1">
        <Icon className="h-3 w-3" />
        {config.label}
      </Badge>
    );
  };

  // Workflow: Các trạng thái tiếp theo có thể chuyển đến
  const getNextStatuses = (currentStatus: OrderStatus, order?: Order): OrderStatus[] => {
    const workflow: Partial<Record<string, OrderStatus[]>> = {
      PENDING: ['PROCESSING', 'CANCEL_REQUESTED'],
      PROCESSING: ['SHIPPED', 'CANCEL_REQUESTED'],
      SHIPPED: ['FAILED'],
      DELIVERED: [],
      CANCEL_REQUESTED: ['CANCELLED'],
      RETURN_REQUESTED: ['RETURNED'],
      CANCELLED: [],
      RETURNED: [],
      FAILED: ['PENDING'],   // Staff có thể re-queue để giao lại
    };

    // Normalize: Python chatbot trả lowercase ('processing'), Java trả UPPERCASE ('PROCESSING')
    // → chuẩn hóa về UPPERCASE để lookup đúng
    const key = String(currentStatus).toUpperCase();
    return (workflow[key] as OrderStatus[] | undefined) || [];
  };


  // Check if transition is auto or manual
  const isAutoStatus = (fromStatus: OrderStatus, toStatus: OrderStatus): boolean => {
    const autoTransitions: [OrderStatus, OrderStatus][] = [
      ['PROCESSING', 'SHIPPED'],
      ['SHIPPED', 'DELIVERED'],
    ];
    return autoTransitions.some(([from, to]) => from === fromStatus && to === toStatus);
  };

  // Get workflow help text
  const getWorkflowHelp = (currentStatus: OrderStatus): string => {
    const helps: Record<string, string> = {
      PENDING: 'Click "Xử lý" để bắt đầu xử lý hoặc "Yêu cầu hủy" để hủy đơn.',
      PROCESSING: 'Đơn đang được xử lý. Click "Giao hàng" khi sẵn sàng hoặc "Yêu cầu hủy" để hủy.',
      SHIPPED: 'Đơn sẽ tự động chuyển sang "Đã giao" khi khách nhận được hoặc click "Thất bại".',
      DELIVERED: 'Đơn hàng đã giao. Khách có thể yêu cầu trả hàng.',
      CANCEL_REQUESTED: 'Khách yêu cầu hủy. Click "Xác nhận hủy" để hoàn tất.',
      RETURN_REQUESTED: 'Khách yêu cầu trả hàng. Click "Duyệt trả" hoặc "Từ chối" để giữ nguyên.',
      CANCELLED: 'Đơn hàng đã bị hủy.',
      RETURNED: 'Đơn hàng đã được hoàn trả và nhập kho.',
      FAILED: 'Lỗi giao hàng.',
    };
    return helps[currentStatus] || '';
  };

  const handleQuickAction = async (order: Order, newStatus: OrderStatus) => {
    setUpdatingOrderId(order.id);
    const toastId = toast.loading('Đang cập nhật trạng thái...');
    try {
      // Gửi API để cập nhật trạng thái
      await adminService.updateOrderStatus(order.id, { status: newStatus });

      // Cập nhật trong global context
      updateOrderStatus(order.id, newStatus);

      // Cập nhật selectedOrder nếu đang xem chi tiết đơn này
      if (selectedOrder && String(selectedOrder.id) === String(order.id)) {
        setSelectedOrder(prev => prev ? { ...prev, status: newStatus } : null);
      }

      toast.success('Cập nhật trạng thái thành công', { id: toastId });

      // Làm mới dữ liệu trong nền
      refreshOrders();
    } catch (err) {
      toast.error('Lỗi khi cập nhật trạng thái', { id: toastId });
    } finally {
      setUpdatingOrderId(null);
    }
  };

  // Bulk actions
  const handleSelectAll = () => {
    if (selectedOrderIds.size === pagedOrders.length) {
      setSelectedOrderIds(new Set());
    } else {
      setSelectedOrderIds(new Set(pagedOrders.map(o => String(o.id))));
    }
  };

  const handleSelectOrder = (orderId: string) => {
    const newSet = new Set(selectedOrderIds);
    if (newSet.has(orderId)) {
      newSet.delete(orderId);
    } else {
      newSet.add(orderId);
    }
    setSelectedOrderIds(newSet);
  };

  const getBulkActions = (): { status: OrderStatus; label: string; icon: any }[] => {
    if (selectedOrderIds.size === 0) return [];

    const selectedOrders = pagedOrders.filter(o => selectedOrderIds.has(String(o.id)));
    const statuses = new Set(selectedOrders.map(o => o.status));

    if (statuses.size !== 1) return [];

    const currentStatus = Array.from(statuses)[0];

    const allowedBulkActions: Partial<Record<OrderStatus, OrderStatus[]>> = {
      PENDING: ['PROCESSING'],
      PROCESSING: ['SHIPPED'],
    };

    const allowedStatuses = allowedBulkActions[currentStatus] || [];
    if (allowedStatuses.length === 0) return [];

    return allowedStatuses.map(status => {
      const config = getStatusConfig(status);
      return {
        status,
        label: config.label,
        icon: config.icon,
      };
    });
  };

  const handleBulkAction = async (newStatus: OrderStatus) => {
    if (selectedOrderIds.size === 0) return;

    setUpdatingOrderId('bulk');
    const toastId = toast.loading(`Đang cập nhật ${selectedOrderIds.size} đơn hàng...`);
    try {
      const selectedOrders = pagedOrders.filter(o => selectedOrderIds.has(String(o.id)));

      // Cập nhật trạng thái từng đơn hàng
      for (const order of selectedOrders) {
        await adminService.updateOrderStatus(order.id, { status: newStatus });
        updateOrderStatus(order.id, newStatus);
      }

      toast.success(`Đã cập nhật ${selectedOrderIds.size} đơn hàng thành công`, { id: toastId });
      setSelectedOrderIds(new Set());

      // Làm mới dữ liệu trong nền
      refreshOrders();
    } catch (err) {
      console.error('Error bulk updating orders:', err);
      toast.error('Lỗi khi cập nhật hàng loạt', { id: toastId });
    } finally {
      setUpdatingOrderId(null);
    }
  };

  // Statistics fetched from dedicated API endpoint (accurate, not from partial context)
  // Computed statistics (dynamic based on current search and date filters)
  const statistics = React.useMemo(() => {
    const totalOrders = baseOrders.length;
    const pendingOrders = baseOrders.filter(o => String(o.status).toUpperCase() === 'PENDING').length;
    const returnRequestedOrders = baseOrders.filter(o => String(o.status).toUpperCase() === 'RETURN_REQUESTED').length;

    const totalRevenue = baseOrders
      .filter(o => {
        const s = String(o.status || '').toUpperCase();
        return s !== 'CANCELLED' && s !== 'RETURNED' && s !== 'FAILED';
      })
      .reduce((sum, o) => sum + o.totalAmount, 0);

    const deliveredRevenue = baseOrders
      .filter(o => String(o.status).toUpperCase() === 'DELIVERED')
      .reduce((sum, o) => sum + o.totalAmount, 0);

    const pendingRevenue = baseOrders
      .filter(o => ['PENDING', 'PROCESSING', 'SHIPPED'].includes(String(o.status).toUpperCase()))
      .reduce((sum, o) => sum + o.totalAmount, 0);

    return {
      totalOrders,
      pendingOrders,
      returnRequestedOrders,
      totalRevenue,
      deliveredRevenue,
      pendingRevenue
    };
  }, [baseOrders]);

  return (
    <div id="order-management" className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold">Quản lý đơn hàng</h2>
        <p className="text-muted-foreground">
          Theo dõi và xử lý đơn hàng của khách hàng • Tự động cập nhật mỗi 15 giây
        </p>
      </div>

      {/* Statistics Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Tổng đơn hàng</CardTitle>
            <ShoppingCart className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{statistics.totalOrders}</div>
            <p className="text-xs text-muted-foreground">
              +{statistics.pendingOrders} đơn chờ xử lý
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Chờ xử lý</CardTitle>
            <Clock className="h-4 w-4 text-yellow-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-yellow-600">{statistics.pendingOrders}</div>
            <p className="text-xs text-muted-foreground">
              Cần xác nhận
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Yêu cầu trả hàng</CardTitle>
            <RotateCcw className="h-4 w-4 text-orange-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-orange-600">{statistics.returnRequestedOrders}</div>
            <p className="text-xs text-muted-foreground">
              Cần xử lý
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              {filterStatus === 'all' ? 'Doanh thu thực nhận' : 'Doanh thu'}
            </CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatCurrency(
                filterStatus === 'all' ? statistics.deliveredRevenue : filteredRevenue
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              {filterStatus === 'all'
                ? 'Từ đơn hàng đã giao'
                : 'Tổng giá trị đơn hàng'}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <Card>
        <CardHeader>
          <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
            <div>
              <CardTitle>Danh sách đơn hàng</CardTitle>
              <CardDescription>
                {filteredTotalItems} đơn hàng
                {' • '}
                {filterStatus === 'all' ? (
                  <>
                    Doanh thu thực nhận: <span className="font-semibold text-emerald-600">{formatCurrency(statistics.deliveredRevenue)}</span>
                  </>
                ) : (
                  <>
                    Tổng doanh thu: <span className="font-semibold text-emerald-600">{formatCurrency(filteredRevenue)}</span>
                  </>
                )}
                {selectedOrderIds.size > 0 && (
                  <span className="ml-2 text-primary font-medium">
                    • Đã chọn {selectedOrderIds.size}
                  </span>
                )}
              </CardDescription>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {/* Date Filters */}
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-muted-foreground whitespace-nowrap">Từ:</span>
                <Input
                  type="date"
                  value={filterStartDate}
                  onChange={(e) => setFilterStartDate(e.target.value)}
                  className="h-9 w-[130px] text-xs px-2"
                />
                <span className="text-xs text-muted-foreground whitespace-nowrap">Đến:</span>
                <Input
                  type="date"
                  value={filterEndDate}
                  onChange={(e) => setFilterEndDate(e.target.value)}
                  className="h-9 w-[130px] text-xs px-2"
                />
                {(filterStartDate || filterEndDate) && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => { setFilterStartDate(''); setFilterEndDate(''); }}
                    className="h-9 px-2 text-xs"
                  >
                    Xóa ngày
                  </Button>
                )}
              </div>

              <div className="relative w-48">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Tìm kiếm..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-9 h-9 text-xs"
                />
              </div>
              {selectedOrderIds.size > 0 && getBulkActions().length > 0 && (
                <div className="flex items-center gap-2">
                  {getBulkActions().map(({ status, label, icon: Icon }) => {
                    const isBulkUpdating = updatingOrderId === 'bulk';
                    return (
                      <Button
                        key={status}
                        onClick={() => handleBulkAction(status)}
                        disabled={isBulkUpdating}
                        size="sm"
                        variant="default"
                        className="gap-1 text-xs h-9"
                      >
                        {isBulkUpdating ? <Loader2 className="h-3 w-3 animate-spin" /> : <Icon className="h-3 w-3" />}
                        {isBulkUpdating ? 'Đang xử lý...' : `${label} (${selectedOrderIds.size})`}
                      </Button>
                    );
                  })}
                  <Button
                    onClick={() => setSelectedOrderIds(new Set())}
                    size="sm"
                    variant="outline"
                    disabled={updatingOrderId === 'bulk'}
                    className="text-xs h-9"
                  >
                    Bỏ chọn
                  </Button>
                </div>
              )}
              <Select
                value={filterStatus}
                onValueChange={(value) => setFilterStatus(value as any)}
              >
                <SelectTrigger className="w-[150px] h-9 text-xs">
                  <SelectValue placeholder="Lọc theo trạng thái" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tất cả trạng thái</SelectItem>
                  <SelectItem value="PENDING">Chờ xử lý</SelectItem>
                  <SelectItem value="PROCESSING">Đang xử lý</SelectItem>
                  <SelectItem value="SHIPPED">Đang giao hàng</SelectItem>
                  <SelectItem value="DELIVERED">Đã giao hàng</SelectItem>
                  <SelectItem value="CANCEL_REQUESTED">Yêu cầu hủy</SelectItem>
                  <SelectItem value="CANCELLED">Đã hủy</SelectItem>
                  <SelectItem value="RETURN_REQUESTED">Yêu cầu trả hàng</SelectItem>
                  <SelectItem value="RETURNED">Đã trả hàng</SelectItem>
                  <SelectItem value="FAILED">Thất bại</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[40px]">
                    <input
                      type="checkbox"
                      checked={pagedOrders.length > 0 && selectedOrderIds.size === pagedOrders.length}
                      onChange={handleSelectAll}
                      className="cursor-pointer w-4 h-4"
                    />
                  </TableHead>
                  <TableHead>Mã đơn</TableHead>
                  <TableHead>Khách hàng</TableHead>
                  <TableHead>Ngày đặt</TableHead>
                  <TableHead>Tổng tiền</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead>Thao tác</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isInitialLoading ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                      <div className="flex items-center justify-center gap-2">
                        <Loader2 className="h-5 w-5 animate-spin text-primary" />
                        <span>Đang tải danh sách đơn hàng...</span>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : pagedOrders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                      Không có đơn hàng nào
                    </TableCell>
                  </TableRow>
                ) : (
                  pagedOrders.map((order) => {
                    const nextStatuses = getNextStatuses(order.status, order);
                    const isSelected = selectedOrderIds.has(String(order.id));
                    return (
                      <TableRow
                        key={order.id}
                        className={isSelected ? 'bg-muted/50' : ''}
                      >
                        <TableCell>
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => handleSelectOrder(String(order.id))}
                            className="cursor-pointer w-4 h-4"
                          />
                        </TableCell>
                        <TableCell className="font-medium">#{order.id}</TableCell>
                        <TableCell>
                          <div>
                            <p className="font-medium">{order.customerName || 'N/A'}</p>
                            <p className="text-sm text-muted-foreground">
                              {order.items.length} sản phẩm
                            </p>
                          </div>
                        </TableCell>
                        <TableCell>{formatDate(order.orderDate)}</TableCell>
                        <TableCell className="font-medium">
                          {formatCurrency(order.totalAmount)}
                        </TableCell>
                        <TableCell>{getStatusBadge(order.status)}</TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleViewDetails(order)}
                              disabled={updatingOrderId === order.id}
                            >
                              <Eye className="h-4 w-4 mr-1" />
                              Chi tiết
                            </Button>
                            {nextStatuses.length > 0 && (
                              <Select
                                key={`${order.id}-${order.status}`}
                                onValueChange={(value) => handleQuickAction(order, value as OrderStatus)}
                                disabled={updatingOrderId === order.id}
                              >
                                <SelectTrigger className="w-[140px] h-8">
                                  <SelectValue placeholder={updatingOrderId === order.id ? "Đang xử lý..." : "Cập nhật"} />
                                </SelectTrigger>
                                <SelectContent>
                                  {nextStatuses.map((status) => {
                                    const config = getStatusConfig(status);
                                    const Icon = config.icon;
                                    return (
                                      <SelectItem key={status} value={status}>
                                        <div className="flex items-center gap-2">
                                          <Icon className="h-3 w-3" />
                                          {config.label}
                                        </div>
                                      </SelectItem>
                                    );
                                  })}
                                </SelectContent>
                              </Select>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })
                )}
              </TableBody>
            </Table>
          </div>
          <PaginationControls
            totalItems={filteredTotalItems}
            currentPage={currentPage}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={setCurrentPage}
            onPageSizeChange={setPageSize}
            containerId="order-management"
          />
        </CardContent>
      </Card>

      {/* Order Detail Dialog */}
      <Dialog open={detailDialogOpen} onOpenChange={setDetailDialogOpen}>
        <DialogContent className="max-w-6xl w-[95vw] max-h-[90vh] overflow-y-auto p-6">
          <DialogHeader>
            <DialogTitle>Chi tiết đơn hàng #{selectedOrder?.id}</DialogTitle>
            <DialogDescription>
              Thông tin chi tiết về đơn hàng
            </DialogDescription>
          </DialogHeader>

          {selectedOrder && (
            <div className="space-y-4 overflow-y-auto max-h-[calc(90vh-150px)]">
              {/* Order Info */}
              <div className="grid grid-cols-4 gap-4">
                <div>
                  <p className="text-sm text-muted-foreground">Khách hàng</p>
                  <p className="font-medium text-sm">{selectedOrder.customerName || 'N/A'}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Số điện thoại</p>
                  <p className="font-medium text-sm">{selectedOrder.customerPhone || 'N/A'}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Ngày đặt hàng</p>
                  <p className="font-medium text-sm">{formatDate(selectedOrder.orderDate)}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Phương thức</p>
                  <p className="font-medium text-sm">{selectedOrder.paymentMethod || 'COD'}</p>
                </div>
                <div className="col-span-4">
                  <p className="text-sm text-muted-foreground">Địa chỉ giao hàng</p>
                  <p className="font-medium text-sm">{selectedOrder.shippingAddress || 'N/A'}</p>
                </div>
                {selectedOrder.note && (
                  <div className="col-span-4">
                    <p className="text-sm text-muted-foreground">Ghi chú</p>
                    <p className="font-medium text-sm">{selectedOrder.note}</p>
                  </div>
                )}
              </div>

              <Separator className="my-2" />

              {/* Order Items */}
              <div>
                <h4 className="font-semibold text-sm mb-2">Sản phẩm ({selectedOrder.items.length})</h4>
                <div className="space-y-2 max-h-48 overflow-y-auto">
                  {selectedOrder.items.map((item) => {
                    console.log('Order item:', item);
                    return (
                      <div key={item.id} className="flex items-center gap-3 p-2 border rounded text-sm">
                        <div className="relative w-12 h-16 flex-shrink-0">
                          {item.imageUrl ? (
                            <img
                              src={item.imageUrl}
                              alt={item.title}
                              className="absolute inset-0 w-full h-full object-cover rounded border"
                              onError={(e) => {
                                console.error('Image failed to load:', item.imageUrl);
                                e.currentTarget.style.display = 'none';
                                const fallback = e.currentTarget.nextElementSibling;
                                if (fallback) (fallback as HTMLElement).style.display = 'flex';
                              }}
                            />
                          ) : null}
                          <div
                            className="absolute inset-0 w-full h-full bg-gray-100 rounded border flex items-center justify-center"
                            style={{ display: item.imageUrl ? 'none' : 'flex' }}
                          >
                            <div className="text-xs text-gray-400 text-center px-1">
                              <div>Không</div>
                              <div>ảnh</div>
                            </div>
                          </div>
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="font-medium truncate text-sm">{item.title}</p>
                          <p className="text-xs text-muted-foreground">{item.author}</p>
                        </div>
                        <div className="text-right whitespace-nowrap">
                          <p className="font-medium text-sm">{formatCurrency(item.price / item.quantity)}</p>
                          <p className="text-xs text-muted-foreground">x{item.quantity}</p>
                        </div>
                        <div className="font-medium text-sm">
                          {formatCurrency(item.price)}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <Separator className="my-2" />

              {/* Total */}
              <div className="flex flex-col gap-2">
                <div className="flex justify-between items-center">
                  <span className="font-semibold text-sm">Tổng cộng:</span>
                  <span className="font-bold text-primary text-sm">
                    {formatCurrency(selectedOrder.totalAmount)}
                  </span>
                </div>

                {/* Chỉ hiển thị mã giảm giá của đơn hàng nếu có */}
                {selectedOrder.promoCode ? (
                  <div className="flex justify-between items-center">
                    <span className="text-sm">Mã giảm giá áp dụng:</span>
                    <span className="font-medium">{selectedOrder.promoCode}</span>
                  </div>
                ) : (
                  <div className="flex justify-between items-center">
                    <span className="text-sm">Mã giảm giá áp dụng:</span>
                    <span className="font-medium text-muted-foreground">Không có</span>
                  </div>
                )}
              </div>

              {/* Status Workflow */}
              <div>
                <h4 className="font-semibold text-sm mb-2">Luồng trạng thái</h4>
                <div className="flex flex-wrap items-center gap-1">
                  {(() => {
                    const workflowArray: OrderStatus[] = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED'];

                    return workflowArray.map((status, index, arr) => {
                      const config = getStatusConfig(status);
                      const Icon = config.icon;
                      const isCurrent = selectedOrder.status === status;

                      // Logic để xác định trạng thái đã qua (past)
                      let isPast = false;
                      const currentStatusIndex = arr.indexOf(selectedOrder.status);
                      if (currentStatusIndex > -1 && index < currentStatusIndex) {
                        isPast = true;
                      }

                      return (
                        <React.Fragment key={status}>
                          <div
                            className={`
                              flex items-center gap-1 px-2 py-1 rounded border text-xs transition-all
                              ${isCurrent ? 'border-primary bg-primary/10 font-semibold' : ''}
                              ${isPast ? 'border-green-500 bg-green-50' : 'border-border'}
                              ${!isCurrent && !isPast ? 'opacity-50' : ''}
                            `}
                          >
                            <Icon className={`h-3 w-3 ${isCurrent ? 'text-primary' : isPast ? 'text-green-600' : ''}`} />
                            <span className="text-xs">{config.label}</span>
                          </div>
                          {index < arr.length - 1 && (
                            <ChevronRight className="h-3 w-3 text-muted-foreground" />
                          )}
                        </React.Fragment>
                      );
                    });
                  })()}
                </div>
              </div>

              {/* Quick Actions */}
              {getNextStatuses(selectedOrder.status, selectedOrder).length > 0 && (
                <>
                  <Separator className="my-2" />
                  <div>
                    <h4 className="font-semibold text-sm mb-2">Cập nhật trạng thái</h4>
                    <p className="text-xs text-muted-foreground mb-2">
                      {getWorkflowHelp(selectedOrder.status)}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {getNextStatuses(selectedOrder.status, selectedOrder).map((status) => {
                        const config = getStatusConfig(status);
                        const Icon = config.icon;
                        const isAutoTransition = isAutoStatus(selectedOrder.status, status);

                        const isUpdating = updatingOrderId === selectedOrder.id;
                        return (
                          <Button
                            key={status}
                            onClick={async () => {
                              await handleQuickAction(selectedOrder, status);
                              setDetailDialogOpen(false);
                            }}
                            disabled={isUpdating}
                            size="sm"
                            className="gap-1 text-xs"
                            variant="default"
                          >
                            {isUpdating ? <Loader2 className="h-3 w-3 animate-spin" /> : <Icon className="h-3 w-3" />}
                            {isUpdating ? 'Đang xử lý...' : config.label}
                          </Button>
                        );
                      })}
                    </div>
                  </div>
                </>
              )}
            </div>
          )}

          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setDetailDialogOpen(false)} size="sm">
              Đóng
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
