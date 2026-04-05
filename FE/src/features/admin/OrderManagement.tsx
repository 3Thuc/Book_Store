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
  const [totalPages, setTotalPages] = useState<number>(1);
  const [totalItems, setTotalItems] = useState<number>(0);
  const [paginatedOrders, setPaginatedOrders] = useState<Order[]>([]); // Track paginated data separately
  const [isLoadingPage, setIsLoadingPage] = useState<boolean>(true);
  const prevOrdersLength = useRef(orders.length);
  const [selectedOrderIds, setSelectedOrderIds] = useState<Set<string>>(new Set());

  // Check for new orders and show notification
  useEffect(() => {
    if (orders.length > prevOrdersLength.current) {
      const newOrdersCount = orders.length - prevOrdersLength.current;
    }
    prevOrdersLength.current = orders.length;
  }, [orders.length]);

  // Fetch orders when page, pageSize, filter, or search changes
  useEffect(() => {
    const fetchOrders = async () => {
      setIsLoadingPage(true);
      try {
        const params: any = { 
          page: currentPage, 
          size: pageSize 
        };
        
        // Add status filter if not 'all'
        if (filterStatus !== 'all') {
          params.status = filterStatus;
        }
        
        // Add search query if exists
        if (searchQuery.trim()) {
          params.search = searchQuery.trim();
        }
        
        const response = await adminService.getOrders(params);
        const data = response?.result?.books ?? response?.result?.data ?? response?.result ?? [];
        const pagination = response?.result;
        
        console.log('Orders response:', response);
        console.log('Orders data:', data);
        console.log('Pagination:', pagination);
        
        if (Array.isArray(data)) {
          // Set paginated orders directly from API response (not from context)
          setPaginatedOrders(data);
          
          // Update pagination info if available
          if (pagination?.totalPages) setTotalPages(pagination.totalPages);
          if (pagination?.totalElements !== undefined) setTotalItems(pagination.totalElements);
        }
      } catch (error) {
        console.error('Error fetching orders:', error);
        setPaginatedOrders([]);
      } finally {
        setIsLoadingPage(false);
      }
    };
    
    fetchOrders();
  }, [currentPage, pageSize, filterStatus, searchQuery]);

  // Auto refresh orders every 30 seconds (increased to reduce server load)
  useEffect(() => {
    const interval = setInterval(() => {
      console.log('Auto refreshing orders...');
      const params: any = { page: currentPage, size: pageSize };
      if (filterStatus !== 'all') params.status = filterStatus;
      if (searchQuery.trim()) params.search = searchQuery.trim();
      refreshOrders(params);
    }, 30000); // 30 seconds

    return () => clearInterval(interval);
  }, [refreshOrders, currentPage, pageSize, filterStatus, searchQuery]);

  const handleViewDetails = (order: Order) => {
    setSelectedOrder(order);
    // reset promo UI when opening a different order
    setPromoCode('');
    setAppliedPromo(null);
    setDiscountAmount(0);
    setPromoError('');
    setDetailDialogOpen(true);
  };

  // Reset to page 1 when filter changes
  useEffect(() => {
    setCurrentPage(1);
    setSelectedOrderIds(new Set()); // Clear selection when filter changes
  }, [filterStatus, searchQuery]);

  // Clear selection when changing page
  useEffect(() => {
    setSelectedOrderIds(new Set());
  }, [currentPage]);

  // Use paginated orders from API response (not full context orders)
  const pagedOrders = paginatedOrders;

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
    return configs[status] || { 
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
    const workflow: Partial<Record<OrderStatus, OrderStatus[]>> = {
      PENDING: ['PROCESSING', 'CANCEL_REQUESTED'],
      PROCESSING: ['SHIPPED', 'CANCEL_REQUESTED'],
      SHIPPED: ['FAILED'],
      DELIVERED: [],
      CANCEL_REQUESTED: ['CANCELLED'],
      RETURN_REQUESTED: ['RETURNED'],
      CANCELLED: [],
      RETURNED: [],
      FAILED: [],
    };

    return (workflow[currentStatus] as OrderStatus[] | undefined) || [];
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
      
      // Fetch lại dữ liệu từ API ngay lập tức
      const params: any = { page: currentPage, size: pageSize };
      if (filterStatus !== 'all') params.status = filterStatus;
      if (searchQuery.trim()) params.search = searchQuery.trim();
      
      const response = await adminService.getOrders(params);
      const data = response?.result?.books ?? response?.result?.data ?? response?.result ?? [];
      
      if (Array.isArray(data)) {
        setPaginatedOrders(data);
        
        // Cập nhật selectedOrder với dữ liệu mới từ API
        const updatedOrder = data.find(o => String(o.id) === String(order.id));
        if (updatedOrder) {
          setSelectedOrder(updatedOrder);
        }
      }
      
      // Cũng refresh context orders
      await refreshOrders(params);
      
      toast.success('Cập nhật trạng thái thành công', { id: toastId });
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
      
      // Cập nhật tuần tự từng đơn
      for (const order of selectedOrders) {
        await adminService.updateOrderStatus(order.id, { status: newStatus });
      }
      
      // Fetch lại dữ liệu từ API ngay lập tức
      const params: any = { page: currentPage, size: pageSize };
      if (filterStatus !== 'all') params.status = filterStatus;
      if (searchQuery.trim()) params.search = searchQuery.trim();
      
      const response = await adminService.getOrders(params);
      const data = response?.result?.books ?? response?.result?.data ?? response?.result ?? [];
      
      if (Array.isArray(data)) {
        setPaginatedOrders(data);
      }
      
      // Cũng refresh context orders
      await refreshOrders(params);
      
      toast.success(`Đã cập nhật ${selectedOrderIds.size} đơn hàng thành công`, { id: toastId });
      setSelectedOrderIds(new Set());
    } catch (err) {
      console.error('Error bulk updating orders:', err);
      toast.error('Lỗi khi cập nhật hàng loạt', { id: toastId });
    } finally {
      setUpdatingOrderId(null);
    }
  };

  // Statistics from context orders (same as dashboard for consistency)
  const [statistics, setStatistics] = useState<{
    totalOrders: number;
    pendingOrders: number;
    returnRequestedOrders: number;
    totalRevenue: number;
  }>({
    totalOrders: 0,
    pendingOrders: 0,
    returnRequestedOrders: 0,
    totalRevenue: 0,
  });

  // Calculate statistics from orders context (same logic as dashboard)
  useEffect(() => {
    if (orders.length > 0) {
      const totalRevenue = orders
        .filter(o => o.status !== 'CANCELLED' && o.status !== 'RETURNED')
        .reduce((sum, o) => sum + o.totalAmount, 0);

      const pendingOrders = orders.filter(o => o.status === 'PENDING').length;
      const returnRequestedOrders = orders.filter(o => o.status === 'RETURN_REQUESTED').length;

      setStatistics({
        totalOrders: orders.length,
        pendingOrders,
        returnRequestedOrders,
        totalRevenue,
      });
    }
  }, [orders]);

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
            <CardTitle className="text-sm font-medium">Doanh thu</CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{formatCurrency(statistics.totalRevenue)}</div>
            <p className="text-xs text-muted-foreground">
              Tổng giá trị đơn hàng
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
                {totalItems > 0 ? `${totalItems} đơn hàng` : `${orders.length} đơn hàng`}
                {selectedOrderIds.size > 0 && (
                  <span className="ml-2 text-primary font-medium">
                    • Đã chọn {selectedOrderIds.size}
                  </span>
                )}
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <div className="relative w-64">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Tìm theo mã đơn hàng..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-9 h-9"
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
                        className="gap-1"
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
                  >
                    Bỏ chọn
                  </Button>
                </div>
              )}
              <Select
                value={filterStatus}
                onValueChange={(value) => setFilterStatus(value as any)}
              >
                <SelectTrigger className="w-[200px]">
                  <SelectValue placeholder="Lọc theo trạng thái" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tất cả</SelectItem>
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
                {pagedOrders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground">
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
            totalItems={totalItems}
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
                        <p className="font-medium text-sm">{formatCurrency(item.price)}</p>
                        <p className="text-xs text-muted-foreground">x{item.quantity}</p>
                      </div>
                        <div className="font-medium text-sm">
                          {formatCurrency(item.price * item.quantity)}
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
