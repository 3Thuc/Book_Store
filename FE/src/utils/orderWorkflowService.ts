import { Order, OrderStatus } from '../types/order';
import { toast } from 'sonner';

/**
 * Order Workflow Service - Quản lý luồng tự động chuyển trạng thái đơn hàng
 * 
 * Logic:
 * PENDING → PAID (auto từ payment webhook)
 * PAID → CONFIRMED (auto ngay lập tức)
 * CONFIRMED → PACKING (auto sau 1-2 giờ)
 * PACKING → SHIPPED (admin click)
 * SHIPPED → DELIVERED (auto sau 2-3 ngày hoặc khách xác nhận)
 */

export class OrderWorkflowService {
  private static ORDERS_KEY_PREFIX = 'orders_';
  private static ADMIN_ORDERS_KEY = 'admin_orders';

  // Thời gian tự động chuyển (milliseconds)
  private static TRANSITIONS = {
    PAID_TO_CONFIRMED: 5000, // 5 giây (demo - thực tế: ngay lập tức)
    CONFIRMED_TO_PACKING: 10000, // 10 giây (demo - thực tế: 1-2 giờ)
    SHIPPED_TO_DELIVERED: 20000, // 20 giây (demo - thực tế: 2-3 ngày)
  };

  /**
   * Lấy tất cả orders từ localStorage
   */
  private static getAllOrders(): Order[] {
    const adminOrdersData = localStorage.getItem(this.ADMIN_ORDERS_KEY);
    if (adminOrdersData) {
      return JSON.parse(adminOrdersData, (key, value) => {
        if (key === 'orderDate' || key === 'deliveryDate') {
          return value ? new Date(value) : undefined;
        }
        return value;
      });
    }
    return [];
  }

  /**
   * Lưu orders
   */
  private static saveOrders(orders: Order[]): void {
    localStorage.setItem(this.ADMIN_ORDERS_KEY, JSON.stringify(orders));
    
    // Trigger storage event để update UI
    window.dispatchEvent(new Event('storage'));
  }

  /**
   * Cập nhật trạng thái đơn hàng
   */
  private static updateOrderStatus(orderId: string, newStatus: OrderStatus): void {
    const orders = this.getAllOrders();
    const orderIndex = orders.findIndex(o => o.id === orderId);
    
    if (orderIndex === -1) return;
    
    const order = orders[orderIndex];
    const oldStatus = order.status;
    
    // Update status
    orders[orderIndex] = { ...order, status: newStatus };
    this.saveOrders(orders);
    
    console.log(`🔄 Auto-transition: ${orderId} from ${oldStatus} to ${newStatus}`);
  }

  /**
   * Schedule auto-transition cho 1 đơn hàng
   */
  private static scheduleTransition(
    orderId: string,
    fromStatus: OrderStatus,
    toStatus: OrderStatus,
    delay: number
  ): void {
    setTimeout(() => {
      const orders = this.getAllOrders();
      const order = orders.find(o => o.id === orderId);
      
      // Chỉ chuyển nếu order vẫn ở trạng thái cũ
      if (order && order.status === fromStatus) {
        this.updateOrderStatus(orderId, toStatus);
        
        // Schedule tiếp cho transition tiếp theo (nếu có)
        this.scheduleNextTransition(orderId, toStatus);
      }
    }, delay);
  }

  /**
   * Schedule transition tiếp theo (nếu có)
   */
  private static scheduleNextTransition(orderId: string, currentStatus: OrderStatus): void {
    switch (currentStatus) {
      case 'PAID':
        // PAID → CONFIRMED (ngay lập tức)
        this.scheduleTransition(
          orderId,
          'PAID',
          'CONFIRMED',
          this.TRANSITIONS.PAID_TO_CONFIRMED
        );
        break;
        
      case 'CONFIRMED':
        // CONFIRMED → PACKING (sau 1-2 giờ)
        this.scheduleTransition(
          orderId,
          'CONFIRMED',
          'PACKING',
          this.TRANSITIONS.CONFIRMED_TO_PACKING
        );
        break;
        
      case 'SHIPPED':
        // SHIPPED → DELIVERED (sau 2-3 ngày)
        this.scheduleTransition(
          orderId,
          'SHIPPED',
          'DELIVERED',
          this.TRANSITIONS.SHIPPED_TO_DELIVERED
        );
        break;
    }
  }

  /**
   * Simulate payment webhook - chuyển PENDING → PAID
   */
  static processPayment(orderId: string): void {
    this.updateOrderStatus(orderId, 'PAID');
    toast.success('Thanh toán thành công!');
    
    // Schedule auto-transition tiếp theo
    this.scheduleNextTransition(orderId, 'PAID');
  }

  /**
   * Admin xác nhận đơn COD - chuyển PENDING → CONFIRMED
   */
  static confirmCODOrder(orderId: string): void {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return;
    
    // Update order status to CONFIRMED
    this.updateOrderStatus(orderId, 'CONFIRMED');
    toast.success('Đã xác nhận đơn COD');
    
    // Schedule auto-transition tiếp theo
    this.scheduleNextTransition(orderId, 'CONFIRMED');
  }

  /**
   * Admin giao hàng - chuyển PACKING → SHIPPED
   */
  static shipOrder(orderId: string): void {
    this.updateOrderStatus(orderId, 'SHIPPED');
    toast.success('Đơn hàng đã được giao cho đơn vị vận chuyển');
    
    // Schedule auto-transition tiếp theo
    this.scheduleNextTransition(orderId, 'SHIPPED');
  }

  /**
   * Khách hủy đơn (chỉ khi PENDING)
   */
  static customerCancelOrder(orderId: string): boolean {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return false;
    
    // Chỉ cho phép hủy khi PENDING
    if (order.status !== 'PENDING') {
      toast.error('Không thể hủy đơn hàng đã xác nhận');
      return false;
    }
    
    this.updateOrderStatus(orderId, 'CANCELLED');
    toast.success('Đã hủy đơn hàng');
    return true;
  }

  /**
   * Admin hủy đơn (bất kỳ trạng thái nào trừ DELIVERED)
   */
  static adminCancelOrder(orderId: string): boolean {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return false;
    
    if (order.status === 'DELIVERED') {
      toast.error('Không thể hủy đơn hàng đã giao');
      return false;
    }
    
    this.updateOrderStatus(orderId, 'CANCELLED');
    toast.success('Đã hủy đơn hàng');
    return true;
  }

  /**
   * Khách yêu cầu trả hàng (chỉ cho DELIVERED)
   */
  static customerRequestReturn(orderId: string, reason?: string): boolean {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return false;
    
    if (order.status !== 'DELIVERED') {
      toast.error('Chỉ có thể yêu cầu trả hàng cho đơn đã giao');
      return false;
    }
    
    // Update status to RETURN_REQUESTED
    this.updateOrderStatus(orderId, 'RETURN_REQUESTED');
    
    // Create notification for admin
    this.createReturnRequestNotification(orderId, reason);
    
    toast.success('Yêu cầu trả hàng đã được gửi. Admin sẽ xử lý trong thời gian sớm nhất.');
    return true;
  }

  /**
   * Admin duyệt hoàn hàng (RETURN_REQUESTED → RETURNED)
   */
  static approveReturn(orderId: string): void {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return;
    
    if (order.status !== 'RETURN_REQUESTED') {
      toast.error('Đơn hàng chưa có yêu cầu trả hàng');
      return;
    }
    
    this.updateOrderStatus(orderId, 'RETURNED');
    toast.success('Đã duyệt trả hàng. Hàng đã được hoàn về kho.');
  }

  /**
   * Admin từ chối trả hàng (RETURN_REQUESTED → DELIVERED)
   */
  static rejectReturn(orderId: string): void {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return;
    
    if (order.status !== 'RETURN_REQUESTED') {
      toast.error('Đơn hàng chưa có yêu cầu trả hàng');
      return;
    }
    
    this.updateOrderStatus(orderId, 'DELIVERED');
    toast.success('Đã từ chối yêu cầu trả hàng');
  }

  /**
   * Create notification for return request
   */
  private static createReturnRequestNotification(orderId: string, reason?: string): void {
    const notifications = this.getNotifications();
    
    notifications.unshift({
      id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      type: 'return_requested',
      title: 'Yêu cầu trả hàng',
      message: `Khách hàng yêu cầu trả hàng đơn #${orderId}${reason ? `: ${reason}` : ''}`,
      time: 'Vừa xong',
      isRead: false,
      orderId: orderId,
    });

    this.saveNotifications(notifications);
  }

  /**
   * Get notifications from localStorage
   */
  private static getNotifications(): any[] {
    const data = localStorage.getItem('admin_notifications');
    return data ? JSON.parse(data) : [];
  }

  /**
   * Save notifications to localStorage
   */
  private static saveNotifications(notifications: any[]): void {
    localStorage.setItem('admin_notifications', JSON.stringify(notifications));
  }

  /**
   * Khách xác nhận đã nhận hàng
   */
  static customerConfirmDelivery(orderId: string): void {
    const orders = this.getAllOrders();
    const order = orders.find(o => o.id === orderId);
    
    if (!order) return;
    
    if (order.status !== 'SHIPPED') {
      toast.error('Đơn hàng chưa được giao');
      return;
    }
    
    this.updateOrderStatus(orderId, 'DELIVERED');
    toast.success('Cảm ơn bạn đã xác nhận! Hãy đánh giá sản phẩm nhé.');
  }

  /**
   * Khởi động auto-transitions cho orders đang pending
   * Gọi khi app load
   */
  static initAutoTransitions(): void {
    const orders = this.getAllOrders();
    
    orders.forEach(order => {
      const now = new Date().getTime();
      const orderTime = new Date(order.orderDate).getTime();
      const elapsed = now - orderTime;
      
      // Schedule transitions based on current status
      switch (order.status) {
        case 'PAID':
          if (elapsed < this.TRANSITIONS.PAID_TO_CONFIRMED) {
            this.scheduleTransition(
              order.id,
              'PAID',
              'CONFIRMED',
              this.TRANSITIONS.PAID_TO_CONFIRMED - elapsed
            );
          }
          break;
          
        case 'CONFIRMED':
          if (elapsed < this.TRANSITIONS.CONFIRMED_TO_PACKING) {
            this.scheduleTransition(
              order.id,
              'CONFIRMED',
              'PACKING',
              this.TRANSITIONS.CONFIRMED_TO_PACKING - elapsed
            );
          }
          break;
          
        case 'SHIPPED':
          // Check if enough time has passed
          const shippedTime = orderTime; // In real app, track when it was shipped
          const shippedElapsed = now - shippedTime;
          
          if (shippedElapsed < this.TRANSITIONS.SHIPPED_TO_DELIVERED) {
            this.scheduleTransition(
              order.id,
              'SHIPPED',
              'DELIVERED',
              this.TRANSITIONS.SHIPPED_TO_DELIVERED - shippedElapsed
            );
          }
          break;
      }
    });
    
    console.log('Order auto-transitions initialized');
  }

  /**
   * Mock payment - để test
   */
  static mockPayment(orderId: string): void {
    setTimeout(() => {
      this.processPayment(orderId);
    }, 2000); // Giả lập delay payment gateway
  }
}
