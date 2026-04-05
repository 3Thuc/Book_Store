import { OrderStatus } from '../types/order';

/**
 * Migrate old order status to new OrderStatus format
 * Chuyển đổi từ lowercase sang UPPERCASE
 */
export const migrateOrderStatus = () => {
  // Map old status to new status
  const statusMap: Record<string, OrderStatus> = {
    'pending': 'PENDING',
    'paid': 'PAID',
    'confirmed': 'CONFIRMED',
    'packing': 'PACKING',
    'shipped': 'SHIPPED',
    'completed': 'DELIVERED', // Old 'completed' -> new 'DELIVERED'
    'delivered': 'DELIVERED',
    'cancelled': 'CANCELLED',
    'canceled': 'CANCELLED', // Handle typo
    'returned': 'RETURNED',
  };

  try {
    // Migrate admin_orders
    const adminOrdersData = localStorage.getItem('admin_orders');
    if (adminOrdersData) {
      const orders = JSON.parse(adminOrdersData);
      const migratedOrders = orders.map((order: any) => {
        if (order.status && typeof order.status === 'string') {
          const oldStatus = order.status.toLowerCase();
          const newStatus = statusMap[oldStatus] || 'PENDING';
          return { ...order, status: newStatus };
        }
        return order;
      });
      localStorage.setItem('admin_orders', JSON.stringify(migratedOrders));
      console.log('Migrated admin_orders');
    }

    // Migrate user orders (orders_userId format)
    const allKeys = Object.keys(localStorage);
    const userOrderKeys = allKeys.filter(key => key.startsWith('orders_'));
    
    userOrderKeys.forEach(key => {
      const ordersData = localStorage.getItem(key);
      if (ordersData) {
        const orders = JSON.parse(ordersData);
        const migratedOrders = orders.map((order: any) => {
          if (order.status && typeof order.status === 'string') {
            const oldStatus = order.status.toLowerCase();
            const newStatus = statusMap[oldStatus] || 'PENDING';
            return { ...order, status: newStatus };
          }
          return order;
        });
        localStorage.setItem(key, JSON.stringify(migratedOrders));
        console.log(`Migrated ${key}`);
      }
    });

    console.log('Order status migration completed');
  } catch (error) {
    console.error('❌ Order status migration failed:', error);
  }
};
