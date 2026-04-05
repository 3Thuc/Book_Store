// Order status type
export type OrderStatus = 
  | 'PENDING'           // Chờ xử lý - vừa đặt hàng
  | 'PAID'              // Đã thanh toán
  | 'PROCESSING'        // Đang xử lý
  | 'CONFIRMED'         // Đã xác nhận
  | 'PACKING'           // Đang đóng gói
  | 'SHIPPED'           // Đang giao hàng
  | 'DELIVERED'         // Đã giao hàng
  | 'CANCEL_REQUESTED'  // Yêu cầu hủy đơn (chờ admin duyệt)
  | 'CANCELLED'         // Đã hủy
  | 'RETURN_REQUESTED'  // Khách yêu cầu trả hàng
  | 'RETURNED'         // Đã trả hàng (admin đã duyệt)
  | 'FAILED';           // Giao dịch thất bại

// Order status constants
export const ORDER_STATUS = {
  PENDING: 'PENDING' as OrderStatus,
  PAID: 'PAID' as OrderStatus,
  PROCESSING: 'PROCESSING' as OrderStatus,
  CONFIRMED: 'CONFIRMED' as OrderStatus,
  PACKING: 'PACKING' as OrderStatus,
  SHIPPED: 'SHIPPED' as OrderStatus,
  DELIVERED: 'DELIVERED' as OrderStatus,
  CANCEL_REQUESTED: 'CANCEL_REQUESTED' as OrderStatus,
  CANCELLED: 'CANCELLED' as OrderStatus,
  RETURN_REQUESTED: 'RETURN_REQUESTED' as OrderStatus,
  RETURNED: 'RETURNED' as OrderStatus,
  FAILED: 'FAILED' as OrderStatus,
};

// Payment method type
export type PaymentMethod = 'COD' | 'BANKING';

// Payment method constants
export const PAYMENT_METHODS = {
  COD: 'COD' as PaymentMethod,
  BANKING: 'BANKING' as PaymentMethod,
};

// OrderItem interface
export interface OrderItem {
  id: string;
  bookId: string;
  title: string;
  author: string;
  price: number;
  quantity: number;
  imageUrl: string;
  isReviewed: boolean;
}

// Order interface
export interface Order {
  id: string;
  userId: string;
  items: OrderItem[];
  totalAmount: number;
  orderDate: Date;
  status: OrderStatus;
  deliveryDate: Date;
  promoCode?: string;
  paymentMethod?: PaymentMethod;
  shippingAddress?: string;
  customerName?: string;
  customerPhone?: string;
  note?: string;
  isPaid?: boolean;
}

// Review interface
export interface Review {
  id: string;
  bookId: string;
  userId: string;
  userName: string;
  rating: number;
  title: string;
  content: string;
  date: Date;
  helpful: number;
  unhelpful: number;
  isVerifiedPurchase: boolean;
}

// WriteReviewData interface
export interface WriteReviewData {
  bookId: string;
  rating: number;
  title: string;
  content: string;
}
