import React, { createContext, useContext, useReducer, useEffect, ReactNode } from 'react';
import { useAuth } from './AuthContext';
import { toast } from 'sonner';
import { OrderStatus } from '../types/order';
import OrderService from '../services/orderService';
import  ReviewService  from '../services/reviewService';

interface OrderItem {
  id: string;
  bookId: string;
  title: string;
  author: string;
  price: number;
  quantity: number;
  imageUrl: string;
  isReviewed: boolean;
}

interface Order {
  id: string;
  userId: string;
  items: OrderItem[];
  totalAmount: number;
  orderDate: Date;
  status: OrderStatus;
  deliveryDate: Date;
  paymentMethod?: 'COD' | 'BANKING' | 'MOMO' | 'VNPAY';
  shippingAddress?: string;
  customerName?: string;
  customerPhone?: string;
  note?: string;
  isPaid?: boolean;
}

interface Review {
  rating_id: string;                    
  book_id: string;
  user_id: string;
  rating: number;
  review: string;                       
  status: 'pending' | 'approved' | 'rejected';
  created_at: Date;
  updated_at: Date;
}

interface ReviewData {
  book_id: string;
  rating: number;
  review: string;                       
}

interface CheckoutData {
  paymentMethod?: 'COD' | 'BANKING' | 'MOMO' | 'VNPAY';
  shippingAddress?: string;
  customerName?: string;
  customerPhone?: string;
  note?: string;
}

interface OrderState {
  orders: Order[];
  reviews: Review[];
}

type OrderAction = 
  | { type: 'CREATE_ORDER'; payload: Order }
  | { type: 'LOAD_ORDERS'; payload: Order[] }
  | { type: 'ADD_REVIEW'; payload: Review }
  | { type: 'LOAD_REVIEWS'; payload: Review[] }
  | { type: 'UPDATE_ORDER_ITEM_REVIEWED'; payload: { orderId: string; bookId: string } }
  | { type: 'UPDATE_ORDER_PAYMENT_STATUS'; payload: { orderId: string; isPaid: boolean } }
  | { type: 'UPDATE_REVIEW'; payload: Review };

const orderReducer = (state: OrderState, action: OrderAction): OrderState => {
  switch (action.type) {
    case 'CREATE_ORDER':
      return {
        ...state,
        orders: [...state.orders, action.payload]
      };
    
    case 'LOAD_ORDERS':
      return {
        ...state,
        orders: action.payload
      };
    
    case 'ADD_REVIEW':
      return {
        ...state,
        reviews: [...state.reviews, action.payload]
      };
    
    case 'LOAD_REVIEWS':
      return {
        ...state,
        reviews: action.payload
      };
    
    case 'UPDATE_ORDER_ITEM_REVIEWED':
      return {
        ...state,
        orders: state.orders.map(order =>
          order.id === action.payload.orderId
            ? {
                ...order,
                items: order.items.map(item =>
                  item.bookId === action.payload.bookId
                    ? { ...item, isReviewed: true }
                    : item
                )
              }
            : order
        )
      };
    
    case 'UPDATE_ORDER_PAYMENT_STATUS':
      return {
        ...state,
        orders: state.orders.map(order =>
          order.id === action.payload.orderId
            ? { ...order, isPaid: action.payload.isPaid }
            : order
        )
      };
    
    case 'UPDATE_REVIEW':
      return {
        ...state,
        reviews: state.reviews.map(review =>
          review.rating_id === action.payload.rating_id ? action.payload : review
        )
      };
    
    default:
      return state;
  }
};

interface CheckoutData {
  paymentMethod?: 'COD' | 'BANKING' | 'MOMO' | 'VNPAY';
  shippingAddress?: string;
  customerName?: string;
  customerPhone?: string;
  note?: string;
}

interface OrderContextType {
  orders: Order[];
  reviews: Review[];
  createOrder: (items: OrderItem[], totalAmount: number, checkoutData?: CheckoutData) => string;
  getPurchasedBooks: () => OrderItem[];
  canReviewBook: (bookId: string) => boolean;
  writeReview: (reviewData: ReviewData) => Promise<void>;
  submitReview: (orderId: string, reviewData: ReviewData) => Promise<void>;
  getReviewsForBook: (bookId: string) => Review[];
  updateOrderPaymentStatus: (orderId: string, isPaid: boolean) => void;
  updateReview: (reviewId: string, reviewData: Partial<ReviewData>) => Promise<void>;
}

const OrderContext = createContext<OrderContextType | undefined>(undefined);

interface OrderProviderProps {
  children: ReactNode;
}

export const OrderProvider: React.FC<OrderProviderProps> = ({ children }) => {
  const { user } = useAuth();
  const [state, dispatch] = useReducer(orderReducer, {
    orders: [],
    reviews: []
  });

  // Load orders from localStorage when user changes
  useEffect(() => {
    if (user) {
      const savedOrders = localStorage.getItem(`orders_${user.id}`);
      if (savedOrders) {
        const orders = JSON.parse(savedOrders).map((order: any) => ({
          ...order,
          orderDate: new Date(order.orderDate),
          deliveryDate: new Date(order.deliveryDate)
        }));
        dispatch({ type: 'LOAD_ORDERS', payload: orders });
      }

      // Load reviews
      const savedReviews = localStorage.getItem('reviews');
      if (savedReviews) {
        const reviews = JSON.parse(savedReviews).map((review: any) => ({
          ...review,
          date: new Date(review.date)
        }));
        dispatch({ type: 'LOAD_REVIEWS', payload: reviews });
      }
    }
  }, [user]);

  // Save orders to localStorage when they change
  useEffect(() => {
    if (user && state.orders.length > 0) {
      localStorage.setItem(`orders_${user.id}`, JSON.stringify(state.orders));
    }
  }, [state.orders, user]);

  // Save reviews to localStorage when they change
  useEffect(() => {
    if (state.reviews.length > 0) {
      localStorage.setItem('reviews', JSON.stringify(state.reviews));
    }
  }, [state.reviews]);

  const createOrder = (items: OrderItem[], totalAmount: number, checkoutData?: CheckoutData): string => {
    if (!user) return '';


    const paymentMethod = checkoutData?.paymentMethod || 'COD';
    const needsOnlinePayment = paymentMethod === 'BANKING' || paymentMethod === 'MOMO' || paymentMethod === 'VNPAY';

    const newOrder: Order = {
      id: `ORD-${Date.now()}`,
      userId: user.id,
      items: items.map(item => ({ ...item, isReviewed: false })),
      totalAmount,
      orderDate: new Date(),
      status: 'PENDING', 
      deliveryDate: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), 
      paymentMethod,
      shippingAddress: checkoutData?.shippingAddress,
      customerName: checkoutData?.customerName || user.fullName || user.userName,
      customerPhone: checkoutData?.customerPhone,
      note: checkoutData?.note,
      isPaid: !needsOnlinePayment, 
    };

    dispatch({ type: 'CREATE_ORDER', payload: newOrder });
    
    if (needsOnlinePayment) {
      toast.success('Đơn hàng đã được tạo! Vui lòng hoàn tất thanh toán.');
    } else {
      toast.success('Đặt hàng thành công! Đơn hàng của bạn đang được xử lý.');
    }

    return newOrder.id;
  };

  const updateOrderPaymentStatus = (orderId: string, isPaid: boolean) => {
    dispatch({ 
      type: 'UPDATE_ORDER_PAYMENT_STATUS', 
      payload: { orderId, isPaid } 
    });
    
    // Save to localStorage immediately
    const updatedOrders = state.orders.map(order =>
      order.id === orderId ? { ...order, isPaid } : order
    );
    
    if (user) {
      localStorage.setItem(`orders_${user.id}`, JSON.stringify(updatedOrders));
    }
  };

  const getPurchasedBooks = (): OrderItem[] => {
    if (!user) return [];
    
    const userOrders = state.orders.filter(order => 
      order.userId === user.id && order.status === 'DELIVERED'
    );
    
    const purchasedBooks: OrderItem[] = [];
    userOrders.forEach(order => {
      order.items.forEach(item => {
        if (!purchasedBooks.find(book => book.bookId === item.bookId)) {
          purchasedBooks.push(item);
        }
      });
    });
    
    return purchasedBooks;
  };

  const canReviewBook = (bookId: string): boolean => {
    if (!user) return false;
    
    const purchasedBooks = getPurchasedBooks();
    return purchasedBooks.some(book => book.bookId === bookId);
  };

  const writeReview = async (reviewData: ReviewData): Promise<void> => {
    if (!user) {
      throw new Error('Bạn cần đăng nhập để viết đánh giá');
    }

    if (!canReviewBook(reviewData.book_id)) {
      throw new Error('Bạn chỉ có thể đánh giá sách đã mua');
    }

    const existingReview = state.reviews.find(
      review => review.book_id === reviewData.book_id && review.user_id === user.id
    );

    if (existingReview) {
      throw new Error('Bạn đã đánh giá cuốn sách này rồi');
    }

    try {
      const response = await ReviewService.createReview(
        reviewData.book_id,
        {
          rating: reviewData.rating,
          review: reviewData.review,
        }
      );

      const newReview: Review = {
        rating_id: String(response.result.ratingId),
        book_id: reviewData.book_id,
        user_id: user.id,
        rating: response.result.rating ?? reviewData.rating,
        review: response.result.review ?? reviewData.review,
        created_at: new Date(response.result.createdAt || Date.now()),
        updated_at: new Date(response.result.updatedAt || Date.now()),
        status: response.result.status || 'pending'
      };

      dispatch({ type: 'ADD_REVIEW', payload: newReview });

      const userOrders = state.orders.filter(order => order.userId === user.id);
      userOrders.forEach(order => {
        if (order.items.some(item => item.bookId === reviewData.book_id)) {
          dispatch({
            type: 'UPDATE_ORDER_ITEM_REVIEWED',
            payload: { orderId: order.id, bookId: reviewData.book_id }
          });
        }
      });

      toast.success('Đánh giá của bạn đã được gửi thành công!');
    } catch (error) {
      toast.error('Không thể gửi đánh giá');
      throw error;
    }
  };

  const getReviewsForBook = (bookId: string): Review[] => {
    return state.reviews.filter(review => review.book_id === bookId);
  };

  const submitReview = async (orderId: string, reviewData: ReviewData): Promise<void> => {
    if (!user) {
      throw new Error('Bạn cần đăng nhập để viết đánh giá');
    }

    const existingReview = state.reviews.find(
      review => review.book_id === reviewData.book_id && review.user_id === user.id
    );

    if (existingReview) {
      throw new Error('Bạn đã đánh giá cuốn sách này rồi');
    }

    try {
      const response = await ReviewService.createReview(
        reviewData.book_id,
        {
          rating: reviewData.rating,
          review: reviewData.review,
        }
      );

      const newReview: Review = {
        rating_id: String(response.result.ratingId || `review_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`),
        book_id: reviewData.book_id,
        user_id: user.id,
        rating: reviewData.rating,
        review: reviewData.review,
        created_at: new Date(response.result.createdAt || Date.now()),
        updated_at: new Date(response.result.updatedAt || Date.now()),
        status: response.result.status || 'pending'
      };

      dispatch({ type: 'ADD_REVIEW', payload: newReview });
      dispatch({
        type: 'UPDATE_ORDER_ITEM_REVIEWED',
        payload: { orderId, bookId: reviewData.book_id }
      });

      // Lưu vào localStorage để persist
      const updatedReviews = [...state.reviews, newReview];
      localStorage.setItem('reviews', JSON.stringify(updatedReviews));

      // Cập nhật orders trong localStorage
      const updatedOrders = state.orders.map(order => {
        if (order.id === orderId) {
          return {
            ...order,
            items: order.items.map(item =>
              item.bookId === reviewData.book_id
                ? { ...item, isReviewed: true }
                : item
            )
          };
        }
        return order;
      });
      localStorage.setItem('orders', JSON.stringify(updatedOrders));

    } catch (error: any) {
      throw error;
    }
  };

  const updateReview = async (reviewId: string, reviewData: Partial<ReviewData>): Promise<void> => {
    if (!user) {
      throw new Error('Bạn cần đăng nhập');
    }

    const review = state.reviews.find(r => r.rating_id === reviewId);
    if (!review) {
      throw new Error('Không tìm thấy đánh giá');
    }

    if (review.user_id !== user.id) {
      throw new Error('Bạn không có quyền sửa đánh giá này');
    }

    try {
      const response = await ReviewService.updateReview(
        review.book_id,
        reviewId,
        {
          rating: reviewData.rating,
          review: reviewData.review,
        }
      );

      const updatedReview: Review = {
        ...review,
        rating: response.result.rating ?? reviewData.rating ?? review.rating,
        review: response.result.review ?? reviewData.review ?? review.review,
        updated_at: new Date(response.result.updatedAt || Date.now()),
        status: response.result.status ?? review.status,
      };

      dispatch({ type: 'UPDATE_REVIEW', payload: updatedReview });
      toast.success('Cập nhật đánh giá thành công!');
    } catch (error) {
      toast.error('Không thể cập nhật đánh giá');
      throw error;
    }
  };

  const value: OrderContextType = {
    orders: state.orders,
    reviews: state.reviews,
    createOrder,
    getPurchasedBooks,
    canReviewBook,
    writeReview,
    submitReview,
    getReviewsForBook,
    updateOrderPaymentStatus,
    updateReview
  };

  return (
    <OrderContext.Provider value={value}>
      {children}
    </OrderContext.Provider>
  );
};

export const useOrder = () => {
  const context = useContext(OrderContext);
  if (context === undefined) {
    throw new Error('useOrder must be used within an OrderProvider');
  }
  return context;
};