import React, { createContext, useContext, useReducer, useEffect, useCallback, ReactNode } from 'react';
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
  order_id?: string;  // Per-order review - allows multiple reviews for same book
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
  | { type: 'UPSERT_REVIEWS'; payload: Review[] }
  | { type: 'UPDATE_ORDER_ITEM_REVIEWED'; payload: { orderId: string; bookId: string } }
  | { type: 'UPDATE_ORDER_PAYMENT_STATUS'; payload: { orderId: string; isPaid: boolean } }
  | { type: 'UPDATE_REVIEW'; payload: Review };

const normalizeReview = (review: any, fallbackBookId?: string): Review => ({
  rating_id: String(review.rating_id ?? review.ratingId ?? review.id ?? ''),
  order_id: review.order_id !== undefined || review.orderId !== undefined
    ? String(review.order_id ?? review.orderId)
    : undefined,
  book_id: String(review.book_id ?? review.bookId ?? review.book?.bookId ?? fallbackBookId ?? ''),
  user_id: String(review.user_id ?? review.userId ?? review.user?.userId ?? ''),
  rating: Number(review.rating ?? 0),
  review: review.review ?? review.comment ?? review.content ?? '',
  status: review.status || 'pending',
  created_at: new Date(review.created_at ?? review.createdAt ?? Date.now()),
  updated_at: new Date(review.updated_at ?? review.updatedAt ?? Date.now())
});

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

    case 'UPSERT_REVIEWS': {
      const reviewMap = new Map(
        state.reviews.map(review => [String(review.rating_id), review])
      );

      action.payload.forEach(review => {
        if (review.rating_id) {
          reviewMap.set(String(review.rating_id), review);
        }
      });

      return {
        ...state,
        reviews: Array.from(reviewMap.values())
      };
    }
    
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
  loadReviewsForBook: (bookId: string) => Promise<Review[]>;
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
        const reviews = JSON.parse(savedReviews).map((review: any) => normalizeReview(review));
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

    // Generate numeric orderId that can be parsed as Integer (hash-based, fits in Integer range)
    const numericId = Math.abs(
      (Math.random().toString(36).substr(2, 9) + Date.now().toString()).split('').reduce(
        (acc, char) => acc * 31 + char.charCodeAt(0), 0
      )
    ) % 2147483647; // Keep within Integer.MAX_VALUE

    const newOrder: Order = {
      id: String(numericId),  // Store as numeric string for Integer parsing
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

  const getLatestDeliveredOrderIdForBook = (bookId: string): string | undefined => {
    if (!user) return undefined;

    const deliveredOrders = state.orders
      .filter(order => order.userId === user.id && order.status === 'DELIVERED')
      .filter(order => order.items.some(item => String(item.bookId) === String(bookId)))
      .sort((a, b) => b.orderDate.getTime() - a.orderDate.getTime());

    // Find first unreviewed order for this book
    for (const order of deliveredOrders) {
      const hasReview = state.reviews.some(
        review => 
          review.book_id === bookId && 
          review.user_id === user.id && 
          review.order_id === order.id
      );
      if (!hasReview) {
        return order.id;
      }
    }

    // If all orders are reviewed, return the latest one (will trigger duplicate review error)
    return deliveredOrders[0]?.id;
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

    const orderId = getLatestDeliveredOrderIdForBook(reviewData.book_id);
    if (!orderId) {
      throw new Error('Không tìm thấy đơn hàng phù hợp để gửi đánh giá');
    }

    const existingReview = state.reviews.find(
      review =>
        review.book_id === reviewData.book_id &&
        review.user_id === user.id &&
        review.order_id === orderId
    );

    if (existingReview) {
      throw new Error('Bạn đã đánh giá lần mua này rồi');
    }

    try {
      const response = await ReviewService.createReview(
        reviewData.book_id,
        {
          rating: reviewData.rating,
          review: reviewData.review,
          orderId,
        }
      );

      const newReview: Review = {
        ...normalizeReview(response.result, reviewData.book_id),
        rating_id: String((response.result as any).ratingId ?? (response.result as any).rating_id),
        order_id: String((response.result as any).orderId ?? (response.result as any).order_id ?? orderId),
        book_id: String((response.result as any).bookId ?? (response.result as any).book_id ?? reviewData.book_id),
        user_id: String((response.result as any).userId ?? (response.result as any).user_id ?? user.id),
        rating: (response.result as any).rating ?? reviewData.rating,
        review: (response.result as any).review ?? reviewData.review,
      };

      dispatch({ type: 'ADD_REVIEW', payload: newReview });

      dispatch({
        type: 'UPDATE_ORDER_ITEM_REVIEWED',
        payload: { orderId, bookId: reviewData.book_id }
      });

      toast.success('Đánh giá của bạn đã được gửi thành công!');
    } catch (error) {
      toast.error('Không thể gửi đánh giá');
      throw error;
    }
  };

  const getReviewsForBook = (bookId: string): Review[] => {
    const normalizedBookId = String(bookId);
    return state.reviews.filter(review => String(review.book_id) === normalizedBookId);
  };

  const loadReviewsForBook = useCallback(async (bookId: string): Promise<Review[]> => {
    const response = await ReviewService.getReviewsByBook(String(bookId));
    const result = (response as any).result;
    const rawReviews = Array.isArray(result) ? result : result?.reviews || [];
    const normalizedReviews = rawReviews.map((review: any) => normalizeReview(review, String(bookId)));

    dispatch({ type: 'UPSERT_REVIEWS', payload: normalizedReviews });
    return normalizedReviews;
  }, []);

  const submitReview = async (orderId: string, reviewData: ReviewData): Promise<void> => {
    if (!user) {
      throw new Error('Bạn cần đăng nhập để viết đánh giá');
    }

    const normalizedBookId = String(reviewData.book_id);

    // Check if already reviewed THIS order for this book
    const existingReviewForThisOrder = state.reviews.find(
      review => 
        String(review.book_id) === normalizedBookId && 
        review.user_id === user.id &&
        review.order_id === orderId
    );

    if (existingReviewForThisOrder) {
      throw new Error('Bạn đã đánh giá lần mua này rồi');
    }

    try {
      const response = await ReviewService.createReview(
        normalizedBookId,
        {
          rating: reviewData.rating,
          review: reviewData.review,
          orderId,
        }
      );

      const newReview: Review = {
        ...normalizeReview(response.result, normalizedBookId),
        rating_id: String((response.result as any).ratingId ?? (response.result as any).rating_id ?? `review_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`),
        order_id: String((response.result as any).orderId ?? (response.result as any).order_id ?? orderId),
        book_id: String((response.result as any).bookId ?? (response.result as any).book_id ?? normalizedBookId),
        user_id: String((response.result as any).userId ?? (response.result as any).user_id ?? user.id),
        rating: (response.result as any).rating ?? reviewData.rating,
        review: (response.result as any).review ?? reviewData.review,
      };

      dispatch({ type: 'ADD_REVIEW', payload: newReview });
      dispatch({
        type: 'UPDATE_ORDER_ITEM_REVIEWED',
        payload: { orderId, bookId: normalizedBookId }
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
              String(item.bookId) === normalizedBookId
                ? { ...item, isReviewed: true }
                : item
            )
          };
        }
        return order;
      });
      if (user) {
        localStorage.setItem(`orders_${user.id}`, JSON.stringify(updatedOrders));
      }

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
    loadReviewsForBook,
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
