import React, { createContext, useContext, useReducer, useEffect, ReactNode, useState } from 'react';
import { Book } from '../types/book';
import { cartService, BookService } from '../services';
import { toast } from 'sonner';
import { useAuth } from './AuthContext';

type CartItem = {
  book: Book;
  quantity: number;
  cartItemId?: string; // Backend cart item ID
};

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

type CartAction = 
  | { type: 'ADD_TO_CART'; book: Book; quantity?: number }
  | { type: 'REMOVE_FROM_CART'; bookId: string }
  | { type: 'UPDATE_QUANTITY'; bookId: string; quantity: number }
  | { type: 'CLEAR_CART' }
  | { type: 'LOAD_CART'; items: CartItem[] };

const cartReducer = (state: CartItem[], action: CartAction): CartItem[] => {
  switch (action.type) {
    case 'ADD_TO_CART':
      const existingItem = state.find(item => String((item.book as any).bookId) === String((action.book as any).bookId));
      const quantityToAdd = action.quantity || 1;
      if (existingItem) {
        return state.map(item =>
          String((item.book as any).bookId) === String((action.book as any).bookId)
            ? { ...item, quantity: item.quantity + quantityToAdd }
            : item
        );
      }
      return [...state, { book: action.book, quantity: quantityToAdd }];

    case 'REMOVE_FROM_CART':
      return state.filter(item => String((item.book as any).bookId) !== action.bookId);

    case 'UPDATE_QUANTITY':
      if (action.quantity === 0) {
        return state.filter(item => String((item.book as any).bookId) !== action.bookId);
      }
      return state.map(item =>
        String((item.book as any).bookId) === action.bookId
          ? { ...item, quantity: action.quantity }
          : item
      );

    case 'CLEAR_CART':
      return [];

    case 'LOAD_CART':
      return action.items;

    default:
      return state;
  }
};

interface CartContextType {
  items: CartItem[];
  addToCart: (book: Book, quantity?: number) => Promise<void>;
  removeFromCart: (bookId: string) => Promise<void>;
  updateQuantity: (bookId: string, quantity: number) => Promise<void>;
  getTotalPrice: () => number;
  getTotalItems: () => number;
  clearCart: () => Promise<void>;
  checkout: () => Promise<void>;
  isLoading?: boolean;
}

const CartContext = createContext<CartContextType | undefined>(undefined);

interface CheckoutData {
  paymentMethod?: 'COD' | 'BANKING' | 'MOMO' | 'VNPAY';
  shippingAddress?: string;
  customerName?: string;
  customerPhone?: string;
  note?: string;
}

interface CartProviderProps {
  children: ReactNode;
  createOrder: (orderItems: OrderItem[], totalAmount: number, checkoutData?: CheckoutData) => void;
}

export const CartProvider: React.FC<CartProviderProps> = ({ children, createOrder }) => {
  const { isLoggedIn } = useAuth();
  const [items, dispatch] = useReducer(cartReducer, []);
  const [isLoading, setIsLoading] = useState(false);
  const [isSynced, setIsSynced] = useState(false);

  useEffect(() => {
    const loadCart = async () => {
      if (!isLoggedIn) {
        dispatch({ type: 'CLEAR_CART' });
        const savedCart = localStorage.getItem('bookstore-cart');
        if (savedCart) {
          try {
            const parsedCart = JSON.parse(savedCart);
            dispatch({ type: 'LOAD_CART', items: parsedCart });
          } catch (e) {
            console.error('Failed to parse localStorage cart:', e);
          }
        }
        return;
      }

      // For logged-in users, load from backend
      try {
        setIsLoading(true);
        const response = await cartService.getCart();
        const cartData = response?.result ?? response?.data ?? response;
        
        if (cartData && Array.isArray(cartData.items)) {
          // Fetch full book details for each cart item
          const cartItemsPromises = cartData.items.map(async (item: any) => {
            try {
              // Get full book details from backend
              const bookResponse = await BookService.getBookById(String(item.bookId ?? item.book?.bookId));
              const fullBook = bookResponse?.result ?? bookResponse?.data ?? bookResponse;
              
              return {
                book: fullBook as Book,
                quantity: item.quantity ?? 1,
                cartItemId: item.cartItemId ?? item.id ?? String(item.bookId),
              };
            } catch (error) {
              console.error(`Failed to fetch book ${item.bookId}:`, error);
              // Fallback to partial data if book fetch fails
              return {
                book: {
                  bookId: item.bookId ?? item.book?.bookId,
                  title: item.title ?? item.book?.title ?? 'Unknown',
                  author: item.author ?? item.book?.author ?? 'Unknown',
                  price: item.price ?? item.book?.price ?? 0,
                  imageUrl: item.imageUrl ?? item.book?.imageUrl ?? '',
                } as Book,
                quantity: item.quantity ?? 1,
                cartItemId: item.cartItemId ?? item.id ?? String(item.bookId),
              };
            }
          });
          
          const cartItems = await Promise.all(cartItemsPromises);
          dispatch({ type: 'LOAD_CART', items: cartItems });
          setIsSynced(true);
        } else {
          const savedCart = localStorage.getItem('bookstore-cart');
          if (savedCart) {
            const parsedCart = JSON.parse(savedCart);
            dispatch({ type: 'LOAD_CART', items: parsedCart });
          }
        }
      } catch (error) {
        console.error('Failed to load cart from backend:', error);
        const savedCart = localStorage.getItem('bookstore-cart');
        if (savedCart) {
          try {
            const parsedCart = JSON.parse(savedCart);
            dispatch({ type: 'LOAD_CART', items: parsedCart });
          } catch (e) {
            console.error('Failed to parse localStorage cart:', e);
          }
        }
      } finally {
        setIsLoading(false);
      }
    };

    loadCart();
  }, [isLoggedIn]);

  useEffect(() => {
    localStorage.setItem('bookstore-cart', JSON.stringify(items));
  }, [items]);

  const addToCart = async (book: Book, quantity?: number) => {
    const qty = quantity || 1;
    const bookId = (book as any).bookId ?? (book as any).id;
    
    if (!isLoggedIn) {
      return;
    }
    
    try {
      await cartService.addItem({ bookId: String(bookId), quantity: qty });
      dispatch({ type: 'ADD_TO_CART', book, quantity: qty });
      toast.success('Đã thêm vào giỏ hàng');
    } catch (error: any) {
      console.error('Failed to add to cart:', error);
      toast.error(error?.response?.data?.message || 'Không thể thêm vào giỏ hàng');
    }
  };

  const removeFromCart = async (bookId: string) => {
    const item = items.find(i => String((i.book as any).bookId) === bookId);
    const cartItemId = item?.cartItemId || bookId;
    
    const previousItems = items;
    dispatch({ type: 'REMOVE_FROM_CART', bookId });
    
    if (!isLoggedIn) {
      return;
    }
    
    try {
      await cartService.removeItem(cartItemId);
    } catch (error: any) {
      console.error('Failed to remove from cart:', error);
      // Rollback on error
      dispatch({ type: 'LOAD_CART', items: previousItems });
      toast.error(error?.response?.data?.message || 'Không thể xóa khỏi giỏ hàng');
    }
  };

  const updateQuantity = async (bookId: string, quantity: number) => {
    const item = items.find(i => String((i.book as any).bookId) === bookId);
    const cartItemId = item?.cartItemId || bookId;
    const previousQuantity = item?.quantity || 1;
    
    dispatch({ type: 'UPDATE_QUANTITY', bookId, quantity });
    
    if (!isLoggedIn) {
      return;
    }
    
    try {
      await cartService.updateItem(cartItemId, { quantity });
    } catch (error: any) {
      console.error('Failed to update quantity:', error);
      dispatch({ type: 'UPDATE_QUANTITY', bookId, quantity: previousQuantity });
      toast.error(error?.response?.data?.message || 'Không thể cập nhật số lượng');
    }
  };

  const getTotalPrice = () => {
    return items.reduce((total, item) => total + ((item.book.price ?? 0) * item.quantity), 0);
  };

  const getTotalItems = () => {
    return items.reduce((total, item) => total + item.quantity, 0);
  };

  const clearCart = async () => {
    // Optimistic update
    const previousItems = items;
    dispatch({ type: 'CLEAR_CART' });
    
    // Only call backend if user is logged in
    if (!isLoggedIn) {
      return;
    }
    
    try {
      await cartService.clearCart();
    } catch (error: any) {
      console.error('Failed to clear cart:', error);
      // Rollback on error
      dispatch({ type: 'LOAD_CART', items: previousItems });
      toast.error(error?.response?.data?.message || 'Không thể xóa giỏ hàng');
    }
  };

  const checkout = async () => {
    if (!createOrder) {
      throw new Error('Order system not initialized');
    }

    if (items.length === 0) {
      throw new Error('Giỏ hàng trống');
    }

    // Convert CartItems to OrderItems
    const orderItems: OrderItem[] = items.map(item => ({
      id: `item_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      bookId: String((item.book as any).bookId),
      title: item.book.title,
      author: (item.book as any).author ?? (item.book as any).authorName ?? '',
      price: item.book.price ?? 0,
      quantity: item.quantity,
      imageUrl: (item.book as any).imageUrl ?? (item.book as any).images ?? '',
      isReviewed: false
    }));

    const totalAmount = getTotalPrice();
    
    createOrder(orderItems, totalAmount);
    clearCart();
  };

  const contextValue: CartContextType = {
    items,
    addToCart,
    removeFromCart,
    updateQuantity,
    getTotalPrice,
    getTotalItems,
    clearCart,
    checkout,
    isLoading,
  };

  return (
    <CartContext.Provider value={contextValue}>
      {children}
    </CartContext.Provider>
  );
};

export const useCart = () => {
  const context = useContext(CartContext);
  if (context === undefined) {
    throw new Error('useCart must be used within a CartProvider');
  }
  return context;
};