import React, { createContext, useContext, useState, ReactNode, useEffect, useRef } from 'react';
import { Book } from '../../types/book';
import { Order } from '../../types/order';
import adminService from '../../services/adminService';
import { useAuth } from '../../context/AuthContext';
import { toast } from 'sonner';

// Status enum used across admin UI for soft-delete / active state
export enum ItemStatus {
  Active = 'active',
  Deleted = 'deleted',
}

// User types
export interface User {
  id: string;
  name: string;
  email: string;
  role: 'admin' | 'staff' | 'customer';
  status: 'active' | 'locked' | 'unverified';
  createdAt: string;
  totalOrders: number;
  totalSpent: number;
}

// Category type
export interface Category {
  id: string;
  categoryName: string;
  description?: string;
  bookCount: number;
  status: ItemStatus;
}

// Publisher type used by admin UI (exported so other admin components can import)
// Keep publisher shape minimal to match DB: only id and name are required.
export interface Publisher {
  id: string;
  publisherName: string;
  // optional lightweight flags (not required by DB)
  status?: ItemStatus;
  bookCount?: number;
}

export interface Author {
  id: string;
  authorName: string;
  bio: string;
  birthYear?: number;
  bookCount: number;
  status?: ItemStatus;
}


// Inventory type
export interface InventoryItem {
  id: string;
  bookId: string;
  bookTitle: string;
  sku: string;
  quantity: number;
  reorderLevel: number;
  status: 'in-stock' | 'low-stock' | 'out-of-stock';
  lastRestocked: string;
  // server fields
  orderedQuantity?: number | null;
  availableQuantity?: number | null;
  threshold?: number | null;
  // compatibility aliases used by InventoryManagement UI
  stock?: number;
  reserved?: number;
  available?: number;
  lowStockThreshold?: number;
}

// Promotion type
export interface Promotion {
  id: string;
  code: string;
  discountPercent?: number;
  startDate?: string | Date;
  endDate?: string | Date;
  isActive?: boolean;
  status?: 'active' | 'inactive' | 'deleted';
  // raw server fields (optional)
  promotionId?: number | string;
  promotionCode?: string;
  discount_percent?: number | null;
  start_date?: string | null;
  end_date?: string | null;
  isDelete?: boolean;
}

// Notification type
export interface Notification {
  id: string;
  type: 'order' | 'inventory' | 'user' | 'system';
  title: string;
  message: string;
  timestamp: string;
  isRead: boolean;
  link?: string;
}

// map inventory items from backend to AdminContext.InventoryItem shape
const mapServerInventoryToUi = (it: any): InventoryItem => {
  const id = String(it.bookId ?? it.book_id ?? it.id ?? '');
  const quantity = Number(it.stockQuantity ?? it.stock_quantity ?? 0);
  const ordered = it.orderedQuantity ?? it.ordered_quantity ?? null;
  const available = it.availableQuantity ?? it.available_quantity ?? null;
  const threshold = it.threshold ?? it.reorderLevel ?? null;
  return {
    id,
    bookId: id,
    bookTitle: it.title ?? it.bookTitle ?? '',
    sku: it.sku ?? '',
    quantity,
    orderedQuantity: ordered,
    availableQuantity: available,
    reorderLevel: Number(threshold ?? 0),
    threshold: threshold,
    status: (it.status ?? '').toString() as any,
    lastRestocked: it.lastRestocked ?? it.last_restocked ?? '',
    // aliases used by existing InventoryManagement UI
    // @ts-ignore
    stock: quantity,
    // @ts-ignore
    reserved: ordered ?? 0,
    // @ts-ignore
    available: available ?? (quantity - (ordered ?? 0)),
    // @ts-ignore
    lowStockThreshold: threshold ?? 0,
    // @ts-ignore
    imageUrl: it.imageUrl ?? null,  // proxy path from /admin/inventory
  } as any as InventoryItem;
};

interface AdminContextType {
  // Books
  books: Book[];
  addBook: (book: any) => Promise<void>;
  updateBook: (idOrBook: any, updates?: any) => Promise<void>;
  deleteBook: (id: string) => Promise<void>;
  isDelete?: boolean;


  // Users
  users: User[];
  updateUserStatus: (id: string, status: 'active' | 'locked') => void;
  updateUserRole: (id: string, role: 'admin' | 'staff' | 'customer') => void;
  deleteUser: (id: string) => void;
  createUser: (payload: { email: string; fullName?: string; role?: 'admin' | 'staff' | 'customer' }) => Promise<void>;

  // Categories
  categories: Category[];
  isInventoryLoading: boolean;
  loadInventory: () => Promise<void>;
  addCategory: (category: Omit<Category, 'id' | 'bookCount'>) => Promise<void>;
  updateCategory: (id: string, category: Partial<Category>) => Promise<void>;
  updateCategoryStatus: (id: string, status: ItemStatus) => Promise<void>;
  deleteCategory: (id: string) => Promise<void>;
  hardDeleteCategory: (id: string) => Promise<void>;

  // Publishers
  publishers: Publisher[];
  addPublisher: (publisher: Omit<Publisher, 'id' | 'bookCount'>) => void;
  updatePublisher: (id: string, publisher: Partial<Publisher>) => void;
  deletePublisher: (id: string) => void;
  hardDeletePublisher: (id: string) => Promise<void>;

  // Authors
  authors: Author[];
  addAuthor: (author: Omit<Author, 'id' | 'bookCount'>) => void;
  updateAuthor: (id: string, author: Partial<Author>) => void;
  deleteAuthor: (id: string) => void;
  hardDeleteAuthor: (id: string) => Promise<void>;

  // Orders
  orders: Order[];
  updateOrderStatus: (orderId: string, status: Order['status']) => void;
  refreshOrders: (params?: { page?: number; size?: number }) => Promise<void>;
  // Refresh all admin data (books, users, orders, inventory, etc.)
  refreshAll?: () => Promise<void>;
  // Dữ liệu trang 1 orders được prefetch ngay khi login
  // OrderManagement dùng để hiển thị tức thì không cần chờ API
  prefetchedOrdersPage: { data: any[]; totalPages: number; totalElements: number } | null;

  // Inventory
  inventory: InventoryItem[];
  updateInventory: (id: string, quantity: number) => void;
  // update stock on server and refresh list
  updateStock: (bookId: string | number, payload: { stockQuantity?: number; threshold?: number }) => Promise<void>;
  // Lazy-load inventory – chỉ gọi khi tab kho được mở lần đầu
  loadInventory: () => Promise<void>;
  // Force reload bất kể trạng thái ref – dùng cho nút "Tải lại"
  reloadInventory: () => Promise<void>;
  // true khi đang fetch inventory từ server (kể cả background call)
  isInventoryLoading: boolean;

  // Promotions
  promotions: Promotion[];
  addPromotion: (promotion: Omit<Promotion, 'id'>) => void;
  updatePromotion: (promotion: Promotion) => void;
  deletePromotion: (id: string) => void;

  // Notifications
  notifications: Notification[];
  markNotificationAsRead: (id: string) => void;
  markAllNotificationsAsRead: () => void;
}

const AdminContext = createContext<AdminContextType | undefined>(undefined);

export const useAdmin = () => {
  const context = useContext(AdminContext);
  if (!context) {
    throw new Error('useAdmin must be used within an AdminProvider');
  }
  return context;
};

interface AdminProviderProps {
  children: ReactNode;
}

export const AdminProvider: React.FC<AdminProviderProps> = ({ children }) => {
  // Initialize state from localStorage or defaults
  const [books, setBooks] = useState<Book[]>(() => {
    const stored = localStorage.getItem('admin_books');
    return stored ? JSON.parse(stored) : [];
  });
  const booksRef = useRef<Book[]>(books);

  // Users are not yet backed by admin API - keep empty and persist locally if needed
  const [users, setUsers] = useState<User[]>(() => {
    const stored = localStorage.getItem('admin_users');
    return stored ? JSON.parse(stored) : [];
  });
  const usersRef = useRef<User[]>(users);

  const [categories, setCategories] = useState<Category[]>(() => {
    const stored = localStorage.getItem('admin_categories');
    return stored ? JSON.parse(stored) : [];
  });

  const categoriesRef = useRef<Category[]>(categories);

  // Publishers (admin UI shape)
  const [publishers, setPublishers] = useState<Publisher[]>(() => {
    const stored = localStorage.getItem('admin_publishers');
    return stored ? JSON.parse(stored) : [];
  });
  const publishersRef = useRef<Publisher[]>(publishers);

  // Authors (admin UI shape)
  const [authors, setAuthors] = useState<Author[]>(() => {
    const stored = localStorage.getItem('admin_authors');
    return stored ? JSON.parse(stored) : [];
  });
  const authorsRef = useRef<Author[]>(authors);

  const [orders, setOrders] = useState<Order[]>(() => {
    const stored = localStorage.getItem('orders');
    return stored ? JSON.parse(stored) : [];
  });

  // Prefetched page-1 orders \u2014 populated ngay sau login \u0111\u1ec3 OrderManagement hi\u1ec3n th\u1ecb t\u1ee9c th\u00ec
  const [prefetchedOrdersPage, setPrefetchedOrdersPage] = useState<{
    data: any[];
    totalPages: number;
    totalElements: number;
  } | null>(null);
  const ordersPagePrefetchedRef = useRef(false);

  const [inventory, setInventory] = useState<InventoryItem[]>(() => {
    const stored = localStorage.getItem('admin_inventory');
    return stored ? JSON.parse(stored) : [];
  });
  // true khi đang fetch inventory (kể cả background call lúc login)
  const [isInventoryLoading, setIsInventoryLoading] = useState(true);

  const [promotions, setPromotions] = useState<Promotion[]>(() => {
    const stored = localStorage.getItem('admin_promotions');
    return stored ? JSON.parse(stored) : [];
  });

  const [notifications, setNotificationsRaw] = useState<Notification[]>(() => {
    const stored = localStorage.getItem('admin_notifications');
    const parsed = stored ? JSON.parse(stored) : [];
    console.debug('[AdminContext] initial notifications loaded from localStorage:', parsed?.length ?? 0);
    return parsed;
  });

  // Helper: always merge localStorage isRead flags when updating notifications
  const setNotifications = (incoming: Notification[] | ((prev: Notification[]) => Notification[])) => {
    let next: Notification[];
    if (typeof incoming === 'function') {
      next = incoming(notifications);
    } else {
      next = incoming;
    }
    // Merge isRead from localStorage
    try {
      const stored = localStorage.getItem('admin_notifications');
      const parsed: Notification[] = stored ? JSON.parse(stored) : [];
      const readMap = new Map<string, boolean>();
      parsed.forEach(n => { if (n && n.id) readMap.set(String(n.id), Boolean(n.isRead)); });
      next = next.map(n => readMap.get(String(n.id)) ? { ...n, isRead: true } : n);
    } catch (e) {
      // ignore merge errors
    }
    setNotificationsRaw(next);
    try { localStorage.setItem('admin_notifications', JSON.stringify(next)); } catch (e) { console.error('Failed to persist notifications', e); }
  };

  // Persist to localStorage (skip large datasets like books, orders, inventory)
  useEffect(() => {
    // Don't persist books - too large (3000+ items)
    booksRef.current = books;
  }, [books]);

  useEffect(() => {
    localStorage.setItem('admin_users', JSON.stringify(users));
    usersRef.current = users;
  }, [users]);

  useEffect(() => {
    localStorage.setItem('admin_categories', JSON.stringify(categories));
    categoriesRef.current = categories;
  }, [categories]);

  useEffect(() => {
    localStorage.setItem('admin_publishers', JSON.stringify(publishers));
    publishersRef.current = publishers;
  }, [publishers]);

  useEffect(() => {
    localStorage.setItem('admin_authors', JSON.stringify(authors));
    authorsRef.current = authors;
  }, [authors]);

  useEffect(() => {
    // Don't persist inventory - too large (3000+ items)
  }, [inventory]);

  useEffect(() => {
    localStorage.setItem('admin_promotions', JSON.stringify(promotions));
  }, [promotions]);

  useEffect(() => {
    // Already handled in setNotifications
  }, [notifications]);

  // Fetch books and categories from admin API on mount
  // helper: map server book shape to UI-friendly Book
  const mapServerBookToUi = (b: any): Book => {
    const parsedId = b.bookId ?? b.id ?? (Number(String(b.id || '').replace(/^book-/, '')) || 0);
    const publicationYear = b.publicationYear ?? b.publishedYear ?? undefined;
    const existing = booksRef.current.find(x => Number(x.bookId) === Number(parsedId));
    const stockQuantity = (
      b.stockQuantity ?? b.stock ?? b.availableQuantity ?? b.available ??
      (b.inStock !== undefined ? (b.inStock ? 1 : 0) : undefined) ??
      existing?.stockQuantity ?? 0
    );
    // Map publisher shape
    const publisher = b.publisher
      ? { publisherId: b.publisher.publisherId ?? b.publisher.id ?? 0, publisherName: b.publisher.publisherName ?? b.publisher.name ?? '' }
      : undefined;

    // map categories array from backend (categoryId/categoryName) to Book.categories (Category[] defined in types/book)
    const mappedCategories = Array.isArray(b.categories)
      ? b.categories.map((c: any) => ({ categoryId: c.categoryId ?? c.id ?? 0, categoryName: c.categoryName ?? c.name ?? '' }))
      : (b.category ? [{ categoryId: 0, categoryName: b.category }] : []);

    // map status for soft-delete pattern
    const status = b.status === null || b.status === undefined
      ? 'active'
      : (typeof b.status === 'string'
        ? (String(b.status).toLowerCase() === 'active' ? 'active' : 'deleted')
        : (typeof b.status === 'boolean' ? (b.status ? 'active' : 'deleted') : 'active'));

    return {
      bookId: Number(parsedId),
      title: b.title ?? b.name ?? '',
      author: b.author ?? b.authorName ?? b.authorName ?? '',
      publisher,
      price: b.price ?? b.priceAmount ?? 0,
      stockQuantity: Number(stockQuantity),
      description: b.description ?? b.summary ?? '',
      publicationYear: publicationYear,
      avgRating: b.avgRating ?? b.rating ?? 0,
      ratingCount: b.ratingCount ?? b.reviewCount ?? 0,
      language: b.language ?? undefined,
      format: b.format ?? undefined,
      createdAt: b.createdAt,
      updatedAt: b.updatedAt,
      categories: mappedCategories,
      imageUrl: b.imageUrl ?? b.images ?? undefined,
      images: b.images ?? b.imageUrl ?? undefined,
      status: status as any,
    } as Book;
  };

  // map backend user shape to AdminContext.User (available to other helpers)
  const normalizeRole = (r: any): User['role'] => {
    if (!r) return 'customer';
    const s = String(r).toLowerCase();
    const stripped = s.startsWith('role_') ? s.replace('role_', '') : s;
    if (stripped === 'administrator' || stripped === 'admin') return 'admin';
    if (stripped === 'staff') return 'staff';
    return 'customer';
  };

  const mapServerUserToUi = (u: any): User => ({
    id: String(u.userId ?? u.id ?? u._id ?? ''),
    name: u.name ?? u.fullName ?? u.username ?? '',
    email: u.email ?? u.emailAddress ?? '',
    role: normalizeRole(u.role),
    status: u.status === 'unverified' ? 'unverified' : ((u.status === 'locked' || u.status === 'deleted') ? 'locked' : 'active'),
    createdAt: u.createdAt ?? u.created_at ?? new Date().toISOString(),
    totalOrders: Number(u.totalOrders ?? u.orderCount ?? 0),
    totalSpent: Number(u.totalSpent ?? u.total_spent ?? 0),
  });

  // map promotions from backend to AdminContext.Promotion shape
  const mapServerPromotionToUi = (p: any): Promotion => {
    const id = String(p.promo_id ?? p.promotionId ?? p.id ?? '');
    const start = p.start_date ?? p.startDate ?? '';
    const end = p.end_date ?? p.endDate ?? '';
    const isExpired = end ? (new Date(String(end)) < new Date()) : false;

    // prefer explicit server status when provided (keeps 'disabled' as disabled)
    const rawStatus = p.status ?? p.voucherStatus ?? p.state ?? null;

    let isActive: boolean;
    if (rawStatus !== null && rawStatus !== undefined) {
      isActive = String(rawStatus).toLowerCase() === 'active';
    } else if (p.isActive !== undefined) {
      isActive = Boolean(p.isActive);
    } else if (p.is_active !== undefined) {
      isActive = Boolean(p.is_active);
    } else {
      // fallback: not expired => active, expired => not active
      isActive = !isExpired;
    }

    const status = rawStatus ?? (isExpired ? 'expired' : (isActive ? 'active' : 'disabled'));

    return {
      id,
      code: p.promotionCode ?? p.promotion_code ?? p.code ?? p.name ?? '',
      discountPercent: Number(p.discount_percent ?? p.discountPercent ?? p.discount ?? 0),
      startDate: start,
      endDate: end,
      isActive,
      status,
      isDelete: Boolean(p.isDelete ?? p.is_delete ?? false),
      // keep raw server fields for debugging / exact-match display
      promotionId: p.promotionId ?? p.promo_id ?? p.id,
      promotionCode: p.promotionCode ?? p.promotion_code ?? p.code ?? undefined,
      discount_percent: p.discount_percent ?? p.discountPercent ?? undefined,
      start_date: p.start_date ?? p.startDate ?? undefined,
      end_date: p.end_date ?? p.endDate ?? undefined,
    } as any as Promotion;
  };

    // (moved) inventory mapping is defined above so it can be reused by the polling effect

  // Extracted fetch logic so it can be invoked on-demand by consumers (e.g. Statistics page)
  const mountedRef = useRef(true);
  const isRefreshingRef = useRef(false); // Guard: ngăn concurrent refreshAll calls
  const refreshAll = async () => {
    if (isRefreshingRef.current) {
      console.debug('[AdminContext] refreshAll already in progress, skipping');
      return;
    }
    isRefreshingRef.current = true;
    try {
      console.log('[AdminContext] refreshAll: Calling dedicated dashboard API...');
      
      // Call single dashboard endpoint instead of multiple APIs
      const dashboardRes = await adminService.getDashboard();
      const dashboard = dashboardRes?.result;

      if (!dashboard) {
        console.error('[AdminContext] Dashboard API returned empty result');
        return;
      }

      // Extract data from dashboard response (users now included in parallel fetch)
      const booksData = dashboard.books ?? [];
      const categoriesData = dashboard.categories ?? [];
      const ordersData = dashboard.orders ?? [];
      const publishersData = dashboard.publishers ?? [];
      const authorsData = dashboard.authors ?? [];
      const promotionsData = dashboard.promotions ?? [];
      const inventoryData = dashboard.inventory ?? [];
      // Users trả về từ dashboard (song song), không cần extra call
      const usersFromDashboard = dashboard.users ?? [];

      console.log('[AdminContext] Dashboard data loaded - books:', booksData.length, 'orders:', ordersData.length);

      // Process books if available
      if (booksData && mountedRef.current) {
        const uiBooks = (Array.isArray(booksData) ? booksData : (booksData as any).books ?? []).map(mapServerBookToUi);
        console.log('[AdminContext] Setting books:', uiBooks.length);
        setBooks(uiBooks);
      }

      const mapServerCategoryToUi = (c: any): Category => ({
        id: String(c.categoryId ?? c.id ?? ''),
        categoryName: c.categoryName ?? c.name ?? '',
        description: c.description ?? '',
        bookCount: c.bookCount ?? 0,
        status: c.status === null || c.status === undefined
          ? ItemStatus.Active
          : (typeof c.status === 'string'
            ? (String(c.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
            : (typeof c.status === 'boolean' ? (c.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
      });

      const uiCategories = Array.isArray(categoriesData)
        ? categoriesData.map(mapServerCategoryToUi)
        : (categoriesData && Array.isArray((categoriesData as any).categories) ? (categoriesData as any).categories.map(mapServerCategoryToUi) : []);

      if (categoriesData && mountedRef.current) {
        setCategories(uiCategories);
      }

      const mapServerPublisherToUi = (p: any): Publisher => ({
        id: String(p.publisherId ?? p.id ?? p.publisher_id ?? ''),
        publisherName: p.publisherName ?? p.name ?? '',
        status: p.status === null || p.status === undefined
          ? ItemStatus.Active
          : (typeof p.status === 'string'
            ? (String(p.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
            : (typeof p.status === 'boolean' ? (p.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
        bookCount: p.bookCount ?? 0,
      });

      const uiPublishers = Array.isArray(publishersData)
        ? publishersData.map(mapServerPublisherToUi)
        : (publishersData && Array.isArray((publishersData as any).publishers) ? (publishersData as any).publishers.map(mapServerPublisherToUi) : []);

      if (publishersData && mountedRef.current) {
        setPublishers(uiPublishers);
      }

      const mapServerAuthorToUi = (a: any): Author => ({
        id: String(a.authorId ?? a.id ?? ''),
        authorName: a.authorName ?? a.name ?? '',
        bio: a.bio ?? a.biography ?? '',
        birthYear: a.birthYear ?? a.dobYear ?? undefined,
        bookCount: a.bookCount ?? 0,
        status: a.status === null || a.status === undefined
          ? ItemStatus.Active
          : (typeof a.status === 'string'
            ? (String(a.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
            : (typeof a.status === 'boolean' ? (a.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
      });

      const uiAuthors = Array.isArray(authorsData)
        ? authorsData.map(mapServerAuthorToUi)
        : (authorsData && Array.isArray((authorsData as any).authors) ? (authorsData as any).authors.map(mapServerAuthorToUi) : []);

      if (authorsData && mountedRef.current) {
        setAuthors(uiAuthors);
      }

      // Process users (now from dashboard response, no separate call needed)
      if (mountedRef.current) {
        const uiUsers = Array.isArray(usersFromDashboard)
          ? usersFromDashboard.map(mapServerUserToUi)
          : [];
        setUsers(uiUsers);
      }

      const uiPromotions = Array.isArray(promotionsData)
        ? promotionsData.map(mapServerPromotionToUi)
        : (promotionsData && Array.isArray((promotionsData as any).promotions) ? (promotionsData as any).promotions.map(mapServerPromotionToUi) : []);

      if (promotionsData && mountedRef.current) {
        setPromotions(uiPromotions);
      }

      console.log('[AdminContext] inventoryData:', inventoryData);
      const uiInventory = Array.isArray(inventoryData)
        ? inventoryData.map(mapServerInventoryToUi)
        : (inventoryData && Array.isArray((inventoryData as any).items) ? (inventoryData as any).items.map(mapServerInventoryToUi) : []);
      // Dashboard API intentionally returns inventory=[] (lazy-loaded separately).
      // Only overwrite if dashboard actually returned data — never reset existing inventory to [].
      if (uiInventory.length > 0 && mountedRef.current) {
        console.log('[AdminContext] Setting inventory from dashboard:', uiInventory.length);
        setInventory(uiInventory);
        inventoryLoadedRef.current = true;
      } else {
        console.log('[AdminContext] Dashboard inventory empty — preserving existing data, lazy-load will handle it.');
      }

      console.log('[AdminContext] ordersData extracted:', ordersData);
      const uiOrders = Array.isArray(ordersData)
        ? (ordersData as any[]).map((o: any) => ({
            id: String(o.id ?? ''),
            userId: String(o.userId ?? ''),
            items: Array.isArray(o.items) ? o.items.map((item: any) => ({
              id: String(item.id ?? ''),
              bookId: String(item.bookId ?? ''),
              title: item.title ?? '',
              author: item.author ?? '',
              price: Number(item.price ?? 0),
              quantity: Number(item.quantity ?? 0),
              imageUrl: item.imageUrl ?? null,
              isReviewed: Boolean(item.isReviewed ?? false),
            })) : [],
            totalAmount: Number(o.totalAmount ?? 0),
            orderDate: o.orderDate ?? new Date().toISOString(),
            status: String(o.status ?? 'PENDING') as Order['status'],
            deliveryDate: o.deliveryDate ?? new Date().toISOString(),
            paymentMethod: o.paymentMethod ?? 'COD',
            shippingAddress: o.shippingAddress ?? '',
            customerName: o.customerName ?? '',
            customerPhone: o.customerPhone ?? '',
            note: o.note ?? null,
            isPaid: Boolean(o.isPaid ?? false),
          }) as Order)
        : [];

      if (ordersData && mountedRef.current) {
        console.log('[AdminContext] Setting orders:', uiOrders.length);
        setOrders(uiOrders);
      }
    } catch (err) {
      console.error('[AdminContext] Failed to refresh admin data', err);
    } finally {
      isRefreshingRef.current = false;
    }
  };

  // Theo dõi lần cuối cùng dữ liệu được fetch thành công
  // Dùng timestamp thay vì boolean để tự động invalidate sau TTL
  const dataLastLoadedRef = useRef<number>(0); // 0 = chưa load
  const DATA_TTL_MS = 5 * 60 * 1000; // 5 phút
  // Compat alias cho code cũ dùng dataLoadedRef
  const dataLoadedRef = { 
    get current() { return dataLastLoadedRef.current > 0; },
    set current(v: boolean) { if (!v) dataLastLoadedRef.current = 0; }
  };

  const { user } = useAuth();

  // Inventory lazy-load – khai báo trước useEffect để có thể gọi trong prefetch
  const inventoryLoadedRef = useRef(false);
  const isLoadingInventoryRef = useRef(false);

  const loadInventory = async () => {
    if (inventoryLoadedRef.current || isLoadingInventoryRef.current) return;
    isLoadingInventoryRef.current = true;
    setIsInventoryLoading(true);
    try {
      const res = await adminService.getInventory();
      const inventoryData = (res as any)?.result ?? res ?? [];
      const uiInventory = Array.isArray(inventoryData)
        ? inventoryData.map(mapServerInventoryToUi)
        : (inventoryData && Array.isArray((inventoryData as any).items)
          ? (inventoryData as any).items.map(mapServerInventoryToUi) : []);
      if (mountedRef.current) {
        setInventory(uiInventory);
        // Cache vào localStorage → lần sau mở tab hiện data ngay, không chờ API
        try { localStorage.setItem('admin_inventory', JSON.stringify(uiInventory)); } catch (_) {}
        inventoryLoadedRef.current = true;
      }
    } catch (err) {
      console.error('[AdminContext] Failed to load inventory', err);
    } finally {
      isLoadingInventoryRef.current = false;
      if (mountedRef.current) setIsInventoryLoading(false);
    }
  };


  // Prefetch trang 1 đơn hàng ngay khi login — OrderManagement hiển thị tức thì không chờ API
  const prefetchOrders = async () => {
    if (ordersPagePrefetchedRef.current) return;
    ordersPagePrefetchedRef.current = true;
    try {
      const res = await adminService.getOrders({ page: 1, size: 10 });
      const data = (res as any)?.result?.books
        ?? (res as any)?.result?.data
        ?? [];
      const meta = (res as any)?.result;
      if (Array.isArray(data) && mountedRef.current) {
        setPrefetchedOrdersPage({
          data,
          totalPages: meta?.totalPages ?? 1,
          totalElements: meta?.totalElements ?? data.length,
        });
      }
    } catch (e) {
      ordersPagePrefetchedRef.current = false; // retry on next click
      console.debug('[AdminContext] prefetchOrders failed silently', e);
    }
  };

  // Prefetch ngay khi user đăng nhập với vai trò admin/staff,
  // TRƯỚC khi họ navigate đến /admin.
  // AdminProvider giờ nằm ở App level nên không bị unmount khi navigate.
  useEffect(() => {
    mountedRef.current = true;
    const isAdminUser = user?.role === 'admin' || user?.role === 'staff';

    if (isAdminUser) {
      const now = Date.now();
      const isStale = (now - dataLastLoadedRef.current) > DATA_TTL_MS;
      const neverLoaded = dataLastLoadedRef.current === 0;
      if (neverLoaded || isStale) {
        console.log(`[AdminContext] User is admin/staff — ${neverLoaded ? 'first load' : 'data stale (>5min)'}, refreshing dashboard...`);
        dataLastLoadedRef.current = now; // mark immediately to prevent double-fire
        refreshAll().then(() => { dataLastLoadedRef.current = Date.now(); }); // update after success
        if (neverLoaded) {
          loadInventory();   // background: Quản lý kho
          prefetchOrders();  // background: Quản lý đơn hàng trang 1
        }
      } else {
        console.debug('[AdminContext] Data still fresh, skipping refresh');
      }
    } else if (!isAdminUser) {
      // Reset khi user logout hoặc role thay đổi
      dataLoadedRef.current = false;
      inventoryLoadedRef.current = false;
    }

    return () => {
      // Không unmount vì Provider ở App level
    };
  }, [user?.role]);

  // Poll inventory mỗi 5 phút để giữ freshness – chỉ sau khi đã lazy-loaded
  useEffect(() => {
    let canceled = false;
    const intervalMs = 5 * 60 * 1000; // 5 phút
    const poll = async () => {
      if (!inventoryLoadedRef.current) return; // chưa lazy-load, bỏ qua
      try {
        const res = await adminService.getInventory();
        const inventoryData = (res as any)?.result ?? res ?? [];
        const uiInventory = Array.isArray(inventoryData)
          ? inventoryData.map(mapServerInventoryToUi)
          : (inventoryData && Array.isArray((inventoryData as any).items) ? (inventoryData as any).items.map(mapServerInventoryToUi) : []);
        if (canceled) return;
        setInventory(uiInventory);
      } catch (err) {
        console.debug('[AdminContext] inventory poll failed', err);
      }
    };

    const handle = setInterval(poll, intervalMs);
    return () => { canceled = true; clearInterval(handle); };
  }, []);

  // Lắng nghe sự kiện từ ChatWidget khi bot xác nhận thay đổi dữ liệu
  // → Phase 1: Optimistic update ngay lập tức (0ms) — cập nhật order trong memory
  // → Phase 2: Background refreshAll() để đồng bộ data chính xác từ server
  const refreshAllRef2 = useRef(refreshAll);
  refreshAllRef2.current = refreshAll;
  const setOrdersRef = useRef(setOrders);
  setOrdersRef.current = setOrders;
  const setUsersRef = useRef(setUsers);
  setUsersRef.current = setUsers;

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail as {
        source?: string; role?: string;
        orderId?: number | null; newStatus?: string | null;
        type?: string;
      };
      console.log('[AdminContext] bookstore:data-changed received', detail);

      // ── Phase 1: Optimistic Update — đổi status trong memory NGAY (0ms) ──
      if (detail.orderId && detail.newStatus && detail.type !== 'inventory') {
        const id = String(detail.orderId);
        setOrdersRef.current(prev => prev.map((order: any) => {
          // Hỗ trợ cả order.id, order.orderId, order.order_id
          const oid = String(order.id ?? order.orderId ?? order.order_id ?? '');
          return oid === id ? { ...order, status: detail.newStatus as string } : order;
        }));
        console.log(`[AdminContext] ✅ Optimistic update: đơn #${detail.orderId} → ${detail.newStatus}`);
      }

      // ── User status update (lock/unlock từ chatbot) ──
      if (detail.type === 'user_status' && detail.userId) {
        const uid = String(detail.userId);
        setUsersRef.current(prev => prev.map((u: any) =>
          String(u.id) === uid ? { ...u, status: detail.newUserStatus } : u
        ));
        console.log(`[AdminContext] ✅ Optimistic update user status: #${uid} → ${detail.newUserStatus}`);
      }

      // ── User role update (đổi role từ chatbot) ──
      if (detail.type === 'user_role' && detail.userId && detail.newRole) {
        const uid = String(detail.userId);
        setUsersRef.current(prev => prev.map((u: any) =>
          String(u.id) === uid ? { ...u, role: detail.newRole } : u
        ));
        console.log(`[AdminContext] ✅ Optimistic update user role: #${uid} → ${detail.newRole}`);
      }

      // ── Phase 2: Background refresh để đồng bộ server data ──
      if (detail.type === 'inventory') {
        console.log('[AdminContext] Refreshing inventory only...');
        inventoryLoadedRef.current = false;
        isLoadingInventoryRef.current = false;
        loadInventory().catch(() => {});
      } else {
        refreshAllRef2.current().catch(() => {});
      }
      dataLastLoadedRef.current = Date.now();
    };
    window.addEventListener('bookstore:data-changed', handler);
    return () => window.removeEventListener('bookstore:data-changed', handler);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /**
   * FALLBACK: nếu API /admin/inventory chậm hoặc lỗi, tự tính inventory từ books đã tải.
   * Effect này chạy khi books thay đổi. Nếu books đã có data mà inventory vẫn trống
   * (và không đang trong quá trình fetch thực), derive ngay để UI hiển thị dữ liệu.
   * Khi API inventory thực sự trả về, setInventory() sẽ ghi đè với data chính xác hơn.
   */
  useEffect(() => {
    if (
      books.length > 0 &&
      inventory.length === 0 &&
      !inventoryLoadedRef.current
    ) {
      console.warn('[AdminContext] Inventory API chậm/lỗi — deriving từ books:', books.length);
      const derived: InventoryItem[] = books.map((b: any) => {
        const stock = Number(b.stockQuantity ?? b.stock_quantity ?? 0);
        const available = Number(b.availableQuantity ?? stock);
        const threshold = 5;
        const statusVal: InventoryItem['status'] =
          stock === 0 ? 'out-of-stock'
          : available <= threshold ? 'low-stock'
          : 'in-stock';
        return {
          id: String(b.bookId),
          bookId: String(b.bookId),
          bookTitle: b.title ?? '',
          sku: '',
          quantity: stock,
          orderedQuantity: 0,
          availableQuantity: available,
          reorderLevel: threshold,
          threshold,
          status: statusVal,
          lastRestocked: '',
          stock,
          reserved: 0,
          available,
          lowStockThreshold: threshold,
        } as any as InventoryItem;
      });
      setInventory(derived);
      // Không set inventoryLoadedRef = true → khi API thực trả về vẫn ghi đè được
      if (mountedRef.current) setIsInventoryLoading(false);
    }
  }, [books]);

  // Ensure persisted read flags survive server pushes: merge localStorage's isRead into live notifications
  // No longer needed: merge logic is now in setNotifications



  const uiToCreatePayload = (ui: any) => {
    // Normalize categories: support both selectedCategoryIds and categories array
    let categoryIds: number[] = [];
    if (Array.isArray(ui.selectedCategoryIds)) {
      categoryIds = ui.selectedCategoryIds.map((id: any) => {
        const n = Number(id);
        return !Number.isNaN(n) && n > 0 ? n : 0;
      }).filter((n: number) => n > 0);
    } else if (Array.isArray(ui.categories)) {
      const categoryObjects = ui.categories.map((c: any) => {
        if (typeof c === 'object') return Number(c.categoryId ?? c.id ?? 0);
        const n = Number(c);
        if (!Number.isNaN(n) && String(c).trim() !== '') return n;
        const found = categoriesRef.current.find(cat => String(cat.categoryName).toLowerCase() === String(c).toLowerCase());
        return found ? Number(found.id) : 0;
      });
      categoryIds = categoryObjects.filter((n: number) => n > 0);
    }

    // Always create FormData (backend expects multipart/form-data with @ModelAttribute)
    const formData = new FormData();
    formData.append('title', ui.title || '');
    
    // Resolve authorId: accept selectedAuthorId, authorId, or author name/string
    let resolvedAuthorId = '';
    if (ui.selectedAuthorId) resolvedAuthorId = String(ui.selectedAuthorId);
    else if (ui.authorId) resolvedAuthorId = String(ui.authorId);
    else if (ui.author) {
      // if author is object with id
      if (typeof ui.author === 'object' && (ui.author.authorId || ui.author.id)) {
        resolvedAuthorId = String(ui.author.authorId ?? ui.author.id);
      } else {
        const authorTerm = String(ui.author).trim();
        const parsedAuthorId = Number(authorTerm);
        if (!Number.isNaN(parsedAuthorId) && authorTerm !== '') {
          resolvedAuthorId = String(parsedAuthorId);
        } else {
          const aName = authorTerm.toLowerCase();
          const found = authorsRef.current.find(a => String(a.authorName).toLowerCase() === aName);
          if (found) resolvedAuthorId = String(found.id ?? found.authorId);
        }
      }
    }
    // Only append authorId if it's a valid integer ID
    const authorIdNum = Number(resolvedAuthorId);
    if (!Number.isNaN(authorIdNum) && authorIdNum > 0) {
      formData.append('authorId', String(authorIdNum));
    }

    // Resolve publisherId: accept selectedPublisherId, publisherId, or publisher name/string
    let resolvedPublisherId = '';
    if (ui.selectedPublisherId) resolvedPublisherId = String(ui.selectedPublisherId);
    else if (ui.publisherId) resolvedPublisherId = String(ui.publisherId);
    else if (ui.publisher) {
      if (typeof ui.publisher === 'object' && (ui.publisher.publisherId || ui.publisher.id)) {
        resolvedPublisherId = String(ui.publisher.publisherId ?? ui.publisher.id);
      } else {
        const publisherTerm = String(ui.publisher).trim();
        const parsedPublisherId = Number(publisherTerm);
        if (!Number.isNaN(parsedPublisherId) && publisherTerm !== '') {
          resolvedPublisherId = String(parsedPublisherId);
        } else {
          const pName = publisherTerm.toLowerCase();
          const found = publishersRef.current.find(p => String(p.publisherName).toLowerCase() === pName);
          if (found) resolvedPublisherId = String(found.id ?? found.publisherId);
        }
      }
    }
    // Only append publisherId if it's a valid integer ID
    const publisherIdNum = Number(resolvedPublisherId);
    if (!Number.isNaN(publisherIdNum) && publisherIdNum > 0) {
      formData.append('publisherId', String(publisherIdNum));
    }
    
    formData.append('description', ui.description || '');
    formData.append('price', String(ui.price || 0));
    
    // Resolve stock: support both inStock/stock and stockQuantity
    const stockValue = ui.stock !== undefined ? ui.stock : (ui.stockQuantity !== undefined ? ui.stockQuantity : 0);
    formData.append('stock', String(stockValue || 0));
    formData.append('publishedYear', String(ui.publishedYear || new Date().getFullYear()));
    formData.append('language', ui.language || 'vi');
    formData.append('format', ui.format || 'paperback');
    if (ui.status !== undefined) {
      formData.append('status', ui.status);
    }
    categoryIds.forEach((id: number) => {
      formData.append('categoryIds', String(id));
    });
    
    // Only append imageFile if present and is a File object
    if (ui.imageFile instanceof File) {
      formData.append('imageFile', ui.imageFile);
    }
    
    return formData;
  };

  // Publisher operations (local optimistic + API)
  const addPublisher = (publisher: Omit<Publisher, 'id' | 'bookCount'>) => {
    return (async () => {
      const tempId = `pub-temp-${Date.now()}`;
      const temp: Publisher = { ...publisher, id: tempId, bookCount: 0 } as Publisher;
      setPublishers(prev => [...prev, temp]);
      try {
        // accept enum ItemStatus from UI and forward to backend as-is (string)
        const payload: any = { ...publisher };
        if (publisher.status !== undefined) payload.status = publisher.status;
        const res = await adminService.createPublisher(payload);
        const created = res?.result ?? res ?? publisher;
        const mapped: Publisher = {
          id: String(created.publisherId ?? created.id ?? tempId),
          publisherName: created.publisherName ?? created.name ?? publisher.publisherName,
          status: created.status === null || created.status === undefined
            ? ItemStatus.Active
            : (typeof created.status === 'string'
              ? (String(created.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
              : (typeof created.status === 'boolean' ? (created.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
          bookCount: created.bookCount ?? 0,
        };
        setPublishers(prev => prev.map(p => (p.id === tempId ? mapped : p)));
        const displayName = mapped.publisherName || publisher.publisherName || 'nhà xuất bản';
        toast.success(`Thêm ${displayName} thành công`);
      } catch (err) {
        setPublishers(prev => prev.filter(p => p.id !== tempId));
        console.error('Failed to create publisher', err);
      }
    })();
  };

  const updatePublisher = (id: string, updates: Partial<Publisher>) => {
    const prevPublishers = publishersRef.current;
    setPublishers(prev => prev.map(p => p.id === id ? { ...p, ...updates } : p));
    return (async () => {
      try {
        // accept enum ItemStatus in updates and send to backend as string
        const payload: any = { ...updates };
        if (updates.status !== undefined) payload.status = updates.status;
        const res = await adminService.updatePublisher(id, payload);
        const saved = res?.result ?? res;
        if (saved) {
          const mapped: Publisher = {
            id: String(saved.publisherId ?? saved.id ?? id),
            publisherName: saved.publisherName ?? saved.name ?? updates.publisherName ?? prevPublishers.find(p => p.id === id)?.publisherName ?? '',
            status: saved.status === null || saved.status === undefined
              ? ItemStatus.Active
              : (typeof saved.status === 'string'
                ? (String(saved.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
                : (typeof saved.status === 'boolean' ? (saved.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
            bookCount: saved.bookCount ?? prevPublishers.find(p => p.id === id)?.bookCount ?? 0,
          } as Publisher;
          setPublishers(prev => prev.map(p => p.id === id ? mapped : p));
        }
        toast.success('Cập nhật nhà xuất bản thành công');
      } catch (err: any) {
        setPublishers(prevPublishers);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        console.error('Failed to update publisher', err);
      }
    })();
  };

  const deletePublisher = (id: string) => {
    const prev = publishersRef.current;
    // soft-delete locally (mark as deleted)
    setPublishers(prevList => prevList.map(p => p.id === id ? { ...p, status: ItemStatus.Deleted } : p));
    return (async () => {
      try {
        await adminService.deletePublisher(id);
        toast.success('Vô hiệu hóa nhà xuất bản thành công');
      } catch (err: any) {
        setPublishers(prev);
        const message = err?.response?.data?.message ?? err?.message ?? 'Vô hiệu hóa thất bại';
        toast.error(String(message));
        console.error('Failed to delete publisher', err);
      }
    })();
  };

  /** Hard delete: xóa vĩnh viễn khỏi DB — chỉ thành công nếu backend xác nhận 0 sách */
  const hardDeletePublisher = async (id: string): Promise<void> => {
    const prev = publishersRef.current;
    try {
      await adminService.hardDeletePublisher(id);
      setPublishers(prevList => prevList.filter(p => p.id !== id));
      toast.success('Xóa vĩnh viễn nhà xuất bản thành công');
    } catch (err: any) {
      setPublishers(prev);
      const message = err?.response?.data?.message ?? err?.message ?? 'Xóa vĩnh viễn thất bại';
      toast.error(String(message));
      console.error('Failed to hard delete publisher', err);
    }
  };

  const addAuthor = (author: Omit<Author, 'id' | 'bookCount'>) => {
    return (async () => {
      const tempId = `auth-temp-${Date.now()}`;
      const temp: Author = { ...author, id: tempId, bookCount: 0 } as Author;
      setAuthors(prev => [...prev, temp]);
      try {
        const payload: any = { ...author };
        if (author.status !== undefined) payload.status = author.status;
        const res = await adminService.createAuthor(payload);
        const created = res?.result ?? res ?? author;
        const mapped: Author = {
          id: String(created.authorId ?? created.id ?? tempId),
          authorName: created.authorName ?? created.name ?? author.authorName,
          bio: created.bio ?? created.biography ?? author.bio ?? '',
          birthYear: created.birthYear ?? created.dobYear ?? author.birthYear,
          status: created.status === null || created.status === undefined
            ? ItemStatus.Active
            : (typeof created.status === 'string'
              ? (String(created.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
              : (typeof created.status === 'boolean' ? (created.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
          bookCount: created.bookCount ?? 0,
        };
        setAuthors(prev => prev.map(a => (a.id === tempId ? mapped : a)));
        const displayName = mapped.authorName || author.authorName || 'tác giả';
        toast.success(`Thêm ${displayName} thành công`);
      } catch (err) {
        setAuthors(prev => prev.filter(a => a.id !== tempId));
        console.error('Failed to create author', err);
      }
    })();
  };

  const updateAuthor = (id: string, updates: Partial<Author>) => {
    const prevAuthors = authorsRef.current;
    setAuthors(prev => prev.map(a => a.id === id ? { ...a, ...updates } : a));
    return (async () => {
      try {
        const payload: any = { ...updates };
        if (updates.status !== undefined) payload.status = updates.status;
        const res = await adminService.updateAuthor(id, payload);
        const saved = res?.result ?? res;
        if (saved) {
          const mapped: Author = {
            id: String(saved.authorId ?? saved.id ?? id),
            authorName: saved.authorName ?? saved.name ?? updates.authorName ?? prevAuthors.find(p => p.id === id)?.authorName ?? '',
            bio: saved.bio ?? saved.biography ?? updates.bio ?? prevAuthors.find(p => p.id === id)?.bio ?? '',
            birthYear: saved.birthYear ?? saved.dobYear ?? prevAuthors.find(p => p.id === id)?.birthYear,
            bookCount: saved.bookCount ?? prevAuthors.find(p => p.id === id)?.bookCount ?? 0,
            status: saved.status === null || saved.status === undefined
              ? ItemStatus.Active
              : (typeof saved.status === 'string'
                ? (String(saved.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
                : (typeof saved.status === 'boolean' ? (saved.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
          } as Author;
          setAuthors(prev => prev.map(a => a.id === id ? mapped : a));
        }
        toast.success('Cập nhật thông tin tác giả thành công');
      } catch (err: any) {
        setAuthors(prevAuthors);
        console.error('Failed to update author', err);
      }
    })();
  };

  const deleteAuthor = (id: string) => {
    const prev = authorsRef.current;
    setAuthors(prevList => prevList.map(a => a.id === id ? { ...a, status: ItemStatus.Deleted } : a));
    return (async () => {
      try {
        await adminService.deleteAuthor(id);
        toast.success('Vô hiệu hóa tác giả thành công');
      } catch (err: any) {
        setAuthors(prev);
        const message = err?.response?.data?.message ?? err?.message ?? 'Vô hiệu hóa thất bại';
        toast.error(String(message));
        console.error('Failed to delete author', err);
      }
    })();
  };

  /** Hard delete: xóa vĩnh viễn khỏi DB — chỉ thành công nếu backend xác nhận 0 sách */
  const hardDeleteAuthor = async (id: string): Promise<void> => {
    const prev = authorsRef.current;
    try {
      await adminService.hardDeleteAuthor(id);
      setAuthors(prevList => prevList.filter(a => a.id !== id));
      toast.success('Xóa vĩnh viễn tác giả thành công');
    } catch (err: any) {
      setAuthors(prev);
      const message = err?.response?.data?.message ?? err?.message ?? 'Xóa vĩnh viễn thất bại';
      toast.error(String(message));
      console.error('Failed to hard delete author', err);
    }
  };

  const parseUiId = (idOrBook: any) => {
    if (!idOrBook) return null;
    if (typeof idOrBook === 'string') {
      const s = idOrBook;
      const m = s.match(/(\d+)$/);
      return m ? Number(m[1]) : Number(s);
    }
    if (typeof idOrBook === 'number') return idOrBook;
    if (typeof idOrBook === 'object') {
      const id = idOrBook.id ?? idOrBook.bookId ?? idOrBook.book_id ?? idOrBook.bookId;
      if (typeof id === 'string') {
        const m = String(id).match(/(\d+)$/);
        return m ? Number(m[1]) : Number(id);
      }
      return Number(id);
    }
    return null;
  };

  const addBook = (book: any) => {
    return (async () => {
      const tempId = -Date.now();
      const tempBook: Book = { ...book, bookId: tempId } as Book;
      setBooks(prev => [...prev, tempBook]);
      try {
        const payload = uiToCreatePayload(book);
        const res = await adminService.createBook(payload);
        const created = res?.result ?? res;
        const mapped = mapServerBookToUi(created);
        setBooks(prev => prev.map(b => (b.bookId === tempId ? mapped : b)));
      } catch (err) {
        // rollback
        setBooks(prev => prev.filter(b => b.bookId !== tempId));
      }
    })();
  };

  const updateBook = (idOrBook: any, updates?: any) => {
    // Support both: updateBook(id, payload) or updateBook(book_with_id)
    const isIdFirst = typeof idOrBook === 'string' || typeof idOrBook === 'number';
    const bookId = isIdFirst ? idOrBook : idOrBook?.bookId;
    const payload = isIdFirst ? updates : idOrBook;

    if (!bookId || !payload) return Promise.resolve();

    const idNum = parseUiId(bookId);
    const idStr = String(idNum);

    const prevBooks = booksRef.current;
    // Optimistic update - merge payload into existing book
    setBooks(prev => prev.map(book => book.bookId === idNum ? { ...book, ...payload } : book));

    return (async () => {
      try {
        const serverPayload = uiToCreatePayload(payload);
        const res = await adminService.updateBook(idStr, serverPayload);
        const saved = res?.result ?? res;
        const savedBook = mapServerBookToUi(saved);
        setBooks(prev => prev.map(b => b.bookId === (savedBook.bookId ?? idNum) ? savedBook : b));
        toast.success('Cập nhật sách thành công');
      } catch (err: any) {
        // rollback
        setBooks(prevBooks);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update book', err);
      }
    })();
  };

  // Soft-delete book (set status to 'delete' instead of removing)
  const deleteBook = (id: string) => {
    const prevBooks = booksRef.current;
    const idNum = parseUiId(id);
    // optimistic soft-delete: set status to 'deleted'
    setBooks(prev => prev.map(book => book.bookId === idNum ? { ...book, status: 'deleted' as any } : book));
    return (async () => {
      try {
        await adminService.deleteBook(id);
        toast.success('Xóa sách thành công');
        // refresh from server to get updated list
        try {
          const booksRes = await adminService.getBooks({ page: 1, limit: 1000 });
          const booksData = booksRes?.result?.books ?? booksRes?.result ?? booksRes?.books ?? booksRes ?? [];
          const uiBooks = (Array.isArray(booksData) ? booksData : (booksData as any).books ?? []).map(mapServerBookToUi);
          setBooks(uiBooks);
        } catch (e) {
          // ignore refresh errors
        }
      } catch (err) {
        // rollback
        setBooks(prevBooks);
        console.error('Failed to delete book', err);
      }
    })();
  };

  // User operations (persist status/role changes to admin API with optimistic update)
  const updateUserStatus = (id: string, status: 'active' | 'locked') => {
    const prevUsers = usersRef.current;
    setUsers(prev => prev.map(user => user.id === id ? { ...user, status } : user));
    return (async () => {
      try {
        const res = await adminService.updateUser(id, { status });
        const saved = res?.result ?? res;
        if (saved) {
          const mapped = mapServerUserToUi(saved);
          setUsers(prev => prev.map(u => u.id === String(mapped.id) ? mapped : u));
        }
        toast.success('Cập nhật trạng thái người dùng thành công');
      } catch (err: any) {
        setUsers(prevUsers);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update user status', err);
      }
    })();
  };

  const updateUserRole = (id: string, role: 'admin' | 'staff' | 'customer') => {
    const prevUsers = usersRef.current;
    setUsers(prev => prev.map(user => user.id === id ? { ...user, role } : user));
    return (async () => {
      try {
        const res = await adminService.updateUser(id, { role });
        const saved = res?.result ?? res;
        if (saved) {
          const mapped = mapServerUserToUi(saved);
          setUsers(prev => prev.map(u => u.id === String(mapped.id) ? mapped : u));
        }
        toast.success('Cập nhật vai trò người dùng thành công');
      } catch (err: any) {
        setUsers(prevUsers);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update user role', err);
      }
    })();
  };

  // Delete user (optimistic + API)
  const deleteUser = (id: string) => {
    const prev = usersRef.current;
    setUsers(prevList => prevList.filter(u => u.id !== id));
    return (async () => {
      try {
        await adminService.deleteUser(id);
        toast.success('Xóa người dùng thành công');
      } catch (err: any) {
        setUsers(prev);
        const message = err?.response?.data?.message ?? err?.message ?? 'Xóa thất bại';
        toast.error(String(message));
        console.error('Failed to delete user', err);
      }
    })();
  };

  const createUser = (payload: { email: string; fullName?: string; role?: 'admin' | 'staff' | 'customer' }) => {
    return (async () => {
      try {
        const res = await adminService.createUser(payload);
        const created = res?.result ?? res;
        const mapped = mapServerUserToUi(created);
        setUsers(prev => {
          const filtered = prev.filter(u => u.email !== mapped.email);
          return [mapped, ...filtered];
        });
        toast.success('Tạo người dùng mới thành công — mật khẩu đã được gửi tới email');
      } catch (err: any) {
        console.error('Failed to create user', err);
        const message = err?.response?.data?.message ?? err?.message ?? 'Tạo người dùng thất bại';
        toast.error(String(message));
        throw err;
      }
    })();
  };

  // Category operations
  const addCategory = (category: Omit<Category, 'id' | 'bookCount'>) => {
    return (async () => {
      const tempId = `cat-temp-${Date.now()}`;
      const temp: Category = { ...category, id: tempId, bookCount: 0 } as Category;
      setCategories(prev => [...prev, temp]);
      try {
        const res = await adminService.createCategory(category);
        const raw = res?.result ?? res;
        // Map server response (categoryId, categoryName, status...) → UI Category shape (id, ...)
        const mapped: Category = {
          id: String(raw?.categoryId ?? raw?.id ?? tempId),
          categoryName: raw?.categoryName ?? raw?.name ?? category.categoryName ?? '',
          description: raw?.description ?? '',
          bookCount: raw?.bookCount ?? 0,
          status: raw?.status === null || raw?.status === undefined
            ? ItemStatus.Active
            : (typeof raw.status === 'string'
              ? (String(raw.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
              : (typeof raw.status === 'boolean' ? (raw.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
        };
        setCategories(prev => prev.map(c => (c.id === tempId ? mapped : c)));
        toast.success('Thêm danh mục thành công');
      } catch (err: any) {
        setCategories(prev => prev.filter(c => c.id !== tempId));
        const message = err?.response?.data?.message ?? err?.message ?? 'Thêm danh mục thất bại';
        toast.error(String(message));
        console.error('Failed to create category', err);
      }
    })();
  };

  // TODO-API: Implement PATCH /api/admin/categories/:id
  const updateCategory = (id: string, updates: Partial<Category>) => {
    const prevCats = categoriesRef.current;
    setCategories(prev => prev.map(cat => cat.id === id ? { ...cat, ...updates } : cat));
    return (async () => {
      try {
        const res = await adminService.updateCategory(id, updates);
        const saved = res?.result ?? res;
        if (saved) {
          const mapped: Category = {
            id: String(saved.categoryId ?? saved.id ?? id),
            categoryName: saved.categoryName ?? saved.name ?? updates.categoryName ?? prevCats.find(c => c.id === id)?.categoryName ?? '',
            description: saved.description ?? updates.description ?? prevCats.find(c => c.id === id)?.description ?? '',
            bookCount: saved.bookCount ?? prevCats.find(c => c.id === id)?.bookCount ?? 0,
            status: saved.status === null || saved.status === undefined
              ? ItemStatus.Active
              : (typeof saved.status === 'string'
                ? (String(saved.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
                : (typeof saved.status === 'boolean' ? (saved.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
          };
          setCategories(prev => prev.map(cat => cat.id === id ? mapped : cat));
        }
        toast.success('Cập nhật danh mục thành công');
      } catch (err: any) {
        setCategories(prevCats);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update category', err);
      }
    })();
  };

  const updateCategoryStatus = (id: string, status: ItemStatus) => {
    const prevCats = categoriesRef.current;
    setCategories(prev => prev.map(cat => cat.id === id ? { ...cat, status } : cat));
    return (async () => {
      try {
        const payload: any = { status };
        const res = await adminService.updateCategory(id, payload);
        const saved = res?.result ?? res;
        if (saved) {
          const mapped: Category = {
            id: String(saved.categoryId ?? saved.id ?? id),
            categoryName: saved.categoryName ?? saved.name ?? prevCats.find(c => c.id === id)?.categoryName ?? '',
            description: saved.description ?? prevCats.find(c => c.id === id)?.description ?? '',
            bookCount: saved.bookCount ?? prevCats.find(c => c.id === id)?.bookCount ?? 0,
            status: saved.status === null || saved.status === undefined
              ? ItemStatus.Active
              : (typeof saved.status === 'string'
                ? (String(saved.status).toLowerCase() === 'active' ? ItemStatus.Active : ItemStatus.Deleted)
                : (typeof saved.status === 'boolean' ? (saved.status ? ItemStatus.Active : ItemStatus.Deleted) : ItemStatus.Active)),
          };
          setCategories(prev => prev.map(cat => cat.id === id ? mapped : cat));
        }
        toast.success('Cập nhật trạng thái danh mục thành công');
      } catch (err: any) {
        setCategories(prevCats);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update category status', err);
      }
    })();
  };

  // TODO-API: Implement DELETE /api/admin/categories/:id (soft delete)
  const deleteCategory = (id: string) => {
    // Soft delete locally first
    const prevCats = categoriesRef.current;
    setCategories(prev => prev.map(cat => cat.id === id ? { ...cat, status: ItemStatus.Deleted } : cat));
    return (async () => {
      try {
        await adminService.deleteCategory(id);
        toast.success('Vô hiệu hóa danh mục thành công');
      } catch (err: any) {
        setCategories(prevCats);
        const message = err?.response?.data?.message ?? err?.message ?? 'Vô hiệu hóa thất bại';
        toast.error(String(message));
        console.error('Failed to delete category', err);
      }
    })();
  };

  /** Hard delete: xóa vĩnh viễn khỏi DB — chỉ thành công nếu backend xác nhận 0 sách */
  const hardDeleteCategory = (id: string) => {
    const prevCats = categoriesRef.current;
    return (async () => {
      try {
        await adminService.hardDeleteCategory(id);
        setCategories(prev => prev.filter(cat => cat.id !== id));
        toast.success('Xóa vĩnh viễn danh mục thành công');
      } catch (err: any) {
        setCategories(prevCats);
        const message = err?.response?.data?.message ?? err?.message ?? 'Xóa vĩnh viễn thất bại';
        toast.error(String(message));
        console.error('Failed to hard delete category', err);
      }
    })();
  };

  // TODO-API: Implement PATCH /api/admin/orders/:id/status
  // TODO-API: Send status update notification to customer
  // Order operations
  const updateOrderStatus = (orderId: string, status: Order['status']) => {
    setOrders(prev => prev.map(order =>
      order.id === orderId ? { ...order, status } : order
    ));
    // LOCALSTORAGE-TEMP: Also update in main localStorage - should be in database
    const allOrders = JSON.parse(localStorage.getItem('orders') || '[]');
    const updatedOrders = allOrders.map((order: Order) =>
      order.id === orderId ? { ...order, status } : order
    );
    localStorage.setItem('orders', JSON.stringify(updatedOrders));
  };

  const refreshOrders = async (params?: { page?: number; size?: number }) => {
    try {
      const actualParams = params || { page: 1, size: 999999 };
      const ordersRes = await adminService.getOrders(actualParams);
      const ordersData = ordersRes?.result?.books ?? ordersRes?.result?.data ?? ordersRes?.result ?? ordersRes ?? [];
      
      if (Array.isArray(ordersData)) {
        setOrders(ordersData);
        localStorage.setItem('admin_orders', JSON.stringify(ordersData));
        console.log('Orders refreshed:', ordersData.length);
      }
    } catch (error) {
      console.error('Failed to refresh orders:', error);
    }
  };

  // Ref để dùng trong setInterval mà không bị stale closure
  const refreshOrdersRef = useRef(refreshOrders);
  refreshOrdersRef.current = refreshOrders;

  // Poll đơn hàng mỗi 60 giây — Dashboard và OrderManagement tự cập nhật khi có đơn mới.
  // Dừng khi tab bị ẩn (document.visibilityState) để tiết kiệm tài nguyên.
  useEffect(() => {
    const isAdminUser = user?.role === 'admin' || user?.role === 'staff';
    if (!isAdminUser) return;

    let handle: ReturnType<typeof setInterval> | null = null;
    let canceled = false;

    const pollOrders = async () => {
      if (canceled || document.hidden) return;
      try {
        await refreshOrdersRef.current();
      } catch (err) {
        console.debug('[AdminContext] orders poll failed silently', err);
      }
    };

    handle = setInterval(pollOrders, 60_000); // mỗi 60 giây

    return () => {
      canceled = true;
      if (handle) clearInterval(handle);
    };
  }, [user?.role]);



  // TODO-API: Implement PATCH /api/admin/inventory/:id
  // TODO-API: Create low-stock notification when quantity <= reorderLevel
  // Inventory operations
  const updateInventory = (id: string, quantity: number) => {
    setInventory(prev => prev.map(item => {
      if (item.id === id) {
        const newStatus = quantity <= 0 ? 'out-of-stock' :
          quantity <= item.reorderLevel ? 'low-stock' : 'in-stock';
        return { ...item, quantity, status: newStatus, lastRestocked: new Date().toISOString() };
      }
      return item;
    }));
  };

  const updateStock = async (bookId: string | number, payload: { stockQuantity?: number; threshold?: number }) => {
    const prev = inventory;
    // optimistic update locally
    setInventory(prevList => prevList.map(i => i.bookId === String(bookId) ? { ...i, quantity: payload.stockQuantity ?? i.quantity, reorderLevel: payload.threshold ?? i.reorderLevel } : i));
    try {
      await adminService.updateStock(bookId, payload);
      toast.success('Cập nhật tồn kho thành công!');
      // refresh authoritative list
      try {
        const res = await adminService.getInventory();
        const inventoryData = (res as any)?.result ?? res ?? [];
        const uiInventory = Array.isArray(inventoryData)
          ? inventoryData.map(mapServerInventoryToUi)
          : (inventoryData && Array.isArray((inventoryData as any).items) ? (inventoryData as any).items.map(mapServerInventoryToUi) : []);
        setInventory(uiInventory);
      } catch (e) {
        // ignore
      }
    } catch (err) {
      setInventory(prev);
      console.error('Failed to update stock', err);
      toast.error('Cập nhật kho thất bại');
    }
  };

  // TODO-API: Implement POST /api/admin/promotions
  // Promotion operations
  const addPromotion = (promotion: Omit<Promotion, 'id'>) => {
    return (async () => {
      const tempId = `promo-temp-${Date.now()}`;
      const tempPromotion: Promotion = { ...promotion, id: tempId } as Promotion;
      setPromotions(prev => [...prev, tempPromotion]);
      try {
        const payload: any = {
          promotionCode: promotion.code,
          discount_percent: promotion.discountPercent ?? 0,
          start_date: promotion.startDate ?? undefined,
          end_date: promotion.endDate ?? undefined,
          status: promotion.status ?? (promotion.isActive ? 'active' : 'disabled'),
        };
        const res = await adminService.createPromotion(payload);
        const created = res?.result ?? res ?? payload;
        const mapped: Promotion = {
          id: String(created.promo_id ?? created.id ?? created.promotionId ?? `promo-${Date.now()}`),
          code: created.promotionCode ?? created.promotion_code ?? created.code ?? promotion.code,
          discountPercent: Number(created.discount_percent ?? created.discountPercent ?? promotion.discountPercent ?? 0),
          startDate: created.start_date ?? created.startDate ?? promotion.startDate,
          endDate: created.end_date ?? created.endDate ?? promotion.endDate,
          isActive: (created.isActive !== undefined) ? Boolean(created.isActive) : (created.is_active === undefined ? (promotion.isActive ?? true) : Boolean(created.is_active)),
          status: created.status ?? created.voucherStatus ?? ((created.end_date && new Date(String(created.end_date)) < new Date()) ? 'expired' : (created.isActive || created.is_active ? 'active' : 'deleted')),
          // also keep raw server fields for debugging / exact-match display
          promotionId: created.promotionId ?? created.promo_id ?? created.id,
          promotionCode: created.promotionCode ?? created.promotion_code ?? created.code ?? promotion.code,
          discount_percent: created.discount_percent ?? created.discountPercent ?? promotion.discountPercent ?? 0,
          start_date: created.start_date ?? created.startDate ?? promotion.startDate,
          end_date: created.end_date ?? created.endDate ?? promotion.endDate,
        } as any as Promotion;
        setPromotions(prev => prev.map(p => p.id === tempId ? mapped : p));
        toast.success('Thêm khuyến mãi thành công');
        // refresh from server to keep authoritative list in sync
        try {
          const refreshRes = await adminService.getPromotions();
          const promotionsData = refreshRes?.result ?? refreshRes ?? [];
          const uiPromotions = Array.isArray(promotionsData)
            ? promotionsData.map(mapServerPromotionToUi)
            : (promotionsData && Array.isArray((promotionsData as any).promotions) ? (promotionsData as any).promotions.map(mapServerPromotionToUi) : []);
          setPromotions(uiPromotions);
        } catch (e) {
          // ignore refresh errors; keep optimistic state
        }
      } catch (err) {
        setPromotions(prev => prev.filter(p => p.id !== tempId));
        console.error('Failed to create promotion', err);
      }
    })();
  };

  // TODO-API: Implement PATCH /api/admin/promotions/:id
  const updatePromotion = (promotion: Promotion) => {
    const prevPromos = promotions;
    setPromotions(prev => prev.map(p => p.id === promotion.id ? { ...p, ...promotion } : p));
    return (async () => {
      try {
        const payload: any = {
          promotionCode: promotion.code,
          discount_percent: promotion.discountPercent ?? 0,
          start_date: promotion.startDate ?? undefined,
          end_date: promotion.endDate ?? undefined,
          status: promotion.status ?? (promotion.isActive ? 'active' : 'disabled'),
        };
        const res = await adminService.updatePromotion(promotion.id, payload);
        const saved = res?.result ?? res ?? payload;
        const mapped: Promotion = {
          id: String(saved.promo_id ?? saved.id ?? saved.promotionId ?? promotion.id),
          code: saved.promotionCode ?? saved.promotion_code ?? saved.code ?? promotion.code,
          discountPercent: Number(saved.discount_percent ?? saved.discountPercent ?? promotion.discountPercent ?? 0),
          startDate: saved.start_date ?? saved.startDate ?? promotion.startDate,
          endDate: saved.end_date ?? saved.endDate ?? promotion.endDate,
          isActive: (saved.isActive !== undefined) ? Boolean(saved.isActive) : (saved.is_active === undefined ? (promotion.isActive ?? true) : Boolean(saved.is_active)),
          status: saved.status ?? saved.voucherStatus ?? ((saved.end_date && new Date(String(saved.end_date)) < new Date()) ? 'expired' : (saved.isActive || saved.is_active ? 'active' : 'delete')),
          promotionId: saved.promotionId ?? saved.promo_id ?? saved.id ?? promotion.id,
          promotionCode: saved.promotionCode ?? saved.promotion_code ?? saved.code ?? promotion.code,
          discount_percent: saved.discount_percent ?? saved.discountPercent ?? promotion.discountPercent ?? 0,
          start_date: saved.start_date ?? saved.startDate ?? promotion.startDate,
          end_date: saved.end_date ?? saved.endDate ?? promotion.endDate,
        } as any as Promotion;
        setPromotions(prev => prev.map(p => p.id === String(mapped.id) ? mapped : p));
        toast.success('Cập nhật khuyến mãi thành công');
        // refresh authoritative list from server
        try {
          const refreshRes = await adminService.getPromotions();
          const promotionsData = refreshRes?.result ?? refreshRes ?? [];
          const uiPromotions = Array.isArray(promotionsData)
            ? promotionsData.map(mapServerPromotionToUi)
            : (promotionsData && Array.isArray((promotionsData as any).promotions) ? (promotionsData as any).promotions.map(mapServerPromotionToUi) : []);
          setPromotions(uiPromotions);
        } catch (e) {
          // ignore refresh errors
        }
      } catch (err: any) {
        setPromotions(prevPromos);
        const message = err?.response?.data?.message ?? err?.message ?? 'Cập nhật thất bại';
        toast.error(String(message));
        console.error('Failed to update promotion', err);
      }
    })();
  };

  const deletePromotion = (id: string) => {
    const targetPromo = promotions.find(p => p.id === id);
    const isAlreadyDeleted = (targetPromo as any)?.isDeleted === true || targetPromo?.status === 'deleted';
    const prev = promotions;
    setPromotions(prevList => prevList.filter(p => p.id !== id));
    return (async () => {
      try {
        if (isAlreadyDeleted) {
          // Gọi permanent delete nếu đã bị soft-delete trước đó
          await adminService.permanentDeletePromotion(id);
          toast.success('Đã xóa vĩnh viễn khuyến mãi');
        } else {
          // Soft delete cho khuyến mãi đang active/inactive
          await adminService.deletePromotion(id);
          toast.success('Đã vô hiệu hóa khuyến mãi');
        }
        try {
          const refreshRes = await adminService.getPromotions();
          const promotionsData = refreshRes?.result ?? refreshRes ?? [];
          const uiPromotions = Array.isArray(promotionsData)
            ? promotionsData.map(mapServerPromotionToUi)
            : (promotionsData && Array.isArray((promotionsData as any).promotions) ? (promotionsData as any).promotions.map(mapServerPromotionToUi) : []);
          setPromotions(uiPromotions);
        } catch (e) {
        }
      } catch (err: any) {
        setPromotions(prev);
        const message = err?.response?.data?.message ?? err?.message ?? 'Xóa thất bại';
        toast.error(String(message));
        console.error('Failed to delete promotion', err);
      }
    })();
  };

  const markNotificationAsRead = (id: string) => {
    setNotifications(prev => prev.map(notif => notif.id === id ? { ...notif, isRead: true } : notif));
  };

  const markAllNotificationsAsRead = () => {
    setNotifications(prev => prev.map(notif => ({ ...notif, isRead: true })));
  };

  const value: AdminContextType = {
    books,
    addBook,
    updateBook,
    deleteBook,
    users,
    updateUserStatus,
    updateUserRole,
    deleteUser,
    createUser,
    categories,
    addCategory,
    updateCategory,
    updateCategoryStatus,
    deleteCategory,
    hardDeleteCategory,
    publishers,
    addPublisher,
    updatePublisher,
    deletePublisher,
    hardDeletePublisher,
    authors,
    addAuthor,
    updateAuthor,
    deleteAuthor,
    hardDeleteAuthor,
    orders,
    updateOrderStatus,
    refreshOrders,
    refreshAll,
    prefetchedOrdersPage,
    inventory,
    isInventoryLoading,
    updateInventory,
    updateStock,
    loadInventory,
    reloadInventory: async () => {
      // Reset cả hai refs để buộc fetch lại
      inventoryLoadedRef.current = false;
      isLoadingInventoryRef.current = false;
      await loadInventory();
    },
    promotions,
    addPromotion,
    updatePromotion,
    deletePromotion,
    notifications,
    markNotificationAsRead,
    markAllNotificationsAsRead,
  };

  return <AdminContext.Provider value={value}>{children}</AdminContext.Provider>;
};
