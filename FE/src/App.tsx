import React, { useState, useMemo, useEffect, lazy, Suspense, useRef, useCallback } from 'react';
import { BrowserRouter, Routes, Route, Navigate, useNavigate, useParams, useLocation } from 'react-router-dom';
import { CartProvider } from './context/CartContext';
import { ThemeProvider } from './context/ThemeContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import { OrderProvider, useOrder } from './context/OrderContext';
import { AdminProvider, useAdmin } from './features/admin/AdminContext';
import { DashboardFilterProvider } from './features/admin/DashboardFilterContext';
import { Toaster } from './components/ui/sonner';
import { Header, Footer } from './layouts';
import { Hero } from './features/home';
import {
  BookCard,
  BookFilters,
  PersonalizedRecommendations
} from './features/book';
import { BookRecommendations } from './features/book/BookRecommendations';
import { Cart } from './features/cart';
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious
} from './components/ui/pagination';
import { BookService, PythonRecommendService } from './services';
import { ImageService } from './services/imageService';
import { Book } from './types/book';
import { migrateOrderStatus } from './utils/migrateOrderStatus';
import { OrderWorkflowService } from './utils/orderWorkflowService';
import ChatWidget from './components/chat/ChatWidget';

// Rewrite http://localhost:9000/bookstore/... → /minio/bookstore/...
// so all MinIO images go through the Vite proxy (avoids cross-origin issues)
function rewriteToProxy(url: string | undefined): string {
  if (!url) return '';
  if (url.startsWith('/minio')) return url; // already proxied
  try {
    const parsed = new URL(url);
    if (parsed.hostname === 'localhost' && parsed.port === '9000') {
      return `/minio${parsed.pathname}${parsed.search}`;
    }
  } catch {
    // relative path – serve directly via MinIO proxy
    if (!url.startsWith('http')) return `/minio/bookstore/${url}`;
  }
  return url;
}

// Lazy load pages for code splitting
const AccountPage = lazy(() => import('./pages/AccountPage').then(m => ({ default: m.AccountPage })));
const AdminPage = lazy(() => import('./pages/AdminPage').then(m => ({ default: m.AdminPage })));
const BookDetailPage = lazy(() => import('./pages/BookDetailPage').then(m => ({ default: m.BookDetailPage })));
const CheckoutPage = lazy(() => import('./pages/CheckoutPage').then(m => ({ default: m.CheckoutPage })));
const LoginPage = lazy(() => import('./pages/LoginPage').then(m => ({ default: m.LoginPage })));
const PaymentPage = lazy(() => import('./pages/PaymentPage').then(m => ({ default: m.PaymentPage })));
const PaymentReturnPage = lazy(() => import('./pages/PaymentReturnPage').then(m => ({ default: m.PaymentReturnPage })));
const PaymentCancelPage = lazy(() => import('./pages/PaymentCancelPage').then(m => ({ default: m.PaymentCancelPage })));
const SearchResultsPage = lazy(() => import('./pages/SearchResultsPage').then(m => ({ default: m.SearchResultsPage })));
const ForgotPasswordPage = lazy(() => import('./pages/ForgotPasswordPage').then(m => ({ default: m.ForgotPasswordPage })));
const ResetPasswordSuccessPage = lazy(() => import('./pages/ResetPasswordSuccessPage').then(m => ({ default: m.ResetPasswordSuccessPage })));
const GoogleCallbackPage = lazy(() => import('./pages/GoogleCallbackPage').then(m => ({ default: m.GoogleCallbackPage })));

import PageLoader from './components/PageLoader';

// Home Page Component
function HomePage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth(); // Get user from AuthContext
  const [isCartOpen, setIsCartOpen] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [sortBy, setSortBy] = useState<string>('newest');
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [books, setBooks] = useState<Book[]>([]);
  const [forYouBooks, setForYouBooks] = useState<Book[] | undefined>(undefined);
  const [topRatedBooks, setTopRatedBooks] = useState<Book[]>([]);
  const [trendingBooks, setTrendingBooks] = useState<Book[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [initialLoad, setInitialLoad] = useState<boolean>(true);
  const [totalPages, setTotalPages] = useState<number>(1);
  const booksPerPage = 30;

  // Helper: map item từ Recommendation API → Book shape
  const mapRecommendItem = async (items: any[]): Promise<Book[]> => {
    const mapped = items.map((item: any) => ({
      bookId:        item.book_id,
      id:            item.book_id,
      title:         item.title || '',
      authorName:    item.author_name || 'Unknown',
      author:        item.author_name || 'Unknown',
      price:         item.price || 0,
      avgRating:     item.avg_rating || 0,
      rating:        item.avg_rating || 0,
      ratingCount:   item.rating_count || 0,
      stockQuantity: item.stock_quantity ?? 1,
      imageUrl:      item.main_image || item.image_url || item.imageUrl || '',
      categories:    item.categories || [],
      reason:        item.reason,
    } as any));

    // Separate: paths that need presigning vs URLs already from MinIO
    const rawPaths = mapped
      .map((b: any) => b.imageUrl)
      .filter((url: string) => url && !url.startsWith('http') && !url.startsWith('/minio'));

    if (rawPaths.length > 0) {
      try {
        const urls = await ImageService.getPresignedUrls(rawPaths);
        mapped.forEach((b: any) => {
          if (b.imageUrl && !b.imageUrl.startsWith('http') && !b.imageUrl.startsWith('/minio')) {
            b.imageUrl = urls[b.imageUrl] || rewriteToProxy(b.imageUrl);
          }
        });
      } catch {/* silent */}
    }

    // Rewrite any http://localhost:9000 URLs (presigned from Java) through proxy
    mapped.forEach((b: any) => {
      b.imageUrl = rewriteToProxy(b.imageUrl);
    });

    return mapped as Book[];
  };

  useEffect(() => {
    const fetchPage = async () => {
      setIsLoading(true);
      try {
        const params: any = {
          page: currentPage,
          limit: booksPerPage,
          sort: sortBy,
        };

        if (selectedCategory && selectedCategory !== 'all') {
          const asNum = Number(selectedCategory);
          if (!Number.isNaN(asNum)) params.category = asNum; 
          else params.category = selectedCategory; 
        }

        const response = await BookService.getBooks(params);
        const result = response && (response.result as any);
        const pageBooks = (result && result.books) || [];
        // Normalize rating fields from various backend shapes
        // Also rewrite MinIO presigned URLs through the Vite proxy
        const normalized = pageBooks.map((b: any) => ({
          ...b,
          avgRating: b.avgRating ?? b.avg_rating ?? b.rating ?? 0,
          ratingCount: b.ratingCount ?? b.reviewCount ?? b.rating_count ?? 0,
          imageUrl: rewriteToProxy(b.imageUrl),
        }));
        setBooks(normalized);
        const tp = (result && (result.totalPages ?? result.total_pages)) ?? 1;
        setTotalPages(tp);
      } catch (error) {
        console.error('Failed to fetch paginated books:', error);
        setBooks([]);
        setTotalPages(1);
      } finally {
        setIsLoading(false);
        if (initialLoad) setInitialLoad(false);
      }
    };

    fetchPage();
  }, [currentPage, selectedCategory, sortBy]);

  useEffect(() => {
    let mounted = true;
    const fetchPersonalized = async () => {
      try {
        if (user) {
          // Get userId from user object (backend now properly returns it)
          const userId = user.id;
          
          if (userId) {
            const isNewUser = checkIfNewUser(user);
            
            if (!isNewUser) {
              try {
                const resp = await PythonRecommendService.getForYou(Number(userId), 20);
                const list = Array.isArray(resp) ? resp : (resp as any)?.data || [];
                
                if (list.length > 0) {
                  const mappedBooks = list.map((item: any) => ({
                    bookId: item.book_id,
                    id: item.book_id,
                    title: item.title || '',
                    authorName: item.author_name || 'Unknown',
                    author: item.author_name || 'Unknown',
                    price: item.price || 0,
                    avgRating: item.avg_rating || 0,
                    rating: item.avg_rating || 0,
                    ratingCount: item.rating_count || 0,
                    stockQuantity: item.stock_quantity || 1,
                    imageUrl: rewriteToProxy(item.main_image || item.image_url || ''),
                    categories: item.categories || [],
                    score: item.score
                  }));

                  // Presign any remaining raw paths (not yet http or /minio)
                  const rawPaths = mappedBooks
                    .map((book: any) => book.imageUrl)
                    .filter((url: string) => url && !url.startsWith('http') && !url.startsWith('/minio'));

                  if (rawPaths.length > 0) {
                    const presignedUrls = await ImageService.getPresignedUrls(rawPaths);
                    mappedBooks.forEach((book: any) => {
                      if (book.imageUrl && !book.imageUrl.startsWith('http') && !book.imageUrl.startsWith('/minio')) {
                        book.imageUrl = presignedUrls[book.imageUrl] || rewriteToProxy(book.imageUrl);
                      }
                    });
                  }

                  if (mounted) setForYouBooks(mappedBooks as any);
                  return;
                }
              } catch (e) {
                console.warn('Failed to fetch user recommendations, falling back to popular');
              }
            }
          }
        }
        
        // Fallback: not logged in OR new user OR no recommendations available
        const popularResp = await PythonRecommendService.getPopular(20);
        const popularList = Array.isArray(popularResp) ? popularResp : (popularResp as any)?.data || [];
      
        const mappedBooks = popularList.map((item: any) => ({
          bookId: item.book_id,
          id: item.book_id,
          title: item.title || '',
          authorName: item.author_name || 'Unknown',
          author: item.author_name || 'Unknown',
          price: item.price || 0,
          avgRating: item.avg_rating || 0,
          rating: item.avg_rating || 0,
          ratingCount: item.rating_count || 0,
          stockQuantity: item.stock_quantity || 1,
          imageUrl: rewriteToProxy(item.main_image || item.image_url || ''),
          categories: item.categories || [],
          score: item.score
        }));

        // Presign any remaining raw paths
        const rawPaths = mappedBooks
          .map((book: any) => book.imageUrl)
          .filter((url: string) => url && !url.startsWith('http') && !url.startsWith('/minio'));

        if (rawPaths.length > 0) {
          const presignedUrls = await ImageService.getPresignedUrls(rawPaths);
          mappedBooks.forEach((book: any) => {
            if (book.imageUrl && !book.imageUrl.startsWith('http') && !book.imageUrl.startsWith('/minio')) {
              book.imageUrl = presignedUrls[book.imageUrl] || rewriteToProxy(book.imageUrl);
            }
          });
        }

        if (mounted) setForYouBooks(mappedBooks as any);
      } catch (e) {
        console.error('Failed to fetch recommendations:', e);
      }
    };

    const checkIfNewUser = (user: any): boolean => {
      if (user.orderCount !== undefined && user.orderCount === 0) {
        return true;
      }
      return false;
    };

    fetchPersonalized();
    return () => { mounted = false; };
  }, [user]);

  // Fetch top-rated + trending một lần khi mount
  useEffect(() => {
    let mounted = true;
    const fetchHomeRecs = async () => {
      try {
        const [topRatedResp, trendingResp] = await Promise.all([
          PythonRecommendService.getTopRated(20),
          PythonRecommendService.getTrending(7, 20),
        ]);
        const topRatedArr = Array.isArray(topRatedResp) ? topRatedResp : [];
        const trendingArr = Array.isArray(trendingResp) ? trendingResp : [];
        if (mounted) {
          setTopRatedBooks(await mapRecommendItem(topRatedArr));
          setTrendingBooks(await mapRecommendItem(trendingArr));
        }
      } catch (e) {
        console.warn('Failed to fetch home recommendations:', e);
      }
    };
    fetchHomeRecs();
    return () => { mounted = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const paginatedBooks = books;

  const handleSearch = (query: string) => {
    const trimmed = query.trim();
    if (trimmed) {
      navigate(`/search/${encodeURIComponent(trimmed)}`);
    } else {
      navigate('/search');
    }
  };

  const handleClearSearch = () => {
    setSearchQuery('');
    setCurrentPage(1);
  };

  const handleCategoryChange = (category: string) => {
    setSelectedCategory(category);
    setCurrentPage(1);
  };

  const handleSortChange = (sort: string) => {
    setSortBy(sort);
    setCurrentPage(1);
  };

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    const booksSection = document.getElementById('books-section');
    if (booksSection) {
      booksSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    } else {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  };

  const handleBookClick = (book: Book) => {
    navigate(`/book/${book.bookId}`);
  };

  const handleLogoClick = () => {
    // If already on home page, scroll to top
    if (location.pathname === '/') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      // Navigate to home page (will auto-scroll to top)
      navigate('/');
    }
  };

  const handleLoginClick = () => {
    navigate('/login');
  };

  const handleAccountClick = () => {
    navigate('/account');
  };

  // Show full-page loading only on initial load; for subsequent fetches
  // (pagination/filter changes) show a localized loader below.
  if (isLoading && initialLoad) {
    return <PageLoader />;
  }

  return (
    <div className="min-h-screen bg-background page-container">
      {/* Header */}
      <Header
        onSearch={handleSearch}
        onCartClick={() => setIsCartOpen(true)}
        onLogoClick={handleLogoClick}
        onLoginClick={handleLoginClick}
        onAccountClick={handleAccountClick}
      />

      {/* Hero Section */}
      <Hero />

      {/* Personalized Recommendations */}
      <PersonalizedRecommendations
        books={books}
        onBookClick={handleBookClick}
        forYouBooks={forYouBooks}
      />

      {/* Top-rated – Đánh giá cao nhất (nền trắng xen kẽ PersonalizedRecommendations nền mờ) */}
      {topRatedBooks.length > 0 && (
        <BookRecommendations
          title="Đánh giá cao nhất"
          subtitle="Những cuốn sách được cộng đồng độc giả yêu thích nhất"
          books={topRatedBooks}
          onBookClick={handleBookClick}
          icon="star"
          bgVariant="default"
        />
      )}

      {/* Trending – Xu hướng (nền mờ như PersonalizedRecommendations) */}
      {trendingBooks.length > 0 && (
        <BookRecommendations
          title="Đang xu hướng"
          subtitle="Những cuốn sách được xem nhiều nhất trong 7 ngày qua"
          books={trendingBooks}
          onBookClick={handleBookClick}
          icon="trending"
          bgVariant="muted"
        />
      )}

      {/* Books Section */}
      <section id="books-section" className="py-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold text-foreground mb-4">
              Khám phá kho sách
            </h2>
            <p className="text-muted-foreground max-w-2xl mx-auto">
              Tìm kiếm và khám phá hàng nghìn cuốn sách hay từ nhiều thể loại khác nhau
            </p>
          </div>

          {/* Filters */}
          <div className="mb-8">
            <BookFilters
              selectedCategory={selectedCategory}
              onCategoryChange={handleCategoryChange}
              sortBy={sortBy}
              onSortChange={handleSortChange}
              searchQuery={searchQuery}
              onClearSearch={handleClearSearch}
            />
          </div>

          {/* Books Grid */}
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
            {paginatedBooks.map((book, index) => (
              <BookCard
                key={`main-books-${book.bookId}-${index}`}
                book={book}
                onClick={() => handleBookClick(book)}
                priority={index < 5}
              />
            ))}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="mt-12">
              <Pagination>
                <PaginationContent>
                  <PaginationItem>
                    <PaginationPrevious
                      size="sm"
                      onClick={(e: React.MouseEvent) => {
                        e.preventDefault();
                        if (currentPage > 1) handlePageChange(currentPage - 1);
                      }}
                      className={currentPage === 1 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                    />
                  </PaginationItem>

                  {/* Smart pagination with max 5 visible pages */}
                  {(() => {
                    const maxVisiblePages = 5;
                    const pages = [];
                    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
                    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
                    
                    // Adjust startPage if endPage is near the end
                    if (endPage - startPage + 1 < maxVisiblePages) {
                      startPage = Math.max(1, endPage - maxVisiblePages + 1);
                    }

                    // Add first page and ellipsis if needed
                    if (startPage > 1) {
                      pages.push(1);
                      if (startPage > 2) {
                        pages.push('...');
                      }
                    }

                    // Add middle pages
                    for (let i = startPage; i <= endPage; i++) {
                      pages.push(i);
                    }

                    // Add ellipsis and last page if needed
                    if (endPage < totalPages) {
                      if (endPage < totalPages - 1) {
                        pages.push('...');
                      }
                      pages.push(totalPages);
                    }

                    return pages.map((page, index) => (
                      <PaginationItem key={index}>
                        {page === '...' ? (
                          <span className="px-3 py-2 text-sm text-muted-foreground">...</span>
                        ) : (
                          <PaginationLink
                            size="sm"
                            onClick={(e: React.MouseEvent) => { e.preventDefault(); handlePageChange(page as number); }}
                            isActive={currentPage === page}
                            className="cursor-pointer"
                          >
                            {page}
                          </PaginationLink>
                        )}
                      </PaginationItem>
                    ));
                  })()}

                  <PaginationItem>
                    <PaginationNext
                      size="sm"
                      onClick={(e: React.MouseEvent) => { e.preventDefault(); if (currentPage < totalPages) handlePageChange(currentPage + 1); }}
                      className={currentPage === totalPages ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                    />
                  </PaginationItem>
                </PaginationContent>
              </Pagination>
            </div>
          )}

          {/* No Results */}
          {(!isLoading && paginatedBooks.length === 0) && (
            <div className="text-center py-12">
              <p className="text-muted-foreground mb-4">
                Không tìm thấy kết quả phù hợp
              </p>
              <button
                type="button"
                onClick={() => {
                  setSearchQuery('');
                  setSelectedCategory('all');
                  setCurrentPage(1);
                  window.scrollTo({ top: 0, behavior: 'smooth' });
                }}
                className="text-primary hover:underline"
              >
                Xóa bộ lọc
              </button>
            </div>
          )}
        </div>
      </section>

      {/* Footer */}
      <Footer />

      {/* Cart */}
      <Cart isOpen={isCartOpen} onClose={() => setIsCartOpen(false)} />
    </div>
  );
}

// Wrapper components for routes that need navigation
function BookDetailPageWrapper() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [isCartOpen, setIsCartOpen] = useState(false);
  const [book, setBook] = useState<Book | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });

    const fetchBook = async () => {
      if (!id) return;

      setIsLoading(true);
      try {
        const response = await BookService.getBookById(id);
        const raw: any = (response && (response.result as any)) || null;
        console.log('Book detail API response:', raw);

        // Normalize fields so detail page matches listing logic
        if (raw) {
          const available =
            (raw.availableQuantity ?? raw.available_quantity ?? raw.available);
          const normalized: any = {
            ...raw,
            // prefer availableQuantity when provided (0 must be preserved)
            availableQuantity: available !== undefined ? available : raw.availableQuantity,
            // unify author field
            author: raw.author ?? raw.authorName ?? raw.author_name,
            imageUrl: raw.imageUrl ?? raw.image_url ?? raw.main_image ?? '',
          };

          // Fallback to fetch presigned URL if Java backend failed to return one
          if (normalized.imageUrl && !normalized.imageUrl.startsWith('http')) {
            try {
              const urlMap = await ImageService.getPresignedUrls([normalized.imageUrl]);
              if (urlMap[normalized.imageUrl]) {
                normalized.imageUrl = urlMap[normalized.imageUrl];
              }
            } catch (err) {
              console.error('Failed to presign single book image:', err);
            }
          }

          setBook(normalized);
        } else {
          setBook(null);
        }
      } catch (error) {
        console.error('Failed to fetch book:', error);
        setBook(null);
      } finally {
        setIsLoading(false);
      }
    };

    fetchBook();
  }, [id]);

  if (isLoading) {
    return <PageLoader />;
  }

  const handleLogoClick = () => {
    if (location.pathname === '/') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      navigate('/');
    }
  };

  return (
    <Suspense fallback={<PageLoader />}>
      <BookDetailPage
        book={book}
        onBack={() => navigate('/')}
        onBookClick={(book) => navigate(`/book/${book.bookId}`)}
        onCartClick={() => setIsCartOpen(true)}
        onSearch={(query) => navigate(query.trim() ? `/search/${encodeURIComponent(query.trim())}` : '/search')}
        onLogoClick={handleLogoClick}
        onLoginClick={() => navigate('/login')}
        onAccountClick={() => navigate('/account')}
      />
      <Cart isOpen={isCartOpen} onClose={() => setIsCartOpen(false)} />
    </Suspense>
  );
}

function SearchResultsPageWrapper() {
  const { query } = useParams<{ query?: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [isCartOpen, setIsCartOpen] = useState(false);

  const initialQuery = query ? decodeURIComponent(query) : '';

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, [query]);

  const handleLogoClick = () => {
    if (location.pathname === '/') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      navigate('/');
    }
  };

  return (
    <Suspense fallback={<PageLoader />}>
      <SearchResultsPage
        initialQuery={initialQuery}
        onCartClick={() => setIsCartOpen(true)}
        onLogoClick={handleLogoClick}
        onLoginClick={() => navigate('/login')}
        onAccountClick={() => navigate('/account')}
        onBookClick={(book) => navigate(`/book/${book.bookId}`)}
        onSearch={(query) => navigate(query.trim() ? `/search/${encodeURIComponent(query.trim())}` : '/search')}
      />
      <Cart isOpen={isCartOpen} onClose={() => setIsCartOpen(false)} />
    </Suspense>
  );
}

function AccountPageWrapper() {
  const navigate = useNavigate();
  const location = useLocation();
  const [isCartOpen, setIsCartOpen] = useState(false);

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const handleLogoClick = () => {
    if (location.pathname === '/') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      navigate('/');
    }
  };

  return (
    <Suspense fallback={<PageLoader />}>
      <AccountPage
        onBack={() => navigate('/')}
        onCartClick={() => setIsCartOpen(true)}
        onSearch={(query) => navigate(query.trim() ? `/search/${encodeURIComponent(query.trim())}` : '/search')}
        onLogoClick={handleLogoClick}
        onLoginClick={() => navigate('/login')}
        onBookClick={(book) => navigate(`/book/${book.bookId}`)}
      />
      <Cart isOpen={isCartOpen} onClose={() => setIsCartOpen(false)} />
    </Suspense>
  );
}

function LoginPageWrapper() {
  const navigate = useNavigate();
  const location = useLocation();

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const handleLogoClick = () => {
    if (location.pathname === '/') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      navigate('/');
    }
  };

  return (
    <Suspense fallback={<PageLoader />}>
      <LoginPage
        onLoginSuccess={() => navigate('/')}
        onLogoClick={handleLogoClick}
      />
    </Suspense>
  );
}

function ForgotPasswordPageWrapper() {
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  return (
    <Suspense fallback={<PageLoader />}>
      <ForgotPasswordPage />
    </Suspense>
  );
}

function ResetPasswordSuccessPageWrapper() {
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  return (
    <Suspense fallback={<PageLoader />}>
      <ResetPasswordSuccessPage />
    </Suspense>
  );
}

function CheckoutPageWrapper() {
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  return (
    <Suspense fallback={<PageLoader />}>
      <CheckoutPage />
    </Suspense>
  );
}

function PaymentPageWrapper() {
  const { orderId } = useParams<{ orderId: string }>();

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, [orderId]);

  return (
    <Suspense fallback={<PageLoader />}>
      <PaymentPage orderId={orderId || ''} />
    </Suspense>
  );
}

/**
 * AdminNavOverlay — dùng ref + direct DOM manipulation để bypass React batching.
 *
 * Lý do dùng ref thay vì useState:
 * - window.dispatchEvent() là ĐỒNG BỘ
 * - Header gọi dispatchEvent('adminNavStart') TRƯỚC navigate()
 * - Listener chạy ngay → overlayRef.current.style.opacity = '1' (DOM mutation)
 * - Browser repaint NGAY (không cần chờ React commit)
 * - navigate() sau đó mới bắt đầu lazy-load module
 */
function AdminNavOverlay() {
  const overlayRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLDivElement>(null);
  const startTimeRef = useRef<number>(0);
  const hideTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const MIN_DISPLAY_MS = 1400;

  useEffect(() => {
    const show = () => {
      const el = overlayRef.current;
      const bar = barRef.current;
      if (!el || !bar) return;

      // Dừng hide timer nếu có
      if (hideTimerRef.current) { clearTimeout(hideTimerRef.current); hideTimerRef.current = undefined; }

      startTimeRef.current = Date.now();

      // Reset CSS animation: set none → force reflow → restore
      // (đây là cách chuẩn W3C để restart animation mà không thay đổi DOM structure)
      bar.style.animation = 'none';
      void bar.offsetHeight; // Bắt buộc browser flush style → animation reset về frame 0
      bar.style.animation = 'plFill 15s cubic-bezier(0.1, 0.05, 0.01, 1) forwards, plShimmer 1.4s ease-in-out infinite';

      // Hiện overlay NGAY (DOM mutation, không qua React state)
      el.style.opacity = '1';
      el.style.visibility = 'visible';
      el.style.pointerEvents = 'auto';
    };

    const scheduleHide = () => {
      if (hideTimerRef.current) return;
      const elapsed = Date.now() - startTimeRef.current;
      const delay = Math.max(0, MIN_DISPLAY_MS - elapsed) + 400;
      hideTimerRef.current = setTimeout(() => {
        const el = overlayRef.current;
        if (el) {
          el.style.opacity = '0';
          el.style.pointerEvents = 'none';
          // Ẩn hoàn toàn sau khi transition xong
          setTimeout(() => { if (el) el.style.visibility = 'hidden'; }, 500);
        }
        hideTimerRef.current = undefined;
      }, delay);
    };

    // Fallback: ẩn sau 20s nếu adminPageReady không được dispatch
    const fallback = () => {
      setTimeout(() => scheduleHide(), 20_000);
    };

    window.addEventListener('adminNavStart', show);
    window.addEventListener('adminPageReady', scheduleHide);
    window.addEventListener('adminNavStart', fallback);

    return () => {
      window.removeEventListener('adminNavStart', show);
      window.removeEventListener('adminPageReady', scheduleHide);
      window.removeEventListener('adminNavStart', fallback);
      if (hideTimerRef.current) clearTimeout(hideTimerRef.current);
    };
  }, []);

  // Overlay luôn mounted (display:none via opacity+visibility), điều khiển qua DOM ref
  return (
    <div
      ref={overlayRef}
      style={{
        position: 'fixed', inset: 0, zIndex: 9999,
        display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
        gap: 20, background: 'var(--background, #fff)',
        opacity: 0, visibility: 'hidden', pointerEvents: 'none',
        transition: 'opacity 0.4s ease',
      }}
    >
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor"
        strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
        style={{ opacity: 0.55, color: '#94a3b8', animation: 'plPulse 2s ease-in-out infinite' }}>
        <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20" />
      </svg>
      <div style={{ fontSize: '13px', color: '#64748b', fontWeight: 500 }}>
        Đang tải thư viện giao diện...
      </div>
      <div style={{ width: 220, height: 4, background: '#e2e8f0', borderRadius: 4, overflow: 'hidden' }}>
        <div
          ref={barRef}
          style={{
            height: '100%', borderRadius: 4,
            background: 'linear-gradient(90deg, #0f172a 0%, #475569 45%, #0f172a 100%)',
            backgroundSize: '300% 100%',
            animation: 'plFill 15s cubic-bezier(0.1, 0.05, 0.01, 1) forwards, plShimmer 1.4s ease-in-out infinite',
          }}
        />
      </div>
      <style>{`
        @keyframes plFill {
          0%   { width: 0%;  }
          5%   { width: 15%; }
          12%  { width: 30%; }
          22%  { width: 44%; }
          35%  { width: 56%; }
          50%  { width: 65%; }
          65%  { width: 73%; }
          78%  { width: 80%; }
          88%  { width: 85%; }
          95%  { width: 88%; }
          100% { width: 91%; }
        }
        @keyframes plShimmer {
          0%  {background-position:150% center}
          100%{background-position:-150% center}
        }
        @keyframes plPulse {
          0%,100%{opacity:0.55;transform:scale(1)}
          50%{opacity:0.25;transform:scale(1.06)}
        }
      `}</style>
    </div>
  );
}

function AdminPageWrapper() {
  const { user, isLoggedIn } = useAuth();
  const navigate = useNavigate();
  const { books } = useAdmin();

  useEffect(() => {
    if (!isLoggedIn || (user?.role !== 'admin' && user?.role !== 'staff')) {
      navigate('/');
    }
  }, [isLoggedIn, user, navigate]);

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  // Thông báo AdminNavOverlay ẩn overlay khi books sẵn sàng
  useEffect(() => {
    if (books.length > 0) window.dispatchEvent(new Event('adminPageReady'));
  }, [books.length]);

  // Fallback: báo sau 15s nếu data không load được
  useEffect(() => {
    const t = setTimeout(() => window.dispatchEvent(new Event('adminPageReady')), 15_000);
    return () => clearTimeout(t);
  }, []);

  return (
    <DashboardFilterProvider>
      <Suspense fallback={<PageLoader />}>
        <AdminPage />
      </Suspense>
    </DashboardFilterProvider>
  );
}

// Inner component that uses OrderContext
function AppContent() {
  const { createOrder } = useOrder();

  // Initialize inventory and migrate data on app load (run async for better performance)
  useEffect(() => {
    // Run initialization in background to avoid blocking render
    const initializeApp = async () => {
      try {
        // Run migrations first
        migrateOrderStatus();

        // Then initialize inventory
        // Backend handles inventory initialization

        // Initialize auto-transitions for orders
        OrderWorkflowService.initAutoTransitions();

        console.log('App initialized successfully');
      } catch (error) {
        console.error('Failed to initialize app:', error);
      }
    };

    // Use setTimeout to defer initialization after initial render
    const timeoutId = setTimeout(() => {
      initializeApp();
    }, 100);

    return () => clearTimeout(timeoutId);
  }, []);

  return (
    <CartProvider createOrder={createOrder}>
      {/* Overlay hiển ngay khi navigate đến /admin, trước khi lazy module tải */}
      <AdminNavOverlay />
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/book/:id" element={<BookDetailPageWrapper />} />
        <Route path="/search/:query?" element={<SearchResultsPageWrapper />} />
        <Route path="/account" element={<AccountPageWrapper />} />
        <Route path="/login" element={<LoginPageWrapper />} />
        <Route path="/forgot-password" element={<ForgotPasswordPageWrapper />} />
        <Route path="/bookdb/auth/login/google" element={<GoogleCallbackPage />} />
        <Route path="/reset-password-success" element={<ResetPasswordSuccessPageWrapper />} />
        <Route path="/checkout" element={<CheckoutPageWrapper />} />
        <Route path="/payment/:orderId" element={<PaymentPageWrapper />} />
        <Route path="/payment/return" element={<Suspense fallback={<PageLoader />}><PaymentReturnPage /></Suspense>} />
        <Route path="/payment/cancel" element={<Suspense fallback={<PageLoader />}><PaymentCancelPage /></Suspense>} />
        <Route path="/admin" element={<AdminPageWrapper />} />
        {/* Catch-all route - redirect to home */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>

      <Toaster position="top-right" richColors visibleToasts={1} />

      {/* ── Chat Widget (hiển thị toàn trang, role-aware) ── */}
      <ChatWidget />
    </CartProvider>
  );
}

// Main App component with all providers
function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {/* AdminProvider nằm bên trong AuthProvider để dùng useAuth */}
          {/* Nằm bên ngoài Routes nên không bị unmount khi navigate */}
          {/* Data được prefetch ngay khi login admin/staff, trước khi vo /admin */}
          <AdminProvider>
            <OrderProvider>
              <Suspense fallback={<PageLoader />}>
                <AppContent />
              </Suspense>
            </OrderProvider>
          </AdminProvider>
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

export default App;
