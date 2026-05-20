import React, { useState, useMemo, useEffect } from 'react';
import { useParams, useLocation, useSearchParams } from 'react-router-dom';
import { Search, Grid3X3, List, ChevronDown, X } from 'lucide-react';
import { Header } from '../layouts/Header';
import { BookCard } from '../features/book/BookCard';
import { Footer } from '../layouts/Footer';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { Checkbox } from '../components/ui/checkbox';
import { Badge } from '../components/ui/badge';
import { Separator } from '../components/ui/separator';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '../components/ui/collapsible';
import { 
  Pagination, 
  PaginationContent, 
  PaginationItem, 
  PaginationLink, 
  PaginationNext, 
  PaginationPrevious 
} from '../components/ui/pagination';
import { BookService, PythonSearchService } from '../services';
import { ImageService, toProxiedUrl } from '../services/imageService';
import { Book } from '../types/book';

interface SearchResultsPageProps {
  initialQuery?: string;
  onCartClick?: () => void;
  onLogoClick?: () => void;
  onLoginClick?: () => void;
  onAccountClick?: () => void;
  onBookClick?: (book: Book) => void;
  onSearch?: (query: string) => void;
}

const priceRanges = [
  { label: 'Dưới 50.000đ', min: 0, max: 50000 },
  { label: '50.000đ - 100.000đ', min: 50000, max: 100000 },
  { label: '100.000đ - 200.000đ', min: 100000, max: 200000 },
  { label: '200.000đ - 300.000đ', min: 200000, max: 300000 },
  { label: 'Trên 300.000đ', min: 300000, max: Infinity },
];

export const SearchResultsPage: React.FC<SearchResultsPageProps> = ({
  initialQuery = '',
  onCartClick,
  onLogoClick,
  onLoginClick,
  onAccountClick,
  onBookClick,
  onSearch
}) => {
  const [books, setBooks] = useState<Book[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState<string>(initialQuery);
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [selectedAuthors, setSelectedAuthors] = useState<string[]>([]);
  const [selectedPriceRanges, setSelectedPriceRanges] = useState<number[]>([]);
  const [minRating, setMinRating] = useState<number>(0);
  const [sortBy, setSortBy] = useState<string>('relevance');
  const [viewMode, setViewMode] = useState<'grid' | 'list'>('grid');
  const [currentPage, setCurrentPage] = useState<number>(1);
  const booksPerPage = 20;
  
  // Limit display for performance - only show top items
  const MAX_FILTER_ITEMS = 10;
  
  // Keep original search result filters (like Shopee)
  const [searchResultCategories, setSearchResultCategories] = useState<string[]>([]);
  const [searchResultAuthors, setSearchResultAuthors] = useState<string[]>([]);
  const [totalResults, setTotalResults] = useState<number>(0);


  const params = useParams<{ query?: string }>();
  const [searchParamsQ] = useSearchParams();

  // Helper: decode an already-decoded (by React Router) path param safely
  // React Router gọi decodeURIComponent 1 lần cho path param.
  // Nếu gọi lần 2 mà chuỗi có '%' không theo mẫu hex → URIError → trang trắng.
  const safeDecodeParam = (raw: string | undefined): string => {
    if (!raw) return '';
    try {
      // Thử decode thêm 1 lần nếu còn mã hóa (VD: params.query = '1%25 Nỗ Lực')
      return decodeURIComponent(raw);
    } catch {
      // Nếu lỗi → trả về nguyên chuỗi đã được React Router decode
      return raw;
    }
  };

  useEffect(() => {
    // Ưu tiên ?q= query string, fallback sang :query path param
    const q = searchParamsQ.get('q') || safeDecodeParam(params.query) || '';
    setSearchQuery(q);
    setCurrentPage(1);
    // Reset all filters when search query changes
    setSelectedCategories([]);
    setSelectedAuthors([]);
    setSelectedPriceRanges([]);
    setMinRating(0);
  }, [params.query, searchParamsQ]);

  // Đọc OCR pre-fetched results từ Router state (khi navigate từ OCR modal "Xem tất cả")
  const location = useLocation();
  const ocrState = (location.state as { ocrResults?: any[]; ocrQuery?: string } | null);
  const ocrPreResults: any[] | null = ocrState?.ocrResults ?? null;

  // useRef để giữ giá trị OCR ổn định qua các lần render
  // Đặt ngay trong render body (trước mọi useEffect) để cả 2 effects đọc được giá trị đúng
  const ocrResultsRef = React.useRef<any[] | null>(null);
  if (ocrPreResults && ocrPreResults.length > 0) {
    // Có OCR state mới → cập nhật ref
    ocrResultsRef.current = ocrPreResults;
  } else if (!ocrState) {
    // Không có OCR state (navigate thường) → xóa ref để fetchBooks chạy được
    ocrResultsRef.current = null;
  }

  useEffect(() => {
    // Nếu có kết quả OCR sẵn trong Router state → hiển thị ngay, không fetch lại
    const ocr = ocrResultsRef.current;
    if (!ocr || ocr.length === 0) return;

    const loadOcrResults = async () => {
      const mapped: Book[] = ocr.map((item: any) => ({
        bookId:        item.book_id,
        id:            item.book_id,
        title:         item.title        || '',
        // OcrBookResult dùng author_name (có thể null) – fallback sang tên tác giả từ book_info
        authorName:    item.author_name  || item.authors?.[0] || '',
        author:        item.author_name  || item.authors?.[0] || '',
        price:         item.price        || 0,
        avgRating:     item.avg_rating   || item.rating || 0,
        rating:        item.avg_rating   || item.rating || 0,
        ratingCount:   item.rating_count || 0,
        description:   item.description  || '',
        // OcrBookResult dùng image_url (không phải main_image_url)
        imageUrl:      toProxiedUrl(item.image_url || item.main_image_url || ''),
        categories:    item.categories?.length
          ? [{ categoryId: 0, categoryName: item.categories[0] }]
          : [{ categoryId: 0, categoryName: 'Khác' }],
        category:      item.categories?.[0] || 'Khác',
        stockQuantity: item.stock_quantity ?? 1,
      }));

      // Hiển thị ngay để UI phản hồi tức thì
      setBooks(mapped);
      setTotalResults(mapped.length);
      setIsLoading(false);

      // Xây dựng filter lists ban đầu
      const cats = [...new Set(mapped.flatMap(b =>
        b.categories?.map((c: any) => c.categoryName ?? c) ?? []
      ))].filter(Boolean) as string[];
      const authors = [...new Set(mapped.map(b => b.authorName).filter(Boolean))] as string[];
      setSearchResultCategories(cats);
      setSearchResultAuthors(authors);

      // ── Enrich từ OpenSearch: lấy đủ ảnh bìa, tác giả, đánh giá ──────────────
      // OCR service chỉ trả về book_id + title + image_url (thường thiếu author, avg_rating)
      // Dùng title của cuốn đầu tiên để search OpenSearch lấy đầy đủ dữ liệu
      try {
        const enrichQuery = ocr[0]?.title || ocrState?.ocrQuery || '';
        if (enrichQuery) {
          const enrichResp = await PythonSearchService.search({
            q: enrichQuery,
            page: 1,
            limit: Math.max(ocr.length + 5, 15),
          });
          const enrichedItems: any[] = enrichResp.items || enrichResp.results || [];

          if (enrichedItems.length > 0) {
            // Tạo map book_id → enriched data từ OpenSearch
            const enrichMap = new Map<number, any>();
            enrichedItems.forEach(item => {
              if (item.book_id) enrichMap.set(item.book_id, item);
            });

            // Nếu OCR book_id không match (vì OCR dùng lenient search) →
            // fallback: dùng kết quả OpenSearch theo thứ tự
            let enrichedBooks: Book[];
            const allFromOCRMatched = ocr.every((o: any) => enrichMap.has(o.book_id));

            if (allFromOCRMatched) {
              // Tất cả sách OCR đều có trong OpenSearch → merge data
              enrichedBooks = mapped.map(b => {
                const en = enrichMap.get(b.bookId);
                if (!en) return b;
                const categoryName = (en.categories && en.categories.length > 0)
                  ? en.categories[0] : (b.category || 'Khác');
                return {
                  ...b,
                  authorName:    en.author_name    || b.authorName,
                  author:        en.author_name    || b.author,
                  avgRating:     en.avg_rating     ?? b.avgRating,
                  rating:        en.avg_rating     ?? b.rating,
                  ratingCount:   en.rating_count   ?? b.ratingCount,
                  imageUrl:      toProxiedUrl(en.main_image_url || en.image_url || b.imageUrl),
                  categories:    [{ categoryId: 0, categoryName }],
                  category:      categoryName,
                  stockQuantity: en.stock_quantity ?? b.stockQuantity,
                };
              });
            } else {
              // OCR book_id không khớp OpenSearch → dùng enriched list từ OpenSearch
              enrichedBooks = enrichedItems.slice(0, Math.max(ocr.length, enrichedItems.length)).map((item: any) => {
                const categoryName = (item.categories && item.categories.length > 0)
                  ? item.categories[0] : 'Khác';
                return {
                  bookId: item.book_id, id: item.book_id,
                  title: item.title || '',
                  authorName: item.author_name || '',
                  author: item.author_name || '',
                  price: item.price || 0,
                  avgRating: item.avg_rating || 0,
                  rating: item.avg_rating || 0,
                  ratingCount: item.rating_count || 0,
                  description: item.description || '',
                  imageUrl: toProxiedUrl(item.main_image_url || item.image_url || ''),
                  categories: [{ categoryId: 0, categoryName }],
                  category: categoryName,
                  stockQuantity: item.stock_quantity || 0,
                };
              });
            }

            // Presign ảnh bìa còn là relative path
            const imagePaths = enrichedBooks
              .map(b => b.imageUrl)
              .filter(url => url && !url.startsWith('http') && !url.startsWith('/minio'));
            if (imagePaths.length > 0) {
              const presigned = await ImageService.getPresignedUrls(imagePaths);
              enrichedBooks.forEach(b => {
                if (b.imageUrl && !b.imageUrl.startsWith('http') && !b.imageUrl.startsWith('/minio')) {
                  b.imageUrl = presigned[b.imageUrl] || toProxiedUrl(b.imageUrl);
                }
              });
            }

            setBooks(enrichedBooks);
            setTotalResults(enrichedBooks.length);

            // Cập nhật filter lists với dữ liệu đầy đủ hơn
            const enrichedCats = Array.from(new Set(enrichedBooks.map(b => b.category))).filter(Boolean).sort() as string[];
            const enrichedAuthors = Array.from(new Set(enrichedBooks.map(b => b.authorName))).filter(Boolean).sort() as string[];
            setSearchResultCategories(enrichedCats);
            setSearchResultAuthors(enrichedAuthors);
            return; // Đã enrich xong → bỏ qua presign bên dưới
          }
        }
      } catch (enrichErr) {
        console.warn('OCR enrich from OpenSearch failed (non-critical):', enrichErr);
      }

      // ── Fallback: presign relative paths từ OCR data gốc ──────────────────────
      const relativePaths = mapped
        .map(b => b.imageUrl)
        .filter((url): url is string => !!url && !url.startsWith('http') && !url.startsWith('/minio'));

      if (relativePaths.length > 0) {
        const presigned = await ImageService.getPresignedUrls(relativePaths);
        setBooks(prev => prev.map(b => ({
          ...b,
          imageUrl: presigned[b.imageUrl] || b.imageUrl,
        })));
      }
    };

    loadOcrResults();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.state]);

  useEffect(() => {
    const fetchBooks = async () => {
      // Nếu đang dùng OCR pre-results → không fetch lại
      // Dùng ref thay vì ocrPreResults để tránh stale closure race condition
      if (ocrResultsRef.current && ocrResultsRef.current.length > 0) return;
      setIsLoading(true);
      try {
        const searchParams: any = {
          q: searchQuery.trim(),
          page: 1,
          limit: 20
        };

        // Don't send category filter to API - filter client-side like authors

          if (sortBy) {
            const sortMapping: Record<string, string> = {
              'relevance': 'relevance',
              'price-asc': 'price_asc',
              'price-desc': 'price_desc',
              'newest': 'newest',
              'rating': 'rating_desc',
              'popular': 'relevance', // fallback
              'name': 'relevance' // fallback
            };
            searchParams.sort = sortMapping[sortBy] || 'relevance';
          }

          const searchResponse = await PythonSearchService.search(searchParams);
          
          
          const results = Array.isArray(searchResponse) 
            ? searchResponse 
            : (searchResponse.items || searchResponse.results || []);
          
          // Map first page results immediately for fast display
          let fetchedBooks = results.map((item: any) => {
            const categoryName = (item.categories && item.categories.length > 0) 
              ? item.categories[0] 
              : (item.category_name || 'Khác');
            
            return {
              bookId: item.book_id,
              id: item.book_id,
              title: item.title || '',
              authorName: item.author_name || 'Unknown',
              author: item.author_name || 'Unknown',
              price: item.price || 0,
              avgRating: item.avg_rating || 0,
              rating: item.avg_rating || 0,
              ratingCount: item.rating_count || 0,
              description: item.description || '',
              imageUrl: toProxiedUrl(item.main_image_url || item.image_url || ''),
              categories: [{ categoryId: 0, categoryName: categoryName }],
              category: categoryName,
              stockQuantity: item.stock_quantity || 0,
              _score: item._score
            };
          });

          // Get presigned URLs for raw relative paths (not yet http or /minio)
          let imagePaths = fetchedBooks
            .map(book => book.imageUrl)
            .filter(url => url && !url.startsWith('http') && !url.startsWith('/minio'));

          if (imagePaths.length > 0) {
            const presignedUrls = await ImageService.getPresignedUrls(imagePaths);
            fetchedBooks.forEach(book => {
              if (book.imageUrl && !book.imageUrl.startsWith('http') && !book.imageUrl.startsWith('/minio')) {
                book.imageUrl = presignedUrls[book.imageUrl] || toProxiedUrl(book.imageUrl);
              }
            });
          }
          
          // Set first page immediately for fast display
          setBooks(fetchedBooks);
          setIsLoading(false);
          
          // Check if we need to fetch more pages (if total > limit)
          const total = searchResponse.total || results.length;
          setTotalResults(total);
          
          // Fetch first 5 pages for filters (100 books)
          if (total > 20 && searchParams.limit === 20) {
            try {
              const filterPages = Math.min(5, Math.ceil(total / 20));
              const filterFetches = [];
              
              for (let page = 2; page <= filterPages; page++) {
                filterFetches.push(
                  PythonSearchService.search({ ...searchParams, page })
                    .then(resp => resp.items || resp.results || [])
                );
              }
              
              const filterResults = await Promise.all(filterFetches);
              const booksForFilters = [...fetchedBooks];
              
              filterResults.forEach(pageResults => {
                pageResults.forEach((item: any) => {
                  const categoryName = (item.categories && item.categories.length > 0) 
                    ? item.categories[0] 
                    : (item.category_name || 'Khác');
                  
                  booksForFilters.push({
                    bookId: item.book_id,
                    category: categoryName,
                    author: item.author_name || 'Unknown'
                  } as any);
                });
              });
              
              // Save categories and authors from first 5 pages only
              const initialCategories = Array.from(new Set(booksForFilters.map(b => b.category))).sort();
              const initialAuthors = Array.from(new Set(booksForFilters.map(b => b.author))).sort();
              setSearchResultCategories(initialCategories);
              setSearchResultAuthors(initialAuthors);
            } catch (filterErr) {
              console.warn('Failed to fetch background pages for filters:', filterErr);
              // Fallback to only using first page for filters
              const initialCategories = Array.from(new Set(fetchedBooks.map(b => b.category))).sort();
              const initialAuthors = Array.from(new Set(fetchedBooks.map(b => b.author))).sort();
              setSearchResultCategories(initialCategories);
              setSearchResultAuthors(initialAuthors);
            }
          } else {
            // Less than 5 pages, use all books
            const initialCategories = Array.from(new Set(fetchedBooks.map(b => b.category))).sort();
            const initialAuthors = Array.from(new Set(fetchedBooks.map(b => b.author))).sort();
            setSearchResultCategories(initialCategories);
            setSearchResultAuthors(initialAuthors);
          }
          
          // Fetch remaining pages in background
          if (total > 100 && searchParams.limit === 20) {
            // Giới hạn tải tối đa 25 trang (500 sách) để tránh dính Rate Limit 429 Too Many Requests
            const totalPages = Math.min(25, Math.ceil(total / 20));
            const batchSize = 5;
            const allResults = [...results];
            
            // Fetch in background without blocking UI, starting from page 6
            (async () => {
              try {
                let allLoadedBooks = [...fetchedBooks]; // Track all books loaded
                let batchCount = 0;
                const updateInterval = 10; // Update UI every 10 batches to reduce flickering
                
                for (let startPage = 6; startPage <= totalPages; startPage += batchSize) {
                  const endPage = Math.min(startPage + batchSize - 1, totalPages);
                  const batchFetches = [];
                  
                  for (let page = startPage; page <= endPage; page++) {
                    batchFetches.push(
                      PythonSearchService.search({ ...searchParams, page })
                        .then(resp => resp.items || resp.results || [])
                    );
                  }
                  
                  const batchResults = await Promise.all(batchFetches);
                  batchResults.forEach(pageResults => {
                    allResults.push(...pageResults);
                  });
                  
                  // Map only the new results from this batch
                  const newBooksFromBatch = batchResults.flat().map((item: any) => {
                    const categoryName = (item.categories && item.categories.length > 0) 
                      ? item.categories[0] 
                      : (item.category_name || 'Khác');
                    
                    return {
                      bookId: item.book_id,
                      id: item.book_id,
                      title: item.title || '',
                      authorName: item.author_name || 'Unknown',
                      author: item.author_name || 'Unknown',
                      price: item.price || 0,
                      avgRating: item.avg_rating || 0,
                      rating: item.avg_rating || 0,
                      ratingCount: item.rating_count || 0,
                      description: item.description || '',
                      imageUrl: item.main_image_url || item.image_url || '',
                      categories: [{ categoryId: 0, categoryName: categoryName }],
                      category: categoryName,
                      stockQuantity: item.stock_quantity || 0,
                      _score: item._score
                    };
                  });
                  
                  // Get presigned URLs for new batch only
                  const newImagePaths = newBooksFromBatch
                    .map(book => book.imageUrl)
                    .filter(url => url && !url.startsWith('http'));
                  
                  if (newImagePaths.length > 0) {
                    const presignedUrls = await ImageService.getPresignedUrls(newImagePaths);
                    newBooksFromBatch.forEach(book => {
                      if (book.imageUrl && !book.imageUrl.startsWith('http')) {
                        book.imageUrl = presignedUrls[book.imageUrl] || book.imageUrl;
                      }
                    });
                  }
                  
                  allLoadedBooks = [...allLoadedBooks, ...newBooksFromBatch];
                  batchCount++;
                  
                  // Update UI only every X batches or on the last batch to reduce flickering
                  const isLastBatch = startPage + batchSize > totalPages;
                  if (batchCount % updateInterval === 0 || isLastBatch) {
                    setBooks([...allLoadedBooks]);
                  }
                }
              } catch (err) {
                console.error('Background fetch error:', err);
              }
            })();
          }
      } catch (error) {
        console.error('Failed to fetch books:', error);
        setBooks([]);
        setIsLoading(false);
      }
    };

    fetchBooks();
  }, [searchQuery, sortBy]); // Re-fetch only when query or sort changes

  const unifiedBooks = useMemo(() =>
    books.map((b: any) => ({
      id: String(b.bookId ?? b.id ?? ''),
      title: b.title ?? '',
      author: b.authorName ?? b.author ?? 'Unknown',
      category: (b.categories && b.categories.length > 0) ? (b.categories[0].categoryName ?? b.categories[0]) : (b.category ?? 'Khác'),
      price: b.price ?? 0,
      rating: b.avgRating ?? b.rating ?? 0,
      publishedYear: b.publicationYear ?? b.publishedYear ?? 0,
      reviewCount: b.ratingCount ?? b.reviewCount ?? 0,
      description: b.description ?? '',
      _raw: b,
    })),
    [books]
  );

  const categories = useMemo(() => Array.from(new Set(unifiedBooks.map(book => book.category))).sort(), [unifiedBooks]);
  const authors = useMemo(() => Array.from(new Set(unifiedBooks.map(book => book.author))).sort(), [unifiedBooks]);

  // Display filters from initial search results (Shopee-style - keep filters fixed)
  const displayCategories = searchResultCategories.length > 0 ? searchResultCategories : categories;
  const displayAuthors = searchResultAuthors.length > 0 ? searchResultAuthors : authors;

  const { filteredAndSortedBooks, totalPages, paginatedBooks } = useMemo(() => {
    let filtered: any[] = unifiedBooks;


    if (selectedCategories.length > 0) {
      filtered = filtered.filter(book => selectedCategories.includes(book.category));
    }

    if (selectedAuthors.length > 0) {
      filtered = filtered.filter(book => selectedAuthors.includes(book.author));
    }

    if (selectedPriceRanges.length > 0) {
      filtered = filtered.filter(book => {
        return selectedPriceRanges.some(rangeIndex => {
          const range = priceRanges[rangeIndex];
          return book.price >= range.min && book.price <= range.max;
        });
      });
    }

    if (minRating > 0) {
      filtered = filtered.filter(book => book.rating >= minRating);
    }

    // Sort books
    const sorted = [...filtered].sort((a, b) => {
      switch (sortBy) {
        case 'price-asc':
          return a.price - b.price;
        case 'price-desc':
          return b.price - a.price;
        case 'rating':
          return b.rating - a.rating;
        case 'newest':
          return b.publishedYear - a.publishedYear;
        case 'name':
          return a.title.localeCompare(b.title, 'vi');
        case 'popular':
          return b.reviewCount - a.reviewCount;
        case 'relevance':
        default:
          // Simple relevance scoring based on search query match
          if (!searchQuery.trim()) return b.reviewCount - a.reviewCount;
          const query = searchQuery.toLowerCase();
          const aScore = (a.title.toLowerCase().includes(query) ? 3 : 0) +
                        (a.author.toLowerCase().includes(query) ? 2 : 0) +
                        (a.category.toLowerCase().includes(query) ? 1 : 0);
          const bScore = (b.title.toLowerCase().includes(query) ? 3 : 0) +
                        (b.author.toLowerCase().includes(query) ? 2 : 0) +
                        (b.category.toLowerCase().includes(query) ? 1 : 0);
          return bScore - aScore;
      }
    });

  // Pagination
  const totalPages = Math.ceil(sorted.length / booksPerPage);
    const startIndex = (currentPage - 1) * booksPerPage;
    const endIndex = startIndex + booksPerPage;
    const paginatedBooks = sorted.slice(startIndex, endIndex);

    return { 
      filteredAndSortedBooks: sorted, 
      totalPages, 
      paginatedBooks 
    };
  }, [unifiedBooks, searchQuery, selectedCategories, selectedAuthors, selectedPriceRanges, minRating, sortBy, currentPage]);

  const handleSearch = (query: string) => {
    if (onSearch) {
      onSearch(query);
    } else {
      setSearchQuery(query);
      setCurrentPage(1);
    }
  };

  const handleCategoryToggle = (category: string) => {
    setSelectedCategories(prev => 
      prev.includes(category) 
        ? prev.filter(c => c !== category)
        : [...prev, category]
    );
    setCurrentPage(1);
  };

  const handleAuthorToggle = (author: string) => {
    setSelectedAuthors(prev => 
      prev.includes(author) 
        ? prev.filter(a => a !== author)
        : [...prev, author]
    );
    setCurrentPage(1);
  };

  const handlePriceRangeToggle = (rangeIndex: number) => {
    setSelectedPriceRanges(prev => 
      prev.includes(rangeIndex) 
        ? prev.filter(r => r !== rangeIndex)
        : [...prev, rangeIndex]
    );
    setCurrentPage(1);
  };

  const clearAllFilters = () => {
    setSelectedCategories([]);
    setSelectedAuthors([]);
    setSelectedPriceRanges([]);
    setMinRating(0);
    setCurrentPage(1);
  };

  const getActiveFiltersCount = () => {
    return selectedCategories.length + selectedAuthors.length + selectedPriceRanges.length + (minRating > 0 ? 1 : 0);
  };

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    // Scroll to top of page
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  // Sidebar content
  const SidebarContent = () => (
    <div className="space-y-6">
      {/* Sort Options */}
      <div>
        <h3 className="font-medium mb-3">Sắp xếp theo</h3>
        <Select value={sortBy} onValueChange={setSortBy}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="relevance">{searchQuery.trim() ? 'Độ liên quan' : 'Mặc định'}</SelectItem>
            <SelectItem value="popular">Phổ biến nhất</SelectItem>
            <SelectItem value="newest">Mới nhất</SelectItem>
            <SelectItem value="rating">Đánh giá cao</SelectItem>
            <SelectItem value="price-asc">Giá thấp đến cao</SelectItem>
            <SelectItem value="price-desc">Giá cao đến thấp</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <Separator />

      {/* Active Filters */}
      {getActiveFiltersCount() > 0 && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-medium">Bộ lọc đang áp dụng</h3>
            <Button variant="ghost" size="sm" onClick={clearAllFilters}>
              Xóa tất cả
            </Button>
          </div>
          <div className="flex flex-wrap gap-2">
            {selectedCategories.map(category => (
              <Badge key={category} variant="secondary" className="cursor-pointer" onClick={() => handleCategoryToggle(category)}>
                {category}
                <X className="h-3 w-3 ml-1" />
              </Badge>
            ))}
            {selectedAuthors.map(author => (
              <Badge key={author} variant="secondary" className="cursor-pointer" onClick={() => handleAuthorToggle(author)}>
                {author}
                <X className="h-3 w-3 ml-1" />
              </Badge>
            ))}
            {selectedPriceRanges.map(rangeIndex => (
              <Badge key={rangeIndex} variant="secondary" className="cursor-pointer" onClick={() => handlePriceRangeToggle(rangeIndex)}>
                {priceRanges[rangeIndex].label}
                <X className="h-3 w-3 ml-1" />
              </Badge>
            ))}
            {minRating > 0 && (
              <Badge variant="secondary" className="cursor-pointer" onClick={() => setMinRating(0)}>
                {minRating}+ sao
                <X className="h-3 w-3 ml-1" />
              </Badge>
            )}
          </div>
        </div>
      )}

      <Separator />

      {/* Categories */}
      <Collapsible>
        <CollapsibleTrigger className="flex items-center justify-between w-full">
          <h3 className="font-medium">Thể loại</h3>
          <ChevronDown className="h-4 w-4" />
        </CollapsibleTrigger>
        <CollapsibleContent className="space-y-2 mt-3 max-h-48 overflow-y-auto">
          {displayCategories.slice(0, MAX_FILTER_ITEMS).map(category => (
            <div key={category} className="flex items-center space-x-2">
              <Checkbox
                id={`category-${category}`}
                checked={selectedCategories.includes(category)}
                onCheckedChange={() => handleCategoryToggle(category)}
              />
              <label htmlFor={`category-${category}`} className="text-sm cursor-pointer flex-1">
                {category}
              </label>
            </div>
          ))}
        </CollapsibleContent>
      </Collapsible>

      <Separator />

      {/* Authors */}
      <Collapsible>
        <CollapsibleTrigger className="flex items-center justify-between w-full">
          <h3 className="font-medium">Tác giả</h3>
          <ChevronDown className="h-4 w-4" />
        </CollapsibleTrigger>
        <CollapsibleContent className="space-y-2 mt-3 max-h-48 overflow-y-auto">
          {displayAuthors.slice(0, MAX_FILTER_ITEMS).map(author => (
            <div key={author} className="flex items-center space-x-2">
              <Checkbox
                id={`author-${author}`}
                checked={selectedAuthors.includes(author)}
                onCheckedChange={() => handleAuthorToggle(author)}
              />
              <label htmlFor={`author-${author}`} className="text-sm cursor-pointer flex-1">
                {author}
              </label>
            </div>
          ))}
        </CollapsibleContent>
      </Collapsible>

      <Separator />

      {/* Price Range */}
      <Collapsible>
        <CollapsibleTrigger className="flex items-center justify-between w-full">
          <h3 className="font-medium">Khoảng giá</h3>
          <ChevronDown className="h-4 w-4" />
        </CollapsibleTrigger>
        <CollapsibleContent className="space-y-2 mt-3">
          {priceRanges.map((range, index) => (
            <div key={index} className="flex items-center space-x-2">
              <Checkbox
                id={`price-${index}`}
                checked={selectedPriceRanges.includes(index)}
                onCheckedChange={() => handlePriceRangeToggle(index)}
              />
              <label htmlFor={`price-${index}`} className="text-sm cursor-pointer flex-1">
                {range.label}
              </label>
            </div>
          ))}
        </CollapsibleContent>
      </Collapsible>

      <Separator />

      {/* Rating */}
      <div>
        <h3 className="font-medium mb-3">Đánh giá tối thiểu</h3>
  <Select value={minRating.toString()} onValueChange={(value: string) => setMinRating(Number(value))}>
          <SelectTrigger>
            <SelectValue placeholder="Chọn rating" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="0">Tất cả</SelectItem>
            <SelectItem value="1">1+ sao</SelectItem>
            <SelectItem value="2">2+ sao</SelectItem>
            <SelectItem value="3">3+ sao</SelectItem>
            <SelectItem value="4">4+ sao</SelectItem>
            <SelectItem value="5">5 sao</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <Header 
        onSearch={handleSearch}
        onCartClick={onCartClick}
        onLogoClick={onLogoClick}
        onLoginClick={onLoginClick}
        onAccountClick={onAccountClick}
      />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Page Title & View Mode Controls */}
        <div className="flex items-center justify-between gap-4 mb-6 flex-wrap">
          {/* Left: Title & Count */}
          <div>
            <h1 className="text-2xl font-bold mb-1">Kết quả tìm kiếm</h1>
            {searchQuery && (
              <p className="text-sm text-muted-foreground">
                {isLoading ? (
                  'Đang tìm kiếm...'
                ) : (
                  <>
                    Tìm thấy <span className="font-semibold">{filteredAndSortedBooks.length.toLocaleString()}</span> kết quả
                    {totalResults > filteredAndSortedBooks.length && (
                      <span className="text-xs"> (từ {totalResults.toLocaleString()} tổng)</span>
                    )}
                    {' '}cho "{searchQuery}"
                  </>
                )}
              </p>
            )}
          </div>

          {/* Right: View Mode Toggle */}
          <div className="flex items-center border rounded-md">
            <Button
              variant={viewMode === 'grid' ? 'secondary' : 'ghost'}
              size="sm"
              onClick={() => setViewMode('grid')}
              className="rounded-r-none"
            >
              <Grid3X3 className="h-4 w-4" />
            </Button>
            <Button
              variant={viewMode === 'list' ? 'secondary' : 'ghost'}
              size="sm"
              onClick={() => setViewMode('list')}
              className="rounded-l-none"
            >
              <List className="h-4 w-4" />
            </Button>
          </div>
        </div>

        <div className="flex gap-4 lg:gap-8">
          {/* Sidebar - Always visible */}
          <div className="w-64 sm:w-72 lg:w-80 flex-shrink-0">
            <div className="bg-card rounded-lg border p-4 sm:p-6 sticky top-24">
              <h2 className="font-semibold mb-4 sm:mb-6">Bộ lọc</h2>
              <SidebarContent />
            </div>
          </div>

          {/* Main Content */}
          <div className="flex-1 min-w-0">
            {/* Books Grid/List */}
            {paginatedBooks.length > 0 ? (
              <>
                <div className={
                  viewMode === 'grid' 
                    ? "grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-8"
                    : "space-y-4 mb-8"
                }>
                  {paginatedBooks.map((book, index) => (
                    <BookCard
                      key={book.id}
                      book={book._raw}
                      onClick={() => onBookClick?.(book._raw)}
                      variant={viewMode === 'list' ? 'list' : 'grid'}
                      priority={index < 4}
                    />
                  ))}
                </div>

                {/* Pagination */}
                {totalPages > 1 && (
                  <Pagination>
                    <PaginationContent>
                      <PaginationItem>
                        <PaginationPrevious 
                            size="default"
                            onClick={() => currentPage > 1 && handlePageChange(currentPage - 1)}
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
                                size="default"
                                onClick={() => handlePageChange(page as number)}
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
                          size="default"
                          onClick={() => currentPage < totalPages && handlePageChange(currentPage + 1)}
                          className={currentPage === totalPages ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                        />
                      </PaginationItem>
                    </PaginationContent>
                  </Pagination>
                )}
              </>
            ) : (
              <div className="text-center py-16">
                <div className="max-w-md mx-auto">
                  <Search className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
                  <h3 className="text-xl font-semibold mb-2">Không tìm thấy kết quả</h3>
                  <p className="text-muted-foreground mb-6">
                    Không có sách nào phù hợp với tiêu chí tìm kiếm của bạn. 
                    Hãy thử thay đổi từ khóa hoặc bộ lọc.
                  </p>
                  <div className="space-y-2">
                    <Button onClick={() => setSearchQuery('')} variant="outline">
                      Xóa từ khóa tìm kiếm
                    </Button>
                    {getActiveFiltersCount() > 0 && (
                      <Button onClick={clearAllFilters} variant="outline" className="ml-2">
                        Xóa tất cả bộ lọc
                      </Button>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      <Footer />
    </div>
  );
};
