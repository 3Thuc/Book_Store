import React, { useState, useMemo, useEffect } from 'react';
import { ArrowLeft, Star, ShoppingCart, Heart, Share2, Award, Clock, Package, Shield } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Separator } from '../components/ui/separator';
import { Input } from '../components/ui/input';
import { Textarea } from '../components/ui/textarea';
import { Label } from '../components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { Header } from '../layouts/Header';
import { Footer } from '../layouts/Footer';
import { BookRecommendations } from '../features/book/BookRecommendations';
import { ReviewsSection } from '../features/book/ReviewsSection';
import { ImageWithFallback } from '../components/fallbackimg/ImageWithFallback';
import { useCart } from '../context/CartContext';
import { useAuth } from '../context/AuthContext';
import { useOrder } from '../context/OrderContext';
import { books } from '../data/books';
import { toast } from 'sonner';
import { Book } from '../types/book';
import { formatVND } from '../lib/formatters';
import { PythonRecommendService } from '../services/pythonService';
import { ImageService } from '../services/imageService';

interface BookDetailPageProps {
  book: Book | null;
  onBack: () => void;
  onBookClick: (book: Book) => void;
  onCartClick: () => void;
  onSearch: (query: string) => void;
  onLogoClick: () => void;
  onLoginClick: () => void;
  onAccountClick: () => void;
}

interface ReviewData {
  rating: number;
  title: string;
  content: string;
}

export const BookDetailPage: React.FC<BookDetailPageProps> = ({ 
  book, 
  onBack, 
  onBookClick, 
  onCartClick, 
  onSearch, 
  onLogoClick, 
  onLoginClick, 
  onAccountClick 
}) => {
  const [quantity, setQuantity] = useState<number>(1);
  const [reviewData, setReviewData] = useState<ReviewData>({
    rating: 5,
    title: '',
    content: ''
  });
  const [isReviewDialogOpen, setIsReviewDialogOpen] = useState<boolean>(false);
  const [isDescriptionExpanded, setIsDescriptionExpanded] = useState<boolean>(false);
  const [similarBooks, setSimilarBooks] = useState<any[]>([]);
  const [popularBooks, setPopularBooks] = useState<any[]>([]);
  const [isLoadingRecommendations, setIsLoadingRecommendations] = useState(false);
  
  
  const { addToCart } = useCart();
  const { isLoggedIn } = useAuth();
  const { canReviewBook, writeReview, getReviewsForBook } = useOrder();

  const bookReviews = useMemo(() => {
    return getReviewsForBook(book ? String(book.bookId) : '');
  }, [book?.bookId, getReviewsForBook]);

  useEffect(() => {
    if (!book?.bookId) return;

    const fetchRecommendations = async () => {
      setIsLoadingRecommendations(true);
      try {
        const similarResponse = await PythonRecommendService.getAlsoBought(book.bookId, 30);
        const similarData = (similarResponse && typeof similarResponse === 'object' && 'items' in similarResponse && Array.isArray((similarResponse as any).items))
          ? (similarResponse as any).items
          : (Array.isArray(similarResponse) ? similarResponse : []);

        const popularData = await PythonRecommendService.getTrending(7, 10);

        const mappedSimilar = (Array.isArray(similarData) ? similarData : []).map((item: any) => ({
          bookId: item.book_id,
          title: item.title,
          author: item.author_name,
          price: item.price,
          avgRating: item.avg_rating,
          ratingCount: item.rating_count,
          stockQuantity: item.stock_quantity,
          imageUrl: item.main_image || item.image_url,
          categories: item.categories || []
        }));
        const mappedPopular = (Array.isArray(popularData) ? popularData : []).map((item: any) => ({
          bookId: item.book_id,
          title: item.title,
          author: item.author_name,
          price: item.price,
          avgRating: item.avg_rating,
          ratingCount: item.rating_count,
          stockQuantity: item.stock_quantity,
          imageUrl: item.main_image || item.image_url,
          categories: item.categories || []
        }));

        const allBooks = [...mappedSimilar, ...mappedPopular];
        const imagePaths = allBooks
          .map(b => b.imageUrl)
          .filter((path): path is string => !!path && !path.startsWith('http'));

        if (imagePaths.length > 0) {
          try {
            const presignedUrls = await ImageService.getPresignedUrls(imagePaths);
            mappedSimilar.forEach(book => {
              if (book.imageUrl && !book.imageUrl.startsWith('http')) {
                book.imageUrl = presignedUrls[book.imageUrl] || book.imageUrl;
              }
            });
            mappedPopular.forEach(book => {
              if (book.imageUrl && !book.imageUrl.startsWith('http')) {
                book.imageUrl = presignedUrls[book.imageUrl] || book.imageUrl;
              }
            });
          } catch (error) {
            console.error('Error getting presigned URLs:', error);
          }
        }

        setSimilarBooks(mappedSimilar);
        setPopularBooks(mappedPopular);
      } catch (error) {
        setSimilarBooks([]);
        setPopularBooks([]);
      } finally {
        setIsLoadingRecommendations(false);
      }
    };

    fetchRecommendations();
  }, [book?.bookId]);

  const sameCategoryBooks = useMemo(() => {
    if (!book) return [];
    const primaryCategory = book.categories?.[0]?.categoryName || '';
    return books
      .filter(b => b.category === primaryCategory && b.title !== book.title)
      .sort((a, b) => b.rating - a.rating) 
      .slice(0, 10)
      .map((b, idx) => ({
        bookId: parseInt(String(b.id).replace(/\D/g, '')) || -(idx + 1),
        title: b.title,
        authorName: b.author,
        price: b.price,
        publicationYear: b.publishedYear,
        description: b.description,
        avgRating: b.rating,
        ratingCount: b.reviewCount,
        stockQuantity: b.inStock ? 10 : 0,
        categories: [{ categoryId: 0, categoryName: b.category }],
        imageUrl: b.imageUrl
      } as any));
  }, [book]);

  const mixedRecommendations = useMemo(() => {
    if (!book) return [];

    const sameCategoryTitles = new Set(sameCategoryBooks.map(b => b.title));
    const sameAuthorBooks = books
      .filter(b =>
        b.author === book.author &&
        b.title !== book.title &&
        !sameCategoryTitles.has(b.title)
      )
      .slice(0, 5);

    const excludedTitles = new Set([
      book.title,
      ...sameCategoryTitles,
      ...sameAuthorBooks.map(b => b.title)
    ]);

    const topBooks = books
      .filter(b => !excludedTitles.has(b.title))
      .sort((a, b) => {
        const ratingDiff = b.rating - a.rating;
        if (Math.abs(ratingDiff) > 0.1) return ratingDiff;
        return b.reviewCount - a.reviewCount;
      })
      .slice(0, 8);

    const combined = [...sameAuthorBooks, ...topBooks];

    for (let i = combined.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [combined[i], combined[j]] = [combined[j], combined[i]];
    }

    return combined.slice(0, 10).map((b, idx) => ({
      bookId: parseInt(String(b.id).replace(/\D/g, '')) || -(idx + 1),
      title: b.title,
      authorName: b.author,
      price: b.price,
      publicationYear: b.publishedYear,
      description: b.description,
      avgRating: b.rating,
      ratingCount: b.reviewCount,
      stockQuantity: b.inStock ? 10 : 0,
      categories: [{ categoryId: 0, categoryName: b.category }],
      imageUrl: b.imageUrl
    } as any));
  }, [book, sameCategoryBooks]);

  if (!book) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold mb-4">Không tìm thấy sách</h2>
          <Button onClick={onBack}>Quay lại</Button>
        </div>
      </div>
    );
  }

  const handleAddToCart = () => {
    // Check if user is logged in
    if (!isLoggedIn) {
      toast.error('Bạn cần đăng nhập để thêm sản phẩm vào giỏ hàng');
      onLoginClick();
      return;
    }
    
    console.log('Book data:', { 
      stockQuantity: book?.stockQuantity, 
      availableQuantity: book?.availableQuantity 
    });
    addToCart(book, quantity);
  };

  const handleSubmitReview = async () => {
    if (!isLoggedIn) {
      toast.error('Bạn cần đăng nhập để viết đánh giá');
      return;
    }

    if (!reviewData.title.trim() || !reviewData.content.trim()) {
      toast.error('Vui lòng điền đầy đủ thông tin đánh giá');
      return;
    }

    try {    
      await writeReview({
        book_id: String(book.bookId),
        rating: reviewData.rating,
        review: reviewData.content.trim()
      });
      
      setIsReviewDialogOpen(false);
      setReviewData({ rating: 5, title: '', content: '' });
    } catch (error) {
      toast.error((error as Error).message);
    }
  };

  const formatPrice = (price: number) => formatVND(price);

  const formatDate = (date: Date): string => {
    return new Intl.DateTimeFormat('vi-VN', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    }).format(date);
  };

  // Check if description is long (more than 300 characters)
  const descriptionCharLimit = 300;
  const description = book.description || 'Đây là một cuốn sách tuyệt vời với nội dung phong phú và hấp dẫn. Cuốn sách mang đến cho độc giả những kiến thức bổ ích và trải nghiệm đọc thú vị.';
  const isDescriptionLong = description.length > descriptionCharLimit;
  const displayedDescription = isDescriptionExpanded || !isDescriptionLong 
    ? description 
    : description.slice(0, descriptionCharLimit) + '...';

  return (
    <div className="min-h-screen bg-background">
      {/* Sticky Header */}
      <Header 
        onSearch={onSearch}
        onCartClick={onCartClick}
        onLogoClick={onLogoClick}
        onLoginClick={onLoginClick}
        onAccountClick={onAccountClick}
      />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">


        {/* Book Details Grid */}
        <div className="grid lg:grid-cols-5 gap-8 mb-16">
          {/* Book Image */}
          <div className="lg:col-span-2">
            <div className="sticky top-24">
              <div className="aspect-[3/4] rounded-lg overflow-hidden shadow-lg">
                <ImageWithFallback
                  src={book.imageUrl}
                  alt={book.title}
                  className="w-full h-full object-cover transform-gpu backface-hidden"
                  style={{ imageRendering: '-webkit-optimize-contrast' as any, willChange: 'transform' }}
                />
              </div>
            </div>
          </div>

          {/* Book Information */}
          <div className="lg:col-span-3 space-y-6">
            {/* Title and Author */}
            <div>
              <h1 className="text-3xl font-bold text-foreground mb-2">{book.title}</h1>
              <p className="text-xl text-muted-foreground mb-4">Tác giả: {book.author}</p>
              
              {/* Rating */}
              <div className="flex items-center space-x-4 mb-4">
                <div className="flex items-center space-x-1">
                  {[...Array(5)].map((_, i) => (
                    <Star 
                      key={i} 
                      className={`h-5 w-5 ${i < Math.floor(book.avgRating) ? 'fill-yellow-400 text-yellow-400' : 'text-gray-300'}`} 
                    />
                  ))}
                  <span className="font-medium ml-2">{book.avgRating}</span>
                  <span className="text-muted-foreground">({book.ratingCount} đánh giá)</span>
                </div>
              </div>

              {/* Category and Year */}
              <div className="flex items-center space-x-4 mb-4">
                <Badge variant="secondary">{book.categories?.[0]?.categoryName || 'Khác'}</Badge>
                <span className="text-sm text-muted-foreground">Xuất bản: {book.publicationYear}</span>
              </div>
            </div>

            {/* Price and Stock */}
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <p className="text-3xl font-bold text-primary">{formatPrice(book.price || 0)}</p>
                    <p className={`text-sm ${(book.availableQuantity ?? book.stockQuantity) > 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {(book.availableQuantity ?? book.stockQuantity) > 0 ? 'Còn hàng' : 'Hết hàng'}
                    </p>
                  </div>
                  
                  {(book.availableQuantity ?? book.stockQuantity) > 0 && (
                    <div className="flex items-center space-x-2">
                      <Label htmlFor="quantity">Số lượng:</Label>
                      <Input
                        id="quantity"
                        type="number"
                        min="1"
                        max="10"
                        value={quantity}
                        onChange={(e) => setQuantity(parseInt(e.target.value) || 1)}
                        className="w-20"
                      />
                    </div>
                  )}
                </div>

                <Button 
                  onClick={handleAddToCart} 
                  className="w-full" 
                  size="lg"
                  disabled={(book.availableQuantity ?? book.stockQuantity) === 0}
                >
                  <ShoppingCart className="h-5 w-5 mr-2" />
                  {(book.availableQuantity ?? book.stockQuantity) === 0 ? 'Hết hàng' : 'Thêm vào giỏ hàng'}
                </Button>
              </CardContent>
            </Card>

            {/* Shipping Info */}
            <Card>
              <CardContent className="p-6">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="flex items-center space-x-3">
                    <Package className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Giao hàng miễn phí</p>
                      <p className="text-sm text-muted-foreground">Đơn từ 200k</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-3">
                    <Clock className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Giao nhanh 2-3 ngày</p>
                      <p className="text-sm text-muted-foreground">Trong nội thành</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-3">
                    <Shield className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Đổi trả 7 ngày</p>
                      <p className="text-sm text-muted-foreground">Miễn phí đổi trả</p>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Book Description */}
        <Card className="mb-16">
          <CardHeader>
            <CardTitle>Mô tả sản phẩm</CardTitle>
          </CardHeader>
          <CardContent className="prose max-w-none">
            <p className="text-muted-foreground leading-relaxed whitespace-pre-line">
              {displayedDescription}
            </p>
            {isDescriptionLong && (
              <Button 
                variant="link" 
                onClick={() => setIsDescriptionExpanded(!isDescriptionExpanded)}
                className="mt-4 p-0 h-auto font-medium text-primary"
              >
                {isDescriptionExpanded ? 'Thu gọn' : 'Xem thêm'}
              </Button>
            )}
          </CardContent>
        </Card>

        <ReviewsSection bookId={String(book.bookId)} className="mb-16" />

        {!isLoadingRecommendations && popularBooks.length > 0 && (
          <div className="mb-16">
            <BookRecommendations
              title="Sách được yêu thích"
              books={popularBooks}
              onBookClick={onBookClick}
            />
          </div>
        )}

        {!isLoadingRecommendations && similarBooks.length > 0 && (
          <div className="mb-16">
            <BookRecommendations
              title="Sách cùng thể loại & tác giả"
              books={similarBooks}
              onBookClick={onBookClick}
            />          
          </div>
        )}

        {/* Loading state */}
        {isLoadingRecommendations && (
          <div className="text-center py-8">
            <p className="text-muted-foreground">Đang tải gợi ý sách...</p>
          </div>
        )}
      </div>

      <Footer />
    </div>
  );
};
