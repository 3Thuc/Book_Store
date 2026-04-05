import React, { useState, useMemo, useEffect } from 'react';
import { Star, Award, Filter, ChevronDown } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { Badge } from '../../components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Progress } from '../../components/ui/progress';
import { Separator } from '../../components/ui/separator';
import { ReviewService } from '../../services/reviewService';
import { Review } from '../../types/book';

interface ReviewsSectionProps {
  bookId: string;
  className?: string;
}

export const ReviewsSection: React.FC<ReviewsSectionProps> = ({ bookId, className = '' }) => {
  const [sortBy, setSortBy] = useState<string>('newest'); // newest, oldest, highest, lowest, helpful
  const [filterRating, setFilterRating] = useState<string>('all'); // all, 5, 4, 3, 2, 1
  const [showAllReviews, setShowAllReviews] = useState<boolean>(false);
  const [bookReviews, setBookReviews] = useState<Review[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  // Fetch reviews from API
  useEffect(() => {
    const fetchReviews = async () => {
      if (!bookId) return;
      
      try {
        setLoading(true);
        const response = await ReviewService.getReviewsByBook(bookId);
        if (response.code === 1000) {
          const result = (response as any).result;
          const reviews = Array.isArray(result) ? result : result?.reviews || [];
          const mappedReviews = reviews.map((review: any) => ({
            id: review.ratingId || review.id || `review-${review.ratingId}`,
            ratingId: review.ratingId,
            bookId: String(review.bookId || review.book?.bookId || bookId),
            userId: review.userId || review.user?.userId,
            userName: review.userName || review.user?.fullName || review.user?.username || 'Người dùng ẩn danh',
            rating: review.rating || 0,
            comment: review.review || review.comment || review.content,
            date: review.createdAt || review.date,
            createdAt: review.createdAt,
            updatedAt: review.updatedAt,
            helpful: review.helpful || review.helpfulCount || 0,
            helpfulCount: review.helpful || review.helpfulCount || 0,
            isVerified: review.isVerified || review.isVerifiedPurchase || false,
            isVerifiedPurchase: review.isVerified || review.isVerifiedPurchase || false,
            status: review.status
          }));
          setBookReviews(mappedReviews);
        } else {
          console.error('Failed to fetch reviews:', response);
          setBookReviews([]);
        }
      } catch (error) {
        console.error('Error fetching reviews:', error);
        setBookReviews([]);
      } finally {
        setLoading(false);
      }
    };

    fetchReviews();
  }, [bookId]);

  const averageRating = useMemo(() => {
    if (bookReviews.length === 0) return 0;
    const sum = bookReviews.reduce((acc, review) => acc + review.rating, 0);
    return Math.round((sum / bookReviews.length) * 10) / 10;
  }, [bookReviews]);

  const ratingDistribution = useMemo(() => {
    const distribution: Record<number, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    bookReviews.forEach(review => {
      if (review.rating >= 1 && review.rating <= 5) {
        distribution[review.rating]++;
      }
    });
    return distribution;
  }, [bookReviews]);

  // Filter and sort reviews
  const { filteredAndSortedReviews, displayReviews } = useMemo(() => {
    let filtered = [...bookReviews];

    // Filter by rating
    if (filterRating !== 'all') {
      const targetRating = parseInt(filterRating);
      filtered = filtered.filter(review => review.rating === targetRating);
    }

    // Sort reviews
    filtered.sort((a, b) => {
      switch (sortBy) {
        case 'oldest':
          return new Date(a.date || '').getTime() - new Date(b.date || '').getTime();
        case 'highest':
          return b.rating - a.rating;
        case 'lowest':
          return a.rating - b.rating;
        case 'newest':
        default:
          return new Date(b.date || '').getTime() - new Date(a.date || '').getTime();
      }
    });

    // Limit display if not showing all
    const displayReviews = showAllReviews ? filtered : filtered.slice(0, 3);

    return { filteredAndSortedReviews: filtered, displayReviews };
  }, [bookReviews, sortBy, filterRating, showAllReviews]);

  const formatDate = (dateString: string): string => {
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('vi-VN', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    }).format(date);
  };
  const getRatingPercentage = (rating: number): number => {
    const total = bookReviews.length;
    if (total === 0) return 0;
    return Math.round((ratingDistribution[rating] / total) * 100);
  };

  if (loading) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle>Đánh giá từ khách hàng</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8">
            <p className="text-muted-foreground">Đang tải đánh giá...</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (bookReviews.length === 0) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle>Đánh giá từ khách hàng</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8">
            <div className="flex justify-center mb-4">
              {[...Array(5)].map((_, i) => (
                <Star key={i} className="h-8 w-8 text-gray-300" />
              ))}
            </div>
            <p className="text-muted-foreground">Chưa có đánh giá nào cho cuốn sách này</p>
            <p className="text-sm text-muted-foreground mt-2">
              Hãy là người đầu tiên đánh giá cuốn sách này!
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>Đánh giá từ khách hàng</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Rating Overview */}
        <div className="grid md:grid-cols-2 gap-6">
          {/* Average Rating */}
          <div className="text-center">
            <div className="text-4xl font-bold text-primary mb-2">{averageRating}</div>
            <div className="flex justify-center items-center mb-2">
              {[...Array(5)].map((_, i) => (
                <Star 
                  key={i} 
                  className={`h-5 w-5 ${i < Math.floor(averageRating) ? 'fill-yellow-400 text-yellow-400' : 'text-gray-300'}`} 
                />
              ))}
            </div>
            <p className="text-sm text-muted-foreground">
              Dựa trên {bookReviews.length} đánh giá
            </p>
          </div>

          {/* Rating Distribution */}
          <div className="space-y-2">
            {[5, 4, 3, 2, 1].map((rating) => (
              <div key={rating} className="flex items-center space-x-3 text-sm">
                <span className="w-6">{rating}</span>
                <Star className="h-4 w-4 fill-yellow-400 text-yellow-400" />
                <div className="flex-1">
                  <Progress 
                    value={getRatingPercentage(rating)} 
                    className="h-2"
                  />
                </div>
                <span className="w-12 text-right text-muted-foreground">
                  {getRatingPercentage(rating)}%
                </span>
                <span className="w-8 text-right text-muted-foreground">
                  ({ratingDistribution[rating]})
                </span>
              </div>
            ))}
          </div>
        </div>

        <Separator />

        {/* Filters and Sort */}
        <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <Filter className="h-4 w-4" />
              <Select value={filterRating} onValueChange={setFilterRating}>
                <SelectTrigger className="w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tất cả đánh giá</SelectItem>
                  <SelectItem value="5">5 sao ({ratingDistribution[5]})</SelectItem>
                  <SelectItem value="4">4 sao ({ratingDistribution[4]})</SelectItem>
                  <SelectItem value="3">3 sao ({ratingDistribution[3]})</SelectItem>
                  <SelectItem value="2">2 sao ({ratingDistribution[2]})</SelectItem>
                  <SelectItem value="1">1 sao ({ratingDistribution[1]})</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <Select value={sortBy} onValueChange={setSortBy}>
            <SelectTrigger className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="newest">Mới nhất</SelectItem>
              <SelectItem value="oldest">Cũ nhất</SelectItem>
              <SelectItem value="highest">Đánh giá cao nhất</SelectItem>
              <SelectItem value="lowest">Đánh giá thấp nhất</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Reviews List */}
        <div className="space-y-6">
          {filteredAndSortedReviews.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-muted-foreground">
                Không có đánh giá nào phù hợp với bộ lọc
              </p>
            </div>
          ) : (
            <>
              {displayReviews.map((review) => (
                <div key={review.ratingId || review.id} className="border-b pb-6 last:border-b-0">
                  <div className="flex items-start justify-between mb-3">
                    <div className="space-y-1">
                      <div className="flex items-center space-x-2">
                        <span className="font-medium">{review.userName}</span>
                        {(review.isVerified || review.isVerifiedPurchase) && (
                          <Badge variant="outline" className="text-xs">
                            <Award className="h-3 w-3 mr-1" />
                            Đã mua hàng
                          </Badge>
                        )}
                      </div>
                      
                      <div className="flex items-center space-x-3">
                        <div className="flex">
                          {[...Array(5)].map((_, i) => (
                            <Star 
                              key={i} 
                              className={`h-4 w-4 ${i < review.rating ? 'fill-yellow-400 text-yellow-400' : 'text-gray-300'}`} 
                            />
                          ))}
                        </div>
                        <span className="text-sm text-muted-foreground">
                          {formatDate(review.date || '')}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-3">
                    <p className="text-foreground leading-relaxed">{review.comment || review.content}</p>
                    
                    {/* Helpful button - removed */}
                    <div className="flex items-center space-x-2">
                      {/* Removed helpful button */}
                    </div>
                  </div>
                </div>
              ))}

              {/* Show More/Less Button */}
              {filteredAndSortedReviews.length > 3 && (
                <div className="text-center pt-4">
                  <Button 
                    variant="outline" 
                    onClick={() => setShowAllReviews(!showAllReviews)}
                    className="min-w-32"
                  >
                    {showAllReviews ? (
                      <>Ẩn bớt</>
                    ) : (
                      <>
                        Xem thêm {filteredAndSortedReviews.length - 3} đánh giá
                        <ChevronDown className="h-4 w-4 ml-1" />
                      </>
                    )}
                  </Button>
                </div>
              )}
            </>
          )}
        </div>
      </CardContent>
    </Card>
  );
};
