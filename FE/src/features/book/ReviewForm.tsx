import React, { useState } from 'react';
import { Star, Check, X, AlertCircle } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Badge } from '../../components/ui/badge';
import { Alert, AlertDescription } from '../../components/ui/alert';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { useOrder } from '../../context/OrderContext';
import { useAuth } from '../../context/AuthContext';
import { toast } from 'sonner';
import { formatVND } from '../../lib/formatters';
import { OrderItem } from '../../types/order';

interface ReviewFormProps {
  item: OrderItem;
  orderId: string;
  isOpen: boolean;
  onClose: () => void;
  editingReviewId?: string;
}

export const ReviewForm: React.FC<ReviewFormProps> = ({ 
  item, 
  orderId, 
  isOpen, 
  onClose,
  editingReviewId
}) => {
  const { submitReview, updateReview, reviews } = useOrder();
  const { user, refreshOrders } = useAuth();
  
  const [rating, setRating] = useState<number>(0);
  const [hoveredRating, setHoveredRating] = useState<number>(0);
  const [review, setReview] = useState<string>('');
  const [errors, setErrors] = useState<{
    rating?: string;
    review?: string;
  }>({});
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

  // Load existing review when editing
  React.useEffect(() => {
    if (editingReviewId && isOpen) {
      const existingReview = reviews.find(r => r.rating_id === editingReviewId);
      if (existingReview) {
        setRating(existingReview.rating);
        setReview(existingReview.review);
      }
    } else if (isOpen && !editingReviewId) {
      // Reset form for new review
      setRating(0);
      setHoveredRating(0);
      setReview('');
      setErrors({});
    }
  }, [editingReviewId, isOpen, reviews]);

  const validateForm = (): boolean => {
    const newErrors: typeof errors = {};

    if (rating === 0) {
      newErrors.rating = 'Vui lòng chọn số sao đánh giá';
    }

    if (!review.trim()) {
      newErrors.review = 'Vui lòng nhập đánh giá';
    } else if (review.trim().length > 255) {
      newErrors.review = 'Đánh giá không được vượt quá 255 ký tự';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async () => {
    if (!validateForm()) {
      return;
    }

    setIsSubmitting(true);

    try {
      if (editingReviewId) {
        // Update existing review
        console.log('=== Starting review update ===');
        console.log('Review ID:', editingReviewId);
        console.log('Book ID:', item.bookId);
        console.log('Rating:', rating);
        console.log('Review:', review);

        await updateReview(editingReviewId, {
          book_id: item.bookId,
          rating,
          review: review.trim(),
        });
      } else {
        // Create new review
        console.log('=== Starting review submission ===');
        console.log('Order ID:', orderId);
        console.log('Book ID:', item.bookId);
        console.log('Rating:', rating);
        console.log('Review:', review);
        console.log('User:', user);

        await submitReview(orderId, {
          book_id: item.bookId,
          rating,
          review: review.trim(),
        });

        // Refresh orders so isReviewed state updates without a full page reload
        await refreshOrders();

        toast.success('Đánh giá thành công!', {
          description: 'Cảm ơn bạn đã chia sẻ trải nghiệm của mình.',
        });
      }

      // Reset form
      setRating(0);
      setHoveredRating(0);
      setReview('');
      setErrors({});
      onClose();
    } catch (error: any) {
      console.error('=== Review submission error ===');
      console.error('Error object:', error);
      console.error('Error message:', error?.message);
      console.error('Error response:', error?.response);
      console.error('Error response data:', error?.response?.data);
      
      toast.error('Có lỗi xảy ra', {
        description: error?.response?.data?.message || error?.message || 'Không thể gửi đánh giá. Vui lòng thử lại sau.',
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleRatingClick = (value: number) => {
    setRating(value);
    setErrors({ ...errors, rating: undefined });
  };

  const getRatingLabel = (rating: number): string => {
    switch (rating) {
      case 1: return 'Rất tệ';
      case 2: return 'Tệ';
      case 3: return 'Bình thường';
      case 4: return 'Tốt';
      case 5: return 'Tuyệt vời';
      default: return '';
    }
  };

  const formatPrice = (price: number) => formatVND(price);

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[600px] max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{editingReviewId ? 'Chỉnh sửa đánh giá' : 'Đánh giá sản phẩm'}</DialogTitle>
          <DialogDescription>
            {editingReviewId ? 'Cập nhật đánh giá của bạn' : 'Chia sẻ trải nghiệm của bạn để giúp người khác có quyết định tốt hơn'}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-4">
          {/* Book Info */}
          <div className="flex gap-4 p-4 bg-muted rounded-lg">
            <div className="relative w-20 h-28 flex-shrink-0 overflow-hidden rounded-md bg-background">
              <ImageWithFallback
                src={item.imageUrl}
                alt={item.title}
                className="w-full h-full object-cover"
              />
            </div>
            <div className="flex-1 min-w-0">
              <h4 className="font-medium line-clamp-2 mb-1">{item.title}</h4>
              <p className="text-sm text-muted-foreground mb-2">{item.author}</p>
              <div className="flex items-center gap-2">
                <span className="font-semibold text-primary">
                  {formatPrice(item.price)}
                </span>
                {item.quantity > 1 && (
                  <Badge variant="secondary">x{item.quantity}</Badge>
                )}
              </div>
            </div>
          </div>

          {/* Rating Stars */}
          <div className="space-y-2">
            <Label className="flex items-center gap-2">
              Đánh giá của bạn <span className="text-destructive">*</span>
            </Label>
            <div className="flex items-center gap-4">
              <div className="flex gap-1">
                {[1, 2, 3, 4, 5].map((value) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => handleRatingClick(value)}
                    onMouseEnter={() => setHoveredRating(value)}
                    onMouseLeave={() => setHoveredRating(0)}
                    className="focus:outline-none transition-transform hover:scale-110"
                  >
                    <Star
                      className={`h-8 w-8 transition-colors ${
                        value <= (hoveredRating || rating)
                          ? 'fill-yellow-400 text-yellow-400'
                          : 'text-gray-300'
                      }`}
                    />
                  </button>
                ))}
              </div>
              {(rating > 0 || hoveredRating > 0) && (
                <span className="text-sm font-medium text-muted-foreground">
                  {getRatingLabel(hoveredRating || rating)}
                </span>
              )}
            </div>
            {errors.rating && (
              <p className="text-sm text-destructive flex items-center gap-1">
                <AlertCircle className="h-3 w-3" />
                {errors.rating}
              </p>
            )}
          </div>

          {/* Review Text */}
          <div className="space-y-2">
            <Label htmlFor="review-text" className="flex items-center gap-2">
              Nhận xét của bạn <span className="text-destructive">*</span>
            </Label>
            <Textarea
              id="review-text"
              placeholder="Chia sẻ trải nghiệm của bạn về sản phẩm..."
              value={review}
              onChange={(e) => {
                setReview(e.target.value);
                setErrors({ ...errors, review: undefined });
              }}
              rows={4}
              maxLength={255}
              className={errors.review ? 'border-destructive' : ''}
            />
            <div className="flex items-center justify-between">
              {errors.review ? (
                <p className="text-sm text-destructive flex items-center gap-1">
                  <AlertCircle className="h-3 w-3" />
                  {errors.review}
                </p>
              ) : (
                <p className="text-sm text-muted-foreground">
                  Tối đa 255 ký tự
                </p>
              )}
              <span className="text-sm text-muted-foreground">
                {review.length}/255
              </span>
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button 
            variant="outline" 
            onClick={onClose}
            disabled={isSubmitting}
          >
            <X className="h-4 w-4 mr-2" />
            Hủy
          </Button>
          <Button 
            onClick={handleSubmit}
            disabled={isSubmitting}
          >
            {isSubmitting ? (
              <>
                <div className="h-4 w-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin" />
                Đang {editingReviewId ? 'cập nhật' : 'gửi'}...
              </>
            ) : (
              <>
                <Check className="h-4 w-4 mr-2" />
                {editingReviewId ? 'Cập nhật đánh giá' : 'Gửi đánh giá'}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
