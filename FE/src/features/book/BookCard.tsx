import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Star, ShoppingCart, Heart } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { Badge } from '../../components/ui/badge';
import { Card, CardContent } from '../../components/ui/card';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { useCart } from '../../context/CartContext';
import { useAuth } from '../../context/AuthContext';
import { toast } from 'sonner';
import { Book } from '../../types/book';
import { formatVND } from '../../lib/formatters';

interface BookCardProps {
  book: Book;
  onClick?: (book: Book) => void;
  variant?: 'grid' | 'list';
}

export const BookCard: React.FC<BookCardProps> = ({ book, onClick, variant = 'grid' }) => {
  const navigate = useNavigate();
  const { addToCart } = useCart();
  const { isLoggedIn } = useAuth();

  const handleAddToCart = (e: React.MouseEvent) => {
    e.stopPropagation();
    
    // Check if user is logged in
    if (!isLoggedIn) {
      toast.error('Bạn cần đăng nhập để thêm sản phẩm vào giỏ hàng');
      navigate('/login');
      return;
    }
    
    addToCart(book, 1);
  };

  const formatPrice = (price: number) => formatVND(price);

  if (variant === 'list') {
    return (
      <Card 
        className="group cursor-pointer transition-all duration-300 hover:shadow-lg"
        onClick={() => onClick?.(book)}
      >
        <CardContent className="p-4">
          <div className="flex gap-4">
            {/* Book Image */}
            <div className="relative w-24 h-32 flex-shrink-0 overflow-hidden rounded-lg">
              <ImageWithFallback
                src={book.imageUrl}
                alt={book.title}
                className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105 transform-gpu backface-hidden"
                style={{ imageRendering: '-webkit-optimize-contrast' as any, willChange: 'transform' }}
              />
              
              {/* Stock Badge */}
              {((book.availableQuantity ?? book.stockQuantity) === 0) && (
                <Badge variant="destructive" className="absolute top-1 left-1 text-xs">
                  Hết hàng
                </Badge>
              )}
            </div>

            {/* Book Info */}
            <div className="flex-1 flex flex-col justify-between">
              <div>
                <h3 className="font-semibold text-base leading-tight mb-1 line-clamp-2">
                  {book.title}
                </h3>
                
                <p className="text-sm text-muted-foreground mb-2">
                  {book.author}
                </p>

                <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
                  {book.description}
                </p>

                {/* Rating */}
                <div className="flex items-center space-x-1 mb-2">
                  <Star className="h-4 w-4 fill-yellow-400 text-yellow-400" />
                  <span className="text-sm font-medium">{book.avgRating}</span>
                  <span className="text-sm text-muted-foreground">({book.ratingCount} đánh giá)</span>
                  <Badge variant="outline" className="ml-2 text-xs">
                    {book.categories?.[0]?.categoryName || 'Khác'}
                  </Badge>
                </div>
              </div>

              {/* Price and Actions */}
              <div className="flex items-center justify-between">
                <span className="font-bold text-lg text-primary">
                  {formatPrice(book.price)}
                </span>
                
                {book.stockQuantity > 0 && (
                  <Button
                    size="sm"
                    onClick={handleAddToCart}
                    className="ml-4"
                  >
                    <ShoppingCart className="h-4 w-4 mr-2" />
                    Thêm vào giỏ
                  </Button>
                )}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card 
      className="group cursor-pointer transition-all duration-300 hover:shadow-lg hover:-translate-y-1 h-full"
      onClick={() => onClick?.(book)}
    >
      <CardContent className="p-0 h-full flex flex-col">
        {/* Book Image */}
        <div className="relative aspect-[3/4] overflow-hidden rounded-t-lg">
          <ImageWithFallback
            src={book.imageUrl}
            alt={book.title}
            className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105 transform-gpu backface-hidden"
            style={{ imageRendering: '-webkit-optimize-contrast' as any, willChange: 'transform' }}
          />
          
          {/* Overlay Buttons */}
          <div className="absolute inset-0 bg-black/20 opacity-0 group-hover:opacity-100 transition-opacity duration-300 flex items-center justify-center">
            <Button
              size="sm"
              onClick={handleAddToCart}
              className="bg-white dark:bg-white text-black dark:text-black hover:bg-gray-100 dark:hover:bg-gray-200"
            >
              <ShoppingCart className="h-4 w-4 mr-1" />
              Thêm vào giỏ
            </Button>
          </div>

          {/* Stock Badge */}
          {((book.availableQuantity ?? book.stockQuantity) === 0) && (
            <Badge variant="destructive" className="absolute top-2 left-2">
              Hết hàng
            </Badge>
          )}
        </div>

        {/* Book Info */}
        <div className="p-4 flex-1 flex flex-col">
          <h3 className="font-semibold text-sm leading-tight mb-2 line-clamp-2 flex-1">
            {book.title}
          </h3>
          
          <p className="text-xs text-muted-foreground mb-2 line-clamp-1">
            {book.author}
          </p>

          {/* Rating */}
          <div className="flex items-center space-x-1 mb-2">
            <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" />
            <span className="text-xs font-medium">{book.avgRating}</span>
            <span className="text-xs text-muted-foreground">({book.ratingCount})</span>
          </div>

          {/* Price */}
          <div className="flex items-center justify-between">
            <div className="flex flex-col">
              <span className="font-bold text-sm text-primary">
                {formatPrice(book.price)}
              </span>
            </div>
            
            {book.stockQuantity > 0 && (
              <Button
                size="sm"
                variant="ghost"
                onClick={handleAddToCart}
                className="h-8 w-8 p-0 hover:!bg-black hover:!text-white dark:hover:!bg-white dark:hover:!text-black transition-colors"
              >
                <ShoppingCart className="h-4 w-4" />
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
