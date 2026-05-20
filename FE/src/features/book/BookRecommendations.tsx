import React, { useState, useRef } from 'react';
import { ChevronLeft, ChevronRight, Star, TrendingUp } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { BookCard } from './BookCard';
import { Book } from '../../types/book';

interface BookRecommendationsProps {
  title: string;
  subtitle?: string;
  books: Book[];
  onBookClick?: (book: Book) => void;
  className?: string;
  /** Icon hiển thị bên cạnh tiêu đề */
  icon?: 'star' | 'trending' | 'none';
  /** Màu nền section – dùng để xen kẽ với PersonalizedRecommendations */
  bgVariant?: 'default' | 'muted';
}

export const BookRecommendations: React.FC<BookRecommendationsProps> = ({
  title,
  subtitle,
  books,
  onBookClick,
  className = '',
  icon = 'none',
  bgVariant = 'default',
}) => {
  const [currentIndex, setCurrentIndex] = useState<number>(0);
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  if (!books || books.length === 0) return null;

  const ITEMS_PER_VIEW = 5;
  const maxIndex = Math.max(0, books.length - ITEMS_PER_VIEW);

  const updateScrollPosition = (index: number) => {
    if (!scrollContainerRef.current) return;
    const container = scrollContainerRef.current;
    const itemWidth = container.scrollWidth / books.length;
    container.scrollTo({ left: index * itemWidth, behavior: 'smooth' });
  };

  const handlePrevious = () => {
    const next = Math.max(0, currentIndex - 1);
    setCurrentIndex(next);
    updateScrollPosition(next);
  };

  const handleNext = () => {
    const next = Math.min(maxIndex, currentIndex + 1);
    setCurrentIndex(next);
    updateScrollPosition(next);
  };

  const bgClass = bgVariant === 'muted'
    ? 'bg-secondary/20'
    : 'bg-background';

  const IconComponent = icon === 'star'
    ? Star
    : icon === 'trending'
    ? TrendingUp
    : null;

  return (
    <section className={`py-16 ${bgClass} ${className}`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header – căn giữa giống PersonalizedRecommendations */}
        <div className="text-center mb-12">
          <div className="flex items-center justify-center mb-4">
            {IconComponent && (
              <IconComponent className="h-6 w-6 text-primary mr-2" />
            )}
            <h2 className="text-3xl font-bold text-foreground">{title}</h2>
          </div>
          {subtitle && (
            <p className="text-muted-foreground max-w-2xl mx-auto">
              {subtitle}
            </p>
          )}
        </div>

        {/* Carousel – cùng cấu trúc với PersonalizedRecommendations */}
        <div className="relative">
          {/* Nút Previous */}
          <div className="absolute top-1/2 -translate-y-1/2 -left-4 z-10">
            <Button
              variant="outline"
              size="icon"
              className="rounded-full shadow-lg bg-background"
              onClick={handlePrevious}
              disabled={currentIndex === 0}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
          </div>

          {/* Nút Next */}
          <div className="absolute top-1/2 -translate-y-1/2 -right-4 z-10">
            <Button
              variant="outline"
              size="icon"
              className="rounded-full shadow-lg bg-background"
              onClick={handleNext}
              disabled={currentIndex >= maxIndex}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>

          {/* Books Carousel – overflow hidden + scroll điều khiển bằng JS */}
          <div className="overflow-hidden">
            <div
              ref={scrollContainerRef}
              className="flex gap-4 overflow-x-hidden"
            >
              {books.map((book, index) => (
                <div
                  key={`rec-${(book as any).bookId ?? index}-${index}`}
                  className="flex-none"
                  style={{ width: 'calc(20% - 12.8px)' }}
                >
                  <BookCard
                    book={book}
                    onClick={() => onBookClick?.(book)}
                  />
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};
