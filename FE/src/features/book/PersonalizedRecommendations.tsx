import React, { useMemo, useState, useRef } from 'react';
import { User, Heart, ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { BookCard } from './BookCard';
import { Book } from '../../types/book';

interface PersonalizedRecommendationsProps {
  books: Book[];
  onBookClick?: (book: Book) => void;
  forYouBooks?: Book[];
}

export const PersonalizedRecommendations: React.FC<PersonalizedRecommendationsProps> = ({ 
  books, 
  onBookClick,
  forYouBooks
}) => {
  const [currentIndex, setCurrentIndex] = useState<number>(0);
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  // Generate personalized recommendations for "Dành riêng cho bạn"
  const forYou = useMemo(() => {
    // Use API data if available, otherwise use mock data
    return forYouBooks || books
      .filter(book => book.stockQuantity)
      .sort((a, b) => b.ratingCount - a.ratingCount)
      .slice(0, 10);
  }, [books, forYouBooks]);

  const itemsPerView = 5; // Show 5 books at once
  const maxIndex = Math.max(0, forYou.length - itemsPerView);

  const handlePrevious = () => {
    const newIndex = Math.max(0, currentIndex - 1);
    setCurrentIndex(newIndex);
    updateScrollPosition(newIndex);
  };

  const handleNext = () => {
    const newIndex = Math.min(maxIndex, currentIndex + 1);
    setCurrentIndex(newIndex);
    updateScrollPosition(newIndex);
  };

  const updateScrollPosition = (index: number) => {
    if (scrollContainerRef.current) {
      const container = scrollContainerRef.current;
      const itemWidth = container.scrollWidth / forYou.length;
      const scrollPosition = index * itemWidth;
      container.scrollTo({
        left: scrollPosition,
        behavior: 'smooth'
      });
    }
  };

  const handleBookClick = (book: Book) => {
    onBookClick?.(book);
  };

  if (forYou.length === 0) {
    return null;
  }

  return (
    <section className="py-16 bg-secondary/20">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <div className="flex items-center justify-center mb-4">
            <Heart className="h-6 w-6 text-primary mr-2" />
            <h2 className="text-3xl font-bold text-foreground">Dành riêng cho bạn</h2>
          </div>
          <p className="text-muted-foreground max-w-2xl mx-auto">
            Những cuốn sách được chọn lọc kỹ càng dựa trên sở thích đọc của bạn
          </p>
        </div>

        <div className="relative">
          {/* Navigation Buttons */}
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

          {/* Books Carousel */}
          <div className="overflow-hidden">
            <div
              ref={scrollContainerRef}
              className="flex gap-4 overflow-x-hidden"
            >
              {forYou.map((book, index) => (
                <div key={`personalized-${book.bookId}-${index}`} className="flex-none" style={{ width: 'calc(20% - 12.8px)' }}>
                  <BookCard
                    book={book}
                    onClick={() => handleBookClick(book)}
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