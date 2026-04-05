import React from 'react';
import { BookCard } from './BookCard';
import { Book } from '../../types/book';

interface BookRecommendationsProps {
  title: string;
  books: Book[];
  onBookClick?: (book: Book) => void;
  className?: string;
}

export const BookRecommendations: React.FC<BookRecommendationsProps> = ({ 
  title, 
  books, 
  onBookClick,
  className = ""
}) => {
  if (!books || books.length === 0) {
    return null;
  }

  return (
    <section className={`py-8 ${className}`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h2 className="text-2xl font-bold text-foreground mb-4">
            {title}
          </h2>
        </div>

        {/* Books Grid */}
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
          {books.map((book) => (
            <BookCard
              key={String((book as any).bookId ?? (book as any).id ?? book.title)}
              book={book}
              onClick={() => onBookClick?.(book)}
            />
          ))}
        </div>
      </div>
    </section>
  );
};
