import { Review } from '../types/book';

export const reviews: Review[] = [
]

// Helper function to get reviews for a specific book
export function getBookReviews(bookId: string): Review[] {
  return reviews.filter(review => review.bookId === bookId);
}

// Helper function to get average rating from reviews
export function getAverageRating(bookId: string): number {
  const bookReviews = getBookReviews(bookId);
  if (bookReviews.length === 0) return 0;
  
  const sum = bookReviews.reduce((acc, review) => acc + review.rating, 0);
  return Math.round((sum / bookReviews.length) * 10) / 10;
}

// Helper function to get review count
export function getReviewCount(bookId: string): number {
  return getBookReviews(bookId).length;
}

// Helper function to get reviews by rating (for statistics)
export function getReviewsByRating(bookId: string): Record<number, number> {
  const bookReviews = getBookReviews(bookId);
  const ratingCount: Record<number, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
  
  bookReviews.forEach(review => {
    ratingCount[review.rating]++;
  });
  
  return ratingCount;
}