export interface Publisher {
  publisherId: number;
  publisherName: string;
}

export interface Category {
  categoryId: number;
  categoryName: string;
}

// Align Book type with backend payload shape
export interface Book {
  bookId: number;
  title: string;
  author?: string;
  authorName?: string; // Backend uses authorName
  publisher?: Publisher;
  price?: number;
  stockQuantity: number; // Total stock in warehouse
  availableQuantity?: number; // Available for sale (stockQuantity - reserved orders)
  description?: string;
  publicationYear?: number;
  avgRating: number;
  ratingCount: number;
  language?: string;
  format?: "ebook" | "paperback" | "hardcover"; 
  createdAt?: string; 
  updatedAt?: string;
  categories: Category[];
  imageUrl?: string;
  status?: 'active' | 'deleted';
}

export interface Review {
  id?: string;
  ratingId?: number; // Backend primary key
  bookId: string;
  userId?: string;
  user?: {
    userId: number;
    username: string;
    fullName?: string;
  };
  userName?: string;
  rating: number;
  comment?: string;
  review?: string; // Backend field name
  title?: string;
  content?: string; // Alternative field name
  createdAt?: string;
  updatedAt?: string;
  date?: string; // For backward compatibility
  helpful?: number;
  helpfulCount?: number;
  unhelpful?: number;
  isVerified?: boolean;
  isVerifiedPurchase?: boolean;
  status?: 'pending' | 'approved' | 'rejected'; // Backend enum
}
