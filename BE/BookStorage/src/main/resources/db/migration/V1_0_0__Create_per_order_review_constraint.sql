-- Drop old unique constraints on ratings table
ALTER TABLE ratings DROP INDEX IF EXISTS user_id;
ALTER TABLE ratings DROP INDEX IF EXISTS uk_rating_user_book;
ALTER TABLE ratings DROP KEY IF EXISTS user_id;

-- Create new per-order unique constraint
ALTER TABLE ratings ADD UNIQUE INDEX idx_rating_user_book_order (user_id, book_id, order_id);
