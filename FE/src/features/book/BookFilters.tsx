import React, { useEffect, useState } from 'react';
import { Button } from '../../components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Badge } from '../../components/ui/badge';
import { BookService } from '../../services';
import { Category } from '../../types/book';

interface BookFiltersProps {
  selectedCategory: string;
  onCategoryChange: (category: string) => void;
  sortBy: string;
  onSortChange: (sort: string) => void;
  searchQuery: string;
  onClearSearch: () => void;
}

export const BookFilters: React.FC<BookFiltersProps> = ({
  selectedCategory,
  onCategoryChange,
  sortBy,
  onSortChange,
  searchQuery,
  onClearSearch,
}) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [categories, setCategories] = useState<Array<{ id: string; name: string }>>([]);

  useEffect(() => {
    let mounted = true;
    const fetchCategories = async () => {
      setLoading(true);
      try {
        const res = await BookService.getCategories();
        const raw = Array.isArray(res?.result) ? res.result : [];

        const mapped = raw
          .map((c: any) => {
            if (typeof c === 'string') return { id: String(c), name: c };
            return { id: String(c.categoryId ?? c.id ?? c), name: c.categoryName ?? c.name ?? String(c) };
          })
          .filter((c: any) => c && c.name)
          .sort((a: any, b: any) => a.name.localeCompare(b.name, 'vi'));

        // remove duplicates by id
        const unique = Array.from(new Map(mapped.map((m: any) => [m.id, m])).values());
        if (mounted) setCategories(unique);
      } catch (e) {
        if (mounted) setCategories([]);
      } finally {
        if (mounted) setLoading(false);
      }
    };
    fetchCategories();
    return () => { mounted = false; };
  }, []);

  return (
    <div className="space-y-4">
      {/* Search Results Info */}
      {searchQuery && (
        <div className="flex items-center space-x-2">
          <span className="text-sm text-muted-foreground">Kết quả tìm kiếm cho:</span>
          <Badge variant="secondary" className="text-sm">
            "{searchQuery}"
            <button
              type="button"
              onClick={onClearSearch}
              className="ml-2 hover:text-destructive"
            >
              ×
            </button>
          </Badge>
        </div>
      )}

      <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
        {/* Category Filter */}
        <div className="flex items-center space-x-3">
          <span className="text-sm text-muted-foreground whitespace-nowrap">Thể loại:</span>
          <Select value={selectedCategory} onValueChange={onCategoryChange} disabled={loading}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder={loading ? "Đang tải..." : "Chọn thể loại"} />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Tất cả thể loại</SelectItem>
              {categories.map((category) => (
                <SelectItem key={category.id} value={category.id}>
                  {category.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Sort Options */}
        <div className="flex items-center space-x-3">
          <span className="text-sm text-muted-foreground whitespace-nowrap">Sắp xếp:</span>
          <Select value={sortBy} onValueChange={onSortChange}>
            <SelectTrigger className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="rating">Đánh giá cao</SelectItem>
              <SelectItem value="newest">Mới nhất</SelectItem>
              <SelectItem value="price-asc">Giá: Thấp → Cao</SelectItem>
              <SelectItem value="price-desc">Giá: Cao → Thấp</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  );
};
