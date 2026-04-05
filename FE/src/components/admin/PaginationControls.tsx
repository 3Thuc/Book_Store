import React from 'react';
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from '../ui/pagination';
import { Button } from '../ui/button';

interface Props {
  totalItems?: number;
  currentPage: number;
  totalPages: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange?: (size: number) => void;
  loading?: boolean;
  containerId?: string;
  pageSizeOptions?: number[];
}

export const PaginationControls: React.FC<Props> = ({
  totalItems = 0,
  currentPage,
  totalPages,
  pageSize,
  onPageChange,
  onPageSizeChange,
  loading = false,
  containerId,
  pageSizeOptions = [5, 10, 15, 20],
}) => {
  const disabledPrev = loading || currentPage <= 1 || totalPages <= 1;
  const disabledNext = loading || currentPage >= totalPages || totalPages <= 1;

  const handleLinkClick = (page: number) => {
    if (loading || totalPages <= 1) return;
    onPageChange(page);
    if (containerId) {
      const el = document.getElementById(containerId);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      else window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  };

  return (
    <div className="mt-4">
      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">Tổng: {totalItems}</div>
        <div className="flex items-center gap-2">
          <Pagination>
            <PaginationContent>
              <PaginationItem>
                <PaginationPrevious
                  size="default"
                  onClick={() => !disabledPrev && onPageChange(currentPage - 1)}
                  disabled={disabledPrev}
                  className={disabledPrev ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                />
              </PaginationItem>

              {/* Hiển thị số trang có giới hạn */}
              {(() => {
                const maxVisiblePages = 5; // Hiển thị tối đa 5 trang
                const pages = [];
                let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
                let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
                
                // Điều chỉnh lại startPage nếu endPage gần cuối
                if (endPage - startPage + 1 < maxVisiblePages) {
                  startPage = Math.max(1, endPage - maxVisiblePages + 1);
                }

                // Thêm trang đầu và dấu ...
                if (startPage > 1) {
                  pages.push(1);
                  if (startPage > 2) {
                    pages.push('...');
                  }
                }

                // Thêm các trang ở giữa
                for (let i = startPage; i <= endPage; i++) {
                  pages.push(i);
                }

                // Thêm dấu ... và trang cuối
                if (endPage < totalPages) {
                  if (endPage < totalPages - 1) {
                    pages.push('...');
                  }
                  pages.push(totalPages);
                }

                return pages.map((page, index) => (
                  <PaginationItem key={index}>
                    {page === '...' ? (
                      <span className="px-3 py-2 text-sm text-muted-foreground">...</span>
                    ) : (
                      <PaginationLink
                        size="default"
                        onClick={() => handleLinkClick(page as number)}
                        isActive={currentPage === page}
                        disabled={loading || totalPages <= 1}
                        className={loading || totalPages <= 1 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                      >
                        {page}
                      </PaginationLink>
                    )}
                  </PaginationItem>
                ));
              })()}

              <PaginationItem>
                <PaginationNext
                  size="default"
                  onClick={() => !disabledNext && onPageChange(currentPage + 1)}
                  disabled={disabledNext}
                  className={disabledNext ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                />
              </PaginationItem>
            </PaginationContent>
          </Pagination>

          <div className="flex items-center gap-2">
            <label className="text-sm text-muted-foreground whitespace-nowrap">Hiển thị</label>
            <select
              value={pageSize}
              onChange={(e) => onPageSizeChange && onPageSizeChange(Number(e.target.value))}
              className="border rounded px-2 py-1 text-sm bg-background text-foreground border-input dark:bg-background dark:text-foreground"
            >
              {pageSizeOptions.map(opt => (
                <option key={opt} value={opt} className="bg-background text-foreground dark:bg-background dark:text-foreground">{opt}</option>
              ))}
            </select>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PaginationControls;
