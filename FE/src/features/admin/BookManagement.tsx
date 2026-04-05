import React, { useState, useRef, useEffect } from 'react';
import { useAdmin } from './AdminContext';
import adminService from '../../services/adminService';
import { Book } from '../../types/book';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from '../../components/ui/pagination';
import PaginationControls from '../../components/admin/PaginationControls';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { BookOpen, Plus, Edit, Trash2, Search, ChevronsUpDown, Check } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '../../components/ui/popover';
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '../../components/ui/command';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';

export const BookManagement: React.FC = () => {
  const { books, addBook, updateBook, deleteBook, categories, publishers, authors } = useAdmin();
  const [searchTerm, setSearchTerm] = useState('');
  // store category id ('all' = all)
  const [filterCategory, setFilterCategory] = useState('all');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [categoriesOpen, setCategoriesOpen] = useState(false);
  const [authorOpen, setAuthorOpen] = useState(false);
  const [publisherOpen, setPublisherOpen] = useState(false);
  const [authorQuery, setAuthorQuery] = useState('');
  const [publisherQuery, setPublisherQuery] = useState('');
  const [categoryQuery, setCategoryQuery] = useState('');
  const [editingBook, setEditingBook] = useState<Book | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [bookToDelete, setBookToDelete] = useState<Book | null>(null);

  // Server-side pagination state
  const [pageBooks, setPageBooks] = useState<Book[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(5);
  const [totalPages, setTotalPages] = useState(1);
  const [totalItems, setTotalItems] = useState(0);
  const [loading, setLoading] = useState(false);

  // Helper to map server book shape to UI Book with status
  const mapServerBook = (b: any): Book => {
    const status = b.status === null || b.status === undefined
      ? 'active'
      : (typeof b.status === 'string'
        ? (String(b.status).toLowerCase() === 'active' ? 'active' : 'deleted')
        : (typeof b.status === 'boolean' ? (b.status ? 'active' : 'deleted') : 'active'));
    return {
      bookId: Number(b.bookId ?? b.id ?? 0),
      title: b.title ?? b.name ?? '',
      author: b.authorName ?? b.author ?? '', // Map authorName from API
      publisher: b.publisher ? { 
        publisherId: b.publisher.publisherId ?? b.publisher.id ?? 0, 
        publisherName: b.publisher.publisherName ?? b.publisher.name ?? '' 
      } : undefined,
      price: b.price ?? b.priceAmount ?? 0,
      stockQuantity: Number(b.stockQuantity ?? b.availableQuantity ?? b.stock ?? 0),
      description: b.description ?? b.summary ?? '',
      publicationYear: b.publicationYear ?? b.publishedYear ?? undefined,
      avgRating: b.avgRating ?? b.rating ?? 0,
      ratingCount: b.ratingCount ?? b.reviewCount ?? 0,
      categories: Array.isArray(b.categories) ? b.categories.map((c: any) => ({ 
        categoryId: c.categoryId ?? c.id ?? 0, 
        categoryName: c.categoryName ?? c.name ?? '' 
      })) : (b.category ? [{ categoryId: 0, categoryName: b.category }] : []),
      imageUrl: b.imageUrl ?? b.images ?? undefined, 
      images: b.imageUrl ?? b.images ?? undefined, 
      status: status as any,
    } as Book;
  };

  useEffect(() => {
    fetchPage(currentPage, pageSize, searchTerm, filterCategory);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentPage, pageSize, searchTerm, filterCategory]);

  const fetchPage = async (page = 1, limit = pageSize, search?: string, category?: string) => {
    setLoading(true);
    try {
      const params: any = { page, limit };
      if (search && search.trim()) params.search = search.trim();
      if (category && category !== 'all') {
        const asNum = Number(category);
        params.category = Number.isNaN(asNum) ? category : asNum;
      }
      const res = await adminService.getBooks(params);
      const pageData = res?.result ?? res ?? {};
      const items = (pageData as any)?.books ?? (pageData as any)?.items ?? [];
      
      // Remove duplicates based on bookId
      const uniqueItems = items.filter((book: any, index: number, self: any[]) => 
        self.findIndex((b: any) => b.bookId === book.bookId) === index
      );
      
      setPageBooks(uniqueItems.map((b: any) => mapServerBook(b)));

      const total = Number((pageData as any)?.totalElements ?? (pageData as any)?.total ?? (pageData as any)?.totalItems ?? (pageData as any)?.totalCount ?? 0) || 0;
      setTotalItems(total);
      const serverPages = Number((pageData as any)?.totalPages ?? Math.max(1, Math.ceil(total / limit)));
      setTotalPages(serverPages);
      const serverPage = Number((pageData as any)?.page ?? (pageData as any)?.pageNumber ?? page);
      setCurrentPage(serverPage);
    } catch (err) {
      console.error('Failed to load admin books page', err);
      setPageBooks([]);
      setTotalItems(0);
      setTotalPages(1);
    } finally {
      setLoading(false);
    }
  };

  const handleFilterCategoryChange = (val: string) => {
    try {
      console.debug('[BookManagement] category change ->', val);
    } catch (e) {}
    setFilterCategory(val);
    setCurrentPage(1);
  };

  // Filter books based on user role
  const filteredBooksByRole = books;

  // Form state
  const [formData, setFormData] = useState({
    title: '',
    author: '',
    selectedAuthorId: '',
    // allow multiple category selection by storing selected category ids
    selectedCategoryIds: [] as string[],
    selectedPublisherId: '',
    price: '',
    description: '',
    imageUrl: '',
    imageFile: null as File | null,
    publishedYear: new Date().getFullYear().toString(),
    language: 'vi',
    status: 'active' as 'active' | 'deleted',
  });

  const resetForm = () => {
    setFormData({
      title: '',
      author: '',
      selectedAuthorId: '',
      selectedCategoryIds: [],
      selectedPublisherId: '',
      price: '',
      description: '',
      imageUrl: '',
      imageFile: null,
      publishedYear: new Date().getFullYear().toString(),
      language: 'vi',
      status: 'active',
    });
    setEditingBook(null);
  };

  const handleOpenDialog = (book?: Book) => {
    // Reset search queries for clean state
    setAuthorQuery('');
    setPublisherQuery('');
    setCategoryQuery('');
    
    if (book) {
      setEditingBook(book);
      setFormData({
        title: book.title,
        author: book.author ?? '',
        selectedAuthorId: (() => {
          const found = authors.find(a => a.authorName === (book.author ?? ''));
          return found ? String(found.id) : '';
        })(),
        // populate selectedCategoryIds from book.categories if available
        selectedCategoryIds: (book as any).categories ? (book as any).categories.map((c: any) => String(c.categoryId ?? c.id ?? '')) : [],
        selectedPublisherId: (book as any).publisher ? (String((book as any).publisher.publisherId ?? (book as any).publisher.id ?? publishers.find(p => p.publisherName === ((book as any).publisher.publisherName ?? (book as any).publisher.name ?? (book as any).publisher))?.id ?? '')) : '',
        price: book.price != null ? book.price.toString() : '',
        description: book.description ?? '',
        imageUrl: book.imageUrl ?? '',
        imageFile: null,
        publishedYear: book.publicationYear != null ? book.publicationYear.toString() : new Date().getFullYear().toString(),
        language: (book as any).language || 'vi',
        status: (book as any).status === 'deleted' ? 'deleted' : 'active',
      });
    } else {
      resetForm();
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    const priceNum = parseFloat(formData.price || '0');
    const publishedYearNum = parseInt(formData.publishedYear || new Date().getFullYear().toString(), 10);

    const selectedCategoryIds = (formData as any).selectedCategoryIds as string[];
    // map selected ids to backend category payload
    const categoriesPayload = (selectedCategoryIds || []).map(id => {
      const found = categories.find(c => c.id === id);
      return { categoryId: Number(id) || 0, categoryName: found ? found.categoryName : '' };
    });

    const bookData = {
      title: formData.title,
      author: formData.author,
      selectedAuthorId: formData.selectedAuthorId,
      status: formData.status,
      // include selected publisher id (if any)
      publisherId: (formData as any).selectedPublisherId ? Number((formData as any).selectedPublisherId) : undefined,
      // keep a top-level category for compatibility (first selected)
      category: categoriesPayload[0]?.categoryName ?? '',
  categories: categoriesPayload.length ? categoriesPayload : [{ categoryId: 0, categoryName: '' }],
  price: priceNum,
  avgRating: 0,
  ratingCount: 0,
  description: formData.description,
  imageUrl: formData.imageUrl || 'https://images.unsplash.com/photo-1544716278-ca5e3f4abd8c?w=400&h=600&fit=crop',
  imageFile: formData.imageFile,
  publishedYear: publishedYearNum,
  language: formData.language,
  // default new books to out-of-stock (0). When editing, preserve existing stockQuantity if available.
  stockQuantity: editingBook ? (editingBook.stockQuantity ?? 0) : 0,
      bookId: Date.now(),
    };

    (async () => {
      if (editingBook) {
        await updateBook({ ...bookData, bookId: editingBook.bookId ?? bookData.bookId });
      } else {
        await addBook(bookData);
      }
      await fetchPage(currentPage, pageSize, searchTerm, filterCategory);
      setDialogOpen(false);
      resetForm();
    })();
  };

  const handleDelete = (book: Book) => {
    setBookToDelete(book);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (bookToDelete) {
      (async () => {
        await deleteBook(bookToDelete.bookId.toString());
        await fetchPage(currentPage, pageSize, searchTerm, filterCategory);
        setDeleteDialogOpen(false);
        setBookToDelete(null);
      })();
    }
  };

  // Server-driven list is in `pageBooks` (loaded from admin API). UI filters (search/category)
  // are forwarded to the server via fetchPage; `filteredBooksByRole` is kept only as fallback.

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
    }).format(amount);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý sách</CardTitle>
              <CardDescription>Thêm, sửa, xóa sách và cập nhật thông tin</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm sách mới
            </Button>
          </div>
          
        </CardHeader>
        <CardContent>
          {/* Filters */}
          <div className="flex flex-col md:flex-row gap-4 mb-6">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Tìm kiếm theo tên sách hoặc tác giả..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
            <Select value={filterCategory} onValueChange={handleFilterCategoryChange}>
                <SelectTrigger className="w-full md:w-[200px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tất cả</SelectItem>
                  {categories.filter(c => c.status).map(category => (
                    <SelectItem key={category.id} value={category.id}>
                      {category.categoryName}
                    </SelectItem>
                  ))}
                </SelectContent>
            </Select>
          </div>

          {/* Books Table */}
          <div id="admin-books" className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Sách</TableHead>
                  <TableHead>Tác giả</TableHead>
                  <TableHead>Danh mục</TableHead>
                  <TableHead>Giá</TableHead>
                  <TableHead>Đánh giá</TableHead>
                  <TableHead>Năm XB</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead className="text-right">Hành động</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading ? (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center text-muted-foreground py-8">Đang tải sách...</TableCell>
                  </TableRow>
                ) : pageBooks.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center text-muted-foreground py-8">Không tìm thấy sách</TableCell>
                  </TableRow>
                ) : (
                  pageBooks.map((book: Book) => (
                    <TableRow key={book.bookId}>
                      <TableCell>
                        <div className="flex items-center gap-3">
                          <div className="relative w-12 h-16 flex-shrink-0">
                            {book.imageUrl ? (
                              <>
                                {console.log(`Book ${book.bookId} imageUrl:`, book.imageUrl)}
                                <img
                                  src={book.imageUrl}
                                  alt={book.title}
                                  className="absolute inset-0 w-full h-full object-cover rounded border"
                                  onLoad={() => console.log(` Image loaded: Book ${book.bookId}`)}
                                  onError={(e) => {
                                    console.error(` Image failed: Book ${book.bookId}`, book.imageUrl);
                                    e.currentTarget.style.display = 'none';
                                    const fallback = e.currentTarget.nextElementSibling;
                                    if (fallback) (fallback as HTMLElement).style.display = 'flex';
                                  }}
                                />
                              </>
                            ) : (
                              <>
                                {console.log(`Book ${book.bookId} has NO imageUrl`)}
                                {null}
                              </>
                            )}
                            <div 
                              className="absolute inset-0 w-full h-full bg-gray-100 rounded border flex items-center justify-center"
                              style={{ display: book.imageUrl ? 'none' : 'flex' }}
                            >
                              <div className="text-xs text-gray-400 text-center px-1">
                                <div>Không có</div>
                                <div>ảnh</div>
                              </div>
                            </div>
                          </div>
                          <div className="max-w-[200px] min-w-0">
                            <div className="truncate font-medium" title={book.title}>{book.title}</div>
                            <div className="text-sm text-muted-foreground">ID: {book.bookId}</div>
                            {book.stockQuantity !== undefined && (
                              <div className="text-xs text-muted-foreground">Kho: {book.stockQuantity}</div>
                            )}
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>{book.author}</TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1 max-w-[150px]">
                          {(() => {
                            const categories = (book.categories || []).map((c: any) => c.categoryName ?? c.name ?? '').filter(Boolean);
                            const displayCategories = categories.slice(0, 2);
                            const remainingCount = categories.length - 2;
                            
                            return (
                              <>
                                {displayCategories.map((catName, index) => (
                                  <Badge key={index} variant="secondary" className="text-xs truncate max-w-[60px]" title={catName}>
                                    {catName}
                                  </Badge>
                                ))}
                                {remainingCount > 0 && (
                                  <Badge 
                                    variant="outline" 
                                    className="text-xs" 
                                    title={`Còn ${remainingCount} danh mục khác: ${categories.slice(2).join(', ')}`}
                                  >
                                    +{remainingCount}
                                  </Badge>
                                )}
                              </>
                            );
                          })()} 
                        </div>
                      </TableCell>
                      <TableCell>{formatCurrency(book.price || 0 )}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <span>⭐ {(book.avgRating || 0).toFixed(1)}</span>
                          <span className="text-sm text-muted-foreground">({book.ratingCount || 0})</span>
                        </div>
                      </TableCell>
                      <TableCell>{book.publicationYear}</TableCell>
                      <TableCell>
                        <div className="flex flex-col gap-1">
                          {book.status === 'deleted' ? (
                            <Badge variant="destructive">Đã xóa</Badge>
                          ) : (
                            <Badge variant="default">Hoạt động</Badge>
                          )}
                          {book.stockQuantity !== undefined && (
                            <Badge 
                              variant={book.stockQuantity > 0 ? 'secondary' : 'outline'} 
                              className="text-xs"
                            >
                              {book.stockQuantity > 0 ? `Còn: ${book.stockQuantity}` : 'Hết hàng'}
                            </Badge>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex gap-2 justify-end">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleOpenDialog(book)}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="destructive"
                            size="sm"
                            onClick={() => handleDelete(book)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>

          {/* Pagination Controls (shared) */}
          <div>
            <PaginationControls
              totalItems={totalItems}
              currentPage={currentPage}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={(p: number) => setCurrentPage(p)}
              onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
              loading={loading}
              containerId="admin-books"
              pageSizeOptions={[5, 10, 15, 20]}
            />
          </div>
        </CardContent>
      </Card>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-4xl w-[90vw] max-h-[90vh] overflow-hidden flex flex-col">
          <DialogHeader className="pb-4 flex-shrink-0">
            <DialogTitle className="text-xl font-semibold">{editingBook ? 'Chỉnh sửa sách' : 'Thêm sách mới'}</DialogTitle>
            <DialogDescription className="text-muted-foreground">
              {editingBook ? 'Cập nhật thông tin sách trong hệ thống' : 'Thêm thông tin sách mới vào hệ thống'}
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-6 py-4 overflow-y-auto flex-1 pr-2">
            {/* Basic Information Section */}
            <div className="space-y-4">
              <h3 className="text-lg font-medium border-b pb-2">Thông tin cơ bản</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="title" className="text-sm font-medium">Tên sách *</Label>
                  <Input
                    id="title"
                    value={formData.title}
                    onChange={(e) => setFormData({ ...formData, title: e.target.value })}
                    placeholder="Nhập tên sách"
                    className="h-10 max-w-full"
                    maxLength={200}
                  />
                  {formData.title.length > 100 && (
                    <p className="text-xs text-muted-foreground">{formData.title.length}/200 ký tự</p>
                  )}
                </div>
                
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Tác giả *</Label>
                  <div className="relative">
                    <Button
                      type="button"
                      variant="outline"
                      className="w-full justify-start h-10 font-normal"
                      onClick={() => setAuthorOpen(prev => !prev)}
                    >
                      {(() => {
                        const selected = authors.find(a => String(a.id) === String(formData.selectedAuthorId));
                        return selected?.authorName || 'Chọn tác giả';
                      })()}
                    </Button>

                    {authorOpen && (
                      <div key={formData.selectedAuthorId} className="absolute left-0 top-full mt-1 w-full max-w-sm max-h-64 bg-white rounded-md border shadow-lg z-50 overflow-hidden">
                        <div className="p-3 border-b">
                          <Input
                            placeholder="Tìm kiếm tác giả..."
                            value={authorQuery}
                            onChange={(e) => setAuthorQuery(e.target.value)}
                            className="h-9"
                          />
                        </div>
                        <div className="max-h-48 overflow-y-auto p-2">
                          {authors
                            .filter(a => a.authorName && a.authorName.toLowerCase().includes(authorQuery.toLowerCase()))
                            .slice(0, 15)
                            .map(author => {
                            const checked = String(formData.selectedAuthorId || '') === String(author.id || '');
                            return (
                              <label key={author.id} className="flex items-center gap-3 p-2 hover:bg-gray-50 rounded-md cursor-pointer">
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={() => {
                                    if (checked) {
                                      setFormData({ ...formData, selectedAuthorId: '', author: '' });
                                    } else {
                                      setFormData({ ...formData, selectedAuthorId: String(author.id), author: author.authorName });
                                    }
                                  }}
                                  className="rounded"
                                />
                                <span className="text-sm">{author.authorName}</span>
                              </label>
                            );
                          })}
                          {authors.filter(a => a.authorName && a.authorName.toLowerCase().includes(authorQuery.toLowerCase())).length > 15 && (
                            <div className="px-2 py-2 text-xs text-muted-foreground text-center border-t mt-2">
                              Hiển thị 15/{authors.filter(a => a.authorName && a.authorName.toLowerCase().includes(authorQuery.toLowerCase())).length} tác giả. Hãy tìm kiếm để thu hẹp kết quả.
                            </div>
                          )}
                        </div>
                        <div className="p-3 border-t bg-gray-50 flex justify-between gap-2">
                          <Button size="sm" variant="outline" onClick={() => setAuthorQuery('')}>Xóa bộ lọc</Button>
                          <Button size="sm" onClick={() => setAuthorOpen(false)}>Đóng</Button>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Nhà xuất bản</Label>
                  <div className="relative">
                    <Button
                      type="button"
                      variant="outline"
                      className="w-full justify-start h-10 font-normal"
                      onClick={() => setPublisherOpen(prev => !prev)}
                    >
                      {(() => {
                        const selected = publishers.find(p => String(p.id) === String((formData as any).selectedPublisherId));
                        return selected?.publisherName || 'Chọn nhà xuất bản (tùy chọn)';
                      })()}
                    </Button>

                    {publisherOpen && (
                      <div key={formData.selectedPublisherId} className="absolute left-0 top-full mt-1 w-full max-w-sm max-h-64 bg-white rounded-md border shadow-lg z-50 overflow-hidden">
                        <div className="p-3 border-b">
                          <Input
                            placeholder="Tìm kiếm nhà xuất bản..."
                            value={publisherQuery}
                            onChange={(e) => setPublisherQuery(e.target.value)}
                            className="h-9"
                          />
                        </div>
                        <div className="max-h-48 overflow-y-auto p-2">
                          <label className="flex items-center gap-3 p-2 hover:bg-gray-50 rounded-md cursor-pointer">
                            <input
                              type="checkbox"
                              checked={!formData.selectedPublisherId || formData.selectedPublisherId === ''}
                              onChange={() => {
                                setFormData({ ...formData, selectedPublisherId: '' });
                                setPublisherOpen(false);
                              }}
                              className="rounded"
                            />
                            <span className="text-sm">Không chọn</span>
                          </label>
                          {publishers
                            .filter(p => p.publisherName && p.publisherName.toLowerCase().includes(publisherQuery.toLowerCase()))
                            .slice(0, 15)
                            .map(publisher => {
                            const checked = String(formData.selectedPublisherId || '') === String(publisher.id || '');
                            return (
                              <label key={publisher.id} className="flex items-center gap-3 p-2 hover:bg-gray-50 rounded-md cursor-pointer">
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={() => {
                                    if (checked) {
                                      setFormData({ ...formData, selectedPublisherId: '' });
                                    } else {
                                      setFormData({ ...formData, selectedPublisherId: String(publisher.id) });
                                    }
                                    setPublisherOpen(false);
                                  }}
                                  className="rounded"
                                />
                                <span className="text-sm">{publisher.publisherName}</span>
                              </label>
                            );
                          })}
                          {publishers.filter(p => p.publisherName && p.publisherName.toLowerCase().includes(publisherQuery.toLowerCase())).length > 15 && (
                            <div className="px-2 py-2 text-xs text-muted-foreground text-center border-t mt-2">
                              Hiển thị 15/{publishers.filter(p => p.publisherName && p.publisherName.toLowerCase().includes(publisherQuery.toLowerCase())).length} nhà xuất bản. Hãy tìm kiếm để thu hẹp kết quả.
                            </div>
                          )}
                        </div>
                        <div className="p-3 border-t bg-gray-50 flex justify-between gap-2">
                          <Button size="sm" variant="outline" onClick={() => setPublisherQuery('')}>Xóa bộ lọc</Button>
                          <Button size="sm" onClick={() => setPublisherOpen(false)}>Đóng</Button>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
                
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Danh mục *</Label>
                  <div className="relative">
                    <Button
                      type="button"
                      variant="outline"
                      className="w-full justify-start h-10 font-normal"
                      onClick={() => setCategoriesOpen(prev => !prev)}
                    >
                      {(formData as any).selectedCategoryIds.length > 0
                        ? `${(formData as any).selectedCategoryIds.length} danh mục đã chọn`
                        : 'Chọn danh mục'}
                    </Button>

                    {categoriesOpen && (
                      <div className="absolute left-0 top-full mt-1 w-full max-w-sm max-h-64 bg-white rounded-md border shadow-lg z-50 overflow-hidden">
                        <div className="p-3 border-b">
                          <Input
                            placeholder="Tìm kiếm danh mục..."
                            value={categoryQuery}
                            onChange={(e) => setCategoryQuery(e.target.value)}
                            className="h-9"
                          />
                        </div>
                        <div className="max-h-48 overflow-y-auto p-2">
                          {categories
                            .filter(c => c.status)
                            .filter(c => c.categoryName.toLowerCase().includes(categoryQuery.toLowerCase()))
                            .slice(0, 15)
                            .map(category => {
                            const checked = (formData as any).selectedCategoryIds.includes(category.id);
                            return (
                              <label key={category.id} className="flex items-center gap-3 p-2 hover:bg-gray-50 rounded-md cursor-pointer">
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={(e) => {
                                    const current = (formData as any).selectedCategoryIds as string[];
                                    if (e.target.checked) {
                                      setFormData({ ...formData, selectedCategoryIds: [...current, category.id] });
                                    } else {
                                      setFormData({ ...formData, selectedCategoryIds: current.filter(id => id !== category.id) });
                                    }
                                  }}
                                  className="rounded"
                                />
                                <span className="text-sm">{category.categoryName}</span>
                              </label>
                            );
                          })}
                          {categories.filter(c => c.status).filter(c => c.categoryName.toLowerCase().includes(categoryQuery.toLowerCase())).length > 15 && (
                            <div className="px-2 py-2 text-xs text-muted-foreground text-center border-t mt-2">
                              Hiển thị 30/{categories.filter(c => c.status).filter(c => c.categoryName.toLowerCase().includes(categoryQuery.toLowerCase())).length} danh mục. Hãy tìm kiếm để thu hẹp kết quả.
                            </div>
                          )}
                        </div>
                        <div className="p-3 border-t bg-gray-50 flex justify-between gap-2">
                          <Button size="sm" variant="outline" onClick={() => setCategoryQuery('')}>Xóa bộ lọc</Button>
                          <Button size="sm" onClick={() => setCategoriesOpen(false)}>Đóng</Button>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Pricing & Publishing Section */}
            <div className="space-y-4">
              <h3 className="text-lg font-medium border-b pb-2">Thông tin xuất bản & giá cả</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="price" className="text-sm font-medium">Giá bán (VNĐ) *</Label>
                  <Input
                    id="price"
                    type="number"
                    value={formData.price}
                    onChange={(e) => setFormData({ ...formData, price: e.target.value })}
                    placeholder="189,000"
                    className="h-10"
                  />
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="publishedYear" className="text-sm font-medium">Năm xuất bản</Label>
                  <Input
                    id="publishedYear"
                    type="number"
                    value={formData.publishedYear}
                    onChange={(e) => setFormData({ ...formData, publishedYear: e.target.value })}
                    placeholder="2024"
                    className="h-10"
                  />
                </div>
                
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Ngôn ngữ</Label>
                  <Select
                    value={formData.language}
                    onValueChange={(val) => setFormData({ ...formData, language: val })}
                  >
                    <SelectTrigger className="h-10">
                      <SelectValue placeholder="Chọn ngôn ngữ" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="vi">Tiếng Việt</SelectItem>
                      <SelectItem value="en">Tiếng Anh</SelectItem>
                      <SelectItem value="zh">Tiếng Trung</SelectItem>
                      <SelectItem value="ja">Tiếng Nhật</SelectItem>
                      <SelectItem value="ko">Tiếng Hàn</SelectItem>
                      <SelectItem value="fr">Tiếng Pháp</SelectItem>
                      <SelectItem value="other">Khác</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Trạng thái</Label>
                  <Select
                    value={formData.status}
                    onValueChange={(val) => setFormData({ ...formData, status: (val as 'active' | 'deleted') })}
                  >
                    <SelectTrigger className="h-10">
                      <SelectValue placeholder="Chọn trạng thái" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="active">Hoạt động</SelectItem>
                      <SelectItem value="deleted">Đã xóa</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            {/* Image & Description Section */}
            <div className="space-y-4">
              <h3 className="text-lg font-medium border-b pb-2">Hình ảnh & mô tả</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-3">
                  <Label className="text-sm font-medium">Ảnh bìa sách</Label>
                  <input
                    ref={useRef<HTMLInputElement | null>(null)}
                    id="imageFile"
                    type="file"
                    accept="image/*"
                    onChange={(e) => {
                      const file = e.target.files && e.target.files[0] ? e.target.files[0] : null;
                      console.log('[BookManagement] File selected:', file?.name, file?.size);
                      if (!file) {
                        console.log('[BookManagement] No file, clearing imageFile and imageUrl');
                        return setFormData({ ...formData, imageFile: null, imageUrl: '' });
                      }
                      const reader = new FileReader();
                      reader.onload = () => {
                        const dataUrl = reader.result as string;
                        console.log('[BookManagement] FileReader loaded, data URL length:', dataUrl?.length);
                        setFormData({ ...formData, imageFile: file, imageUrl: dataUrl });
                      };
                      reader.onerror = (error) => {
                        console.error('[BookManagement] FileReader error:', error);
                      };
                      reader.readAsDataURL(file);
                    }}
                    className="hidden"
                  />
                  <div className="space-y-3">
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => {
                        const el = document.getElementById('imageFile') as HTMLInputElement | null;
                        el?.click();
                      }}
                      className="w-full h-10"
                    >
                      <Plus className="mr-2 h-4 w-4" />
                      Chọn ảnh từ máy
                    </Button>
                    {formData.imageFile && (
                      <p className="text-xs text-muted-foreground truncate">
                        📁 {(formData.imageFile as File).name}
                      </p>
                    )}
                  </div>
                </div>
                
                <div className="space-y-3">
                  <Label className="text-sm font-medium">Xem trước</Label>
                  <div className="flex justify-center">
                    <div className="relative w-32 h-40">
                      {formData.imageUrl ? (
                        <ImageWithFallback
                          src={formData.imageUrl}
                          alt="preview"
                          className="absolute inset-0 w-full h-full object-cover rounded-lg border shadow-sm"
                        />
                      ) : (
                        <div className="absolute inset-0 w-full h-full bg-gray-100 rounded-lg border-2 border-dashed border-gray-300 flex items-center justify-center">
                          <span className="text-gray-400 text-xs text-center">
                            Chưa có<br/>hình ảnh
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="description" className="text-sm font-medium">Mô tả sách *</Label>
                <div className="relative">
                  <Textarea
                    id="description"
                    value={formData.description}
                    onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                    placeholder="Nhập mô tả chi tiết về nội dung, tác giả, và những điểm đặc biệt của sách..."
                    className="resize-none h-40 max-h-40 overflow-y-auto scrollbar-thin scrollbar-thumb-gray-300 scrollbar-track-gray-100"
                    maxLength={1000}
                  />
                </div>
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span>Mô tả chi tiết giúp khách hàng hiểu rõ về sách</span>
                  <span>{formData.description.length}/1000 ký tự</span>
                </div>
              </div>
            </div>
          </div>
          <DialogFooter className="flex-shrink-0 border-t pt-4 mt-4">
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button 
              onClick={handleSubmit}
              disabled={!formData.title || !formData.author || !formData.price || !formData.description}
            >
              {editingBook ? 'Cập nhật' : 'Thêm sách'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Xóa sách</DialogTitle>
            <DialogDescription>
              Bạn có chắc muốn xóa sách "{bookToDelete?.title}"? Hành động này không thể hoàn tác.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
              Hủy
            </Button>
            <Button variant="destructive" onClick={confirmDelete}>
              Xóa
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};