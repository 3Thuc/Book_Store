import React, { useState } from 'react';
import { useAdmin, Category, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Switch } from '../../components/ui/switch';
import { Plus, Edit, Trash2, Tag } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const CategoryManagement: React.FC = () => {
  const { categories, books, addCategory, updateCategory, deleteCategory } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingCategory, setEditingCategory] = useState<Category | null>(null);
  const [formData, setFormData] = useState({
    categoryName: '',
    status: ItemStatus.Active,
  });
  // pagination for categories similar to authors/publishers
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [categoryToDelete, setCategoryToDelete] = useState<Category | null>(null);

  const handleOpenDialog = (category?: Category) => {
    if (category) {
      setEditingCategory(category);
      setFormData({
        categoryName: category.categoryName,
        
        status: category.status ?? ItemStatus.Active,
      });
    } else {
      setEditingCategory(null);
      setFormData({
        categoryName: '',
        status: ItemStatus.Active,
      });
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    if (!formData.categoryName.trim()) return;

    if (editingCategory) {
      updateCategory(editingCategory.id, {
        categoryName: formData.categoryName.trim(),
        status: formData.status,
      });
    } else {
      addCategory({
        categoryName: formData.categoryName.trim(),
        status: formData.status,
      });
    }

    setDialogOpen(false);
    setFormData({ categoryName: '', status: ItemStatus.Active });
    setEditingCategory(null);
  };

  const handleDelete = (category: Category) => {
    setCategoryToDelete(category);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (categoryToDelete) {
      deleteCategory(categoryToDelete.id);
      setDeleteDialogOpen(false);
      setCategoryToDelete(null);
    }
  };

  // Get book count for each category
  const getCategoryBookCount = (categoryName: string) => {
    return books.filter(book => Array.isArray(book.categories) && book.categories.some((c: any) => c.categoryName === categoryName)).length;
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý danh mục</CardTitle>
              <CardDescription>Thêm, sửa, xóa danh mục sách</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm danh mục
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {(() => {
              const sorted = [...categories].sort((a, b) => {
                const aActive = a.status === ItemStatus.Active;
                const bActive = b.status === ItemStatus.Active;
                if (aActive === bActive) return 0;
                return aActive ? -1 : 1;
              });
              const total = sorted.length;
              const totalPages = Math.max(1, Math.ceil(total / pageSize));
              const start = (currentPage - 1) * pageSize;
              const pageItems = sorted.slice(start, start + pageSize);

              return pageItems.map((category) => {
                const bookCount = getCategoryBookCount(category.categoryName);
                const statusActive = category.status === ItemStatus.Active;

                return (
                  <Card key={category.id} className={`relative cursor-pointer ${statusActive ? '' : 'opacity-60 grayscale'}`} onClick={() => handleOpenDialog(category)}>
                    <CardHeader className="pb-3">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-2">
                          <Tag className="h-5 w-5 text-primary" />
                          <CardTitle className="text-lg">{category.categoryName}</CardTitle>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant={statusActive ? 'secondary' : 'destructive'}>{statusActive ? 'active' : 'deleted'}</Badge>
                          <>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleOpenDialog(category); }}
                            >
                              <Edit className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleDelete(category); }}
                            >
                              <Trash2 className="h-4 w-4 text-destructive" />
                            </Button>
                          </>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-muted-foreground">Số lượng sách</span>
                          <Badge variant="secondary">{bookCount}</Badge>
                        </div>
                        {/* description removed per UI requirement */}
                      </div>
                    </CardContent>
                  </Card>
                );
              });
            })()}
          </div>

          {/* Pagination controls for categories */}
          <PaginationControls
            totalItems={categories.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(categories.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6,9,12,15]}
          />

          {categories.filter(cat => cat.status === ItemStatus.Active).length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              <Tag className="h-12 w-12 mx-auto mb-4 opacity-20" />
              <p>Chưa có danh mục nào</p>
              <p className="text-sm">Nhấn nút "Thêm danh mục" để bắt đầu</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingCategory ? 'Chỉnh sửa danh mục' : 'Thêm danh mục mới'}</DialogTitle>
            <DialogDescription>
              {editingCategory 
                ? 'Cập nhật thông tin danh mục'
                : 'Nhập thông tin danh mục mới cho sách'
              }
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="categoryName">Tên danh mục *</Label>
              <Input
                id="categoryName"
                value={formData.categoryName}
                onChange={(e) => setFormData({ ...formData, categoryName: e.target.value })}
                placeholder="VD: Khoa học viễn tưởng"
                autoFocus
              />
            </div>
            <div className="flex items-center gap-4">
              <div className="grid gap-2">
                <Label htmlFor="categoryActive">Trạng thái</Label>
                <div className="flex items-center gap-2">
                  <Switch id="categoryActive" checked={formData.status === ItemStatus.Active} onCheckedChange={(v) => setFormData({ ...formData, status: v ? ItemStatus.Active : ItemStatus.Deleted })} />
                  <span className="text-sm text-muted-foreground">{formData.status === ItemStatus.Active ? 'active' : 'deleted'}</span>
                </div>
              </div>
            </div>
            {/* slug removed: UI uses `name` only */}
            {/* description field removed */}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button onClick={handleSubmit} disabled={!formData.categoryName.trim()}>
              {editingCategory ? 'Cập nhật' : 'Thêm danh mục'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Xóa danh mục</DialogTitle>
            <DialogDescription>
              Bạn có chắc muốn xóa danh mục "{categoryToDelete?.categoryName}"?
              <br />
              <br />
              Thao tác này sẽ ẩn danh mục khỏi hệ thống. Các sách thuộc danh mục này ({getCategoryBookCount(categoryToDelete?.categoryName || '')}) sẽ vẫn giữ nguyên.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
              Hủy
            </Button>
            <Button variant="destructive" onClick={confirmDelete}>
              Xóa danh mục
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
