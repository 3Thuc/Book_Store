import React, { useState } from 'react';
import { useAdmin, Category, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Plus, Edit, Trash2, Tag, PowerOff, RotateCcw, AlertTriangle } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const CategoryManagement: React.FC = () => {
  const { categories, addCategory, updateCategory, deleteCategory, hardDeleteCategory } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingCategory, setEditingCategory] = useState<Category | null>(null);
  const [formData, setFormData] = useState({ categoryName: '' });
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);

  // Confirm dialog state
  type ConfirmAction = 'deactivate' | 'restore' | 'hardDelete';
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>('deactivate');
  const [actionTarget, setActionTarget] = useState<Category | null>(null);

  // Dùng bookCount từ server (được tính chính xác trong CategoryRes)
  const getCategoryBookCount = (category: Category): number =>
    (category as any).bookCount ?? 0;

  const handleOpenDialog = (category?: Category) => {
    if (category) {
      setEditingCategory(category);
      setFormData({ categoryName: category.categoryName });
    } else {
      setEditingCategory(null);
      setFormData({ categoryName: '' });
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    if (!formData.categoryName.trim()) return;

    if (editingCategory) {
      updateCategory(editingCategory.id, {
        categoryName: formData.categoryName.trim(),
        status: editingCategory.status, // giữ nguyên status khi edit tên
      });
    } else {
      addCategory({
        categoryName: formData.categoryName.trim(),
        status: ItemStatus.Active,
      });
    }

    setDialogOpen(false);
    setFormData({ categoryName: '' });
    setEditingCategory(null);
  };

  const openConfirm = (action: ConfirmAction, category: Category, e: React.MouseEvent) => {
    e.stopPropagation();
    setConfirmAction(action);
    setActionTarget(category);
    setConfirmOpen(true);
  };

  const handleConfirm = async () => {
    if (!actionTarget) return;
    setConfirmOpen(false);

    if (confirmAction === 'deactivate') {
      await deleteCategory(actionTarget.id);
    } else if (confirmAction === 'restore') {
      await updateCategory(actionTarget.id, { status: ItemStatus.Active, categoryName: actionTarget.categoryName });
    } else if (confirmAction === 'hardDelete') {
      await hardDeleteCategory(actionTarget.id);
    }

    setActionTarget(null);
  };

  const confirmMeta = {
    deactivate: {
      title: 'Vô hiệu hóa danh mục',
      desc: (name: string, count: number) =>
        `Danh mục "${name}" có ${count} sách. Vô hiệu hóa sẽ ẩn danh mục khỏi trang người dùng nhưng sách vẫn được giữ nguyên.`,
      btnLabel: 'Vô hiệu hóa',
      btnVariant: 'destructive' as const,
    },
    restore: {
      title: 'Kích hoạt lại danh mục',
      desc: (name: string) => `Danh mục "${name}" sẽ được kích hoạt và hiển thị lại trên trang người dùng.`,
      btnLabel: 'Kích hoạt',
      btnVariant: 'default' as const,
    },
    hardDelete: {
      title: 'Xóa vĩnh viễn danh mục',
      desc: (name: string) =>
        `Bạn có chắc muốn xóa vĩnh viễn danh mục "${name}"? Thao tác này KHÔNG thể hoàn tác.`,
      btnLabel: 'Xóa vĩnh viễn',
      btnVariant: 'destructive' as const,
    },
  };

  const sorted = [...categories].sort((a, b) => {
    const aActive = a.status === ItemStatus.Active;
    const bActive = b.status === ItemStatus.Active;
    if (aActive === bActive) return a.categoryName.localeCompare(b.categoryName, 'vi');
    return aActive ? -1 : 1;
  });
  const start = (currentPage - 1) * pageSize;
  const pageItems = sorted.slice(start, start + pageSize);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý danh mục</CardTitle>
              <CardDescription>Thêm, sửa, vô hiệu hóa hoặc xóa danh mục sách</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm danh mục
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {pageItems.map((category) => {
              const bookCount = getCategoryBookCount(category);
              const isActive = category.status === ItemStatus.Active;

              return (
                <Card
                  key={category.id}
                  className={`relative cursor-pointer transition-all ${isActive ? 'hover:shadow-md' : 'opacity-60 grayscale'}`}
                  onClick={() => handleOpenDialog(category)}
                >
                  <CardHeader className="pb-3">
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-2">
                        <Tag className="h-5 w-5 text-primary shrink-0" />
                        <CardTitle className="text-base leading-snug">{category.categoryName}</CardTitle>
                      </div>
                      <div className="flex items-center gap-1 shrink-0 ml-2">
                        <Badge variant={isActive ? 'secondary' : 'destructive'} className="text-xs">
                          {isActive ? 'active' : 'deleted'}
                        </Badge>
                        {/* Nút Sửa tên */}
                        <Button
                          variant="ghost" size="sm"
                          onClick={(e) => { e.stopPropagation(); handleOpenDialog(category); }}
                          title="Chỉnh sửa tên"
                        >
                          <Edit className="h-4 w-4" />
                        </Button>

                        {isActive ? (
                          <>
                            {/* Vô hiệu hóa */}
                            <Button
                              variant="ghost" size="sm"
                              onClick={(e) => openConfirm('deactivate', category, e)}
                              title="Vô hiệu hóa"
                            >
                              <PowerOff className="h-4 w-4 text-amber-500" />
                            </Button>
                            {/* Xóa vĩnh viễn — chỉ hiện khi 0 sách */}
                            {bookCount === 0 && (
                              <Button
                                variant="ghost" size="sm"
                                onClick={(e) => openConfirm('hardDelete', category, e)}
                                title="Xóa vĩnh viễn"
                              >
                                <Trash2 className="h-4 w-4 text-destructive" />
                              </Button>
                            )}
                          </>
                        ) : (
                          /* Kích hoạt lại */
                          <Button
                            variant="ghost" size="sm"
                            onClick={(e) => openConfirm('restore', category, e)}
                            title="Kích hoạt lại"
                          >
                            <RotateCcw className="h-4 w-4 text-green-600" />
                          </Button>
                        )}
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-muted-foreground">Số lượng sách</span>
                      <Badge variant="secondary">{bookCount}</Badge>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          <PaginationControls
            totalItems={categories.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(categories.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6, 9, 12, 15]}
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

      {/* Add/Edit Dialog — chỉ sửa TÊN, không động status */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingCategory ? 'Chỉnh sửa tên danh mục' : 'Thêm danh mục mới'}</DialogTitle>
            <DialogDescription>
              {editingCategory
                ? 'Cập nhật tên danh mục. Trạng thái không thay đổi.'
                : 'Nhập tên danh mục mới cho sách'}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="categoryName">Tên danh mục *</Label>
              <Input
                id="categoryName"
                value={formData.categoryName}
                onChange={(e) => setFormData({ categoryName: e.target.value })}
                placeholder="VD: Khoa học viễn tưởng"
                autoFocus
                onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>Hủy</Button>
            <Button onClick={handleSubmit} disabled={!formData.categoryName.trim()}>
              {editingCategory ? 'Cập nhật' : 'Thêm danh mục'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Confirm Action Dialog */}
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {confirmAction === 'hardDelete' && <AlertTriangle className="h-5 w-5 text-destructive" />}
              {actionTarget && confirmMeta[confirmAction].title}
            </DialogTitle>
            <DialogDescription>
              {actionTarget && confirmAction === 'deactivate' &&
                confirmMeta.deactivate.desc(actionTarget.categoryName, getCategoryBookCount(actionTarget))}
              {actionTarget && confirmAction === 'restore' &&
                confirmMeta.restore.desc(actionTarget.categoryName)}
              {actionTarget && confirmAction === 'hardDelete' &&
                confirmMeta.hardDelete.desc(actionTarget.categoryName)}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)}>Hủy</Button>
            <Button variant={confirmMeta[confirmAction]?.btnVariant} onClick={handleConfirm}>
              {confirmMeta[confirmAction]?.btnLabel}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
