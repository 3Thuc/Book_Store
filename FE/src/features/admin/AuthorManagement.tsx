import React, { useState } from 'react';
import { useAdmin, Author, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Plus, Edit, Trash2, User, Search, PowerOff, RotateCcw, AlertTriangle } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const AuthorManagement: React.FC = () => {
  const { authors, addAuthor, updateAuthor, deleteAuthor, hardDeleteAuthor } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingAuthor, setEditingAuthor] = useState<Author | null>(null);
  const [formData, setFormData] = useState({ name: '', bio: '' });
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);
  const [searchTerm, setSearchTerm] = useState('');

  // Confirm dialog state
  type ConfirmAction = 'deactivate' | 'restore' | 'hardDelete';
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>('deactivate');
  const [actionTarget, setActionTarget] = useState<Author | null>(null);

  // Dùng bookCount từ server (được tính chính xác trong AuthorRes)
  const getAuthorBookCount = (author: Author): number =>
    (author as any).bookCount ?? 0;

  const handleOpenDialog = (author?: Author) => {
    if (author) {
      setEditingAuthor(author);
      setFormData({ name: author.authorName, bio: author.bio ?? '' });
    } else {
      setEditingAuthor(null);
      setFormData({ name: '', bio: '' });
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    if (!formData.name.trim()) return;
    if (editingAuthor) {
      updateAuthor(editingAuthor.id, {
        authorName: formData.name.trim(),
        bio: formData.bio.trim(),
      });
    } else {
      addAuthor({ authorName: formData.name.trim(), bio: formData.bio.trim(), status: ItemStatus.Active });
    }
    setDialogOpen(false);
    setFormData({ name: '', bio: '' });
    setEditingAuthor(null);
  };

  const openConfirm = (action: ConfirmAction, author: Author, e: React.MouseEvent) => {
    e.stopPropagation();
    setConfirmAction(action);
    setActionTarget(author);
    setConfirmOpen(true);
  };

  const handleConfirm = async () => {
    if (!actionTarget) return;
    setConfirmOpen(false);

    if (confirmAction === 'deactivate') {
      await deleteAuthor(actionTarget.id);
    } else if (confirmAction === 'restore') {
      await updateAuthor(actionTarget.id, {
        authorName: actionTarget.authorName,
        bio: actionTarget.bio ?? '',
        status: ItemStatus.Active,
      });
    } else if (confirmAction === 'hardDelete') {
      await hardDeleteAuthor(actionTarget.id);
    }

    setActionTarget(null);
  };

  const confirmMeta = {
    deactivate: {
      title: 'Vô hiệu hóa tác giả',
      desc: (name: string, count: number) =>
        `Tác giả "${name}" có ${count} sách. Vô hiệu hóa sẽ ẩn tác giả khỏi trang người dùng nhưng sách vẫn được giữ nguyên.`,
      btnLabel: 'Vô hiệu hóa',
      btnVariant: 'destructive' as const,
    },
    restore: {
      title: 'Kích hoạt lại tác giả',
      desc: (name: string) => `Tác giả "${name}" sẽ được kích hoạt và hiển thị lại trên hệ thống.`,
      btnLabel: 'Kích hoạt',
      btnVariant: 'default' as const,
    },
    hardDelete: {
      title: 'Xóa vĩnh viễn tác giả',
      desc: (name: string) =>
        `Bạn có chắc muốn xóa vĩnh viễn tác giả "${name}"? Thao tác này KHÔNG thể hoàn tác.`,
      btnLabel: 'Xóa vĩnh viễn',
      btnVariant: 'destructive' as const,
    },
  };

  const filtered = authors.filter(a =>
    a.authorName.toLowerCase().includes(searchTerm.toLowerCase()) ||
    (a.bio ?? '').toLowerCase().includes(searchTerm.toLowerCase())
  );
  const sorted = [...filtered].sort((a, b) => {
    const aActive = a.status === ItemStatus.Active;
    const bActive = b.status === ItemStatus.Active;
    if (aActive === bActive) return a.authorName.localeCompare(b.authorName, 'vi');
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
              <CardTitle>Quản lý tác giả</CardTitle>
              <CardDescription>Thêm, sửa, vô hiệu hóa hoặc xóa tác giả</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm tác giả
            </Button>
          </div>
          {/* Search bar */}
          <div className="relative mt-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <input
              className="w-full pl-9 pr-4 py-2 text-sm border rounded-lg bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
              placeholder="Tìm kiếm tác giả..."
              value={searchTerm}
              onChange={(e) => { setSearchTerm(e.target.value); setCurrentPage(1); }}
            />
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {pageItems.map((author) => {
              const bookCount = getAuthorBookCount(author);
              const isActive = author.status === ItemStatus.Active;

              return (
                <Card
                  key={author.id}
                  className={`relative cursor-pointer transition-all ${isActive ? 'hover:shadow-md' : 'opacity-60 grayscale'}`}
                  onClick={() => handleOpenDialog(author)}
                >
                  <CardHeader className="pb-3">
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-2">
                        <User className="h-5 w-5 text-primary shrink-0" />
                        <CardTitle className="text-base leading-snug">{author.authorName}</CardTitle>
                      </div>
                      <div className="flex items-center gap-1 shrink-0 ml-2">
                        <Badge variant={isActive ? 'secondary' : 'destructive'} className="text-xs">
                          {isActive ? 'active' : 'deleted'}
                        </Badge>

                        {/* Nút Sửa */}
                        <Button
                          variant="ghost" size="sm"
                          onClick={(e) => { e.stopPropagation(); handleOpenDialog(author); }}
                          title="Chỉnh sửa"
                        >
                          <Edit className="h-4 w-4" />
                        </Button>

                        {isActive ? (
                          <>
                            {/* Vô hiệu hóa */}
                            <Button
                              variant="ghost" size="sm"
                              onClick={(e) => openConfirm('deactivate', author, e)}
                              title="Vô hiệu hóa"
                            >
                              <PowerOff className="h-4 w-4 text-amber-500" />
                            </Button>
                            {/* Xóa vĩnh viễn — chỉ hiện khi 0 sách */}
                            {bookCount === 0 && (
                              <Button
                                variant="ghost" size="sm"
                                onClick={(e) => openConfirm('hardDelete', author, e)}
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
                            onClick={(e) => openConfirm('restore', author, e)}
                            title="Kích hoạt lại"
                          >
                            <RotateCcw className="h-4 w-4 text-green-600" />
                          </Button>
                        )}
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {author.bio && (
                        <p className="text-sm text-muted-foreground line-clamp-2">{author.bio}</p>
                      )}
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-muted-foreground">Số lượng sách</span>
                        <Badge variant="secondary">{bookCount}</Badge>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          <PaginationControls
            totalItems={filtered.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(filtered.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6, 9, 12, 15]}
          />

          {authors.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              <User className="h-12 w-12 mx-auto mb-4 opacity-20" />
              <p>Chưa có tác giả nào</p>
              <p className="text-sm">Nhấn nút "Thêm tác giả" để bắt đầu</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingAuthor ? 'Chỉnh sửa tác giả' : 'Thêm tác giả mới'}</DialogTitle>
            <DialogDescription>
              {editingAuthor ? 'Cập nhật thông tin tác giả' : 'Nhập thông tin tác giả mới'}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="authorName">Tên tác giả *</Label>
              <Input
                id="authorName"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="VD: Nguyễn Nhật Ánh"
                autoFocus
                onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="authorBio">Tiểu sử</Label>
              <Textarea
                id="authorBio"
                value={formData.bio}
                onChange={(e) => setFormData({ ...formData, bio: e.target.value })}
                placeholder="Giới thiệu ngắn về tác giả..."
                rows={3}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>Hủy</Button>
            <Button onClick={handleSubmit} disabled={!formData.name.trim()}>
              {editingAuthor ? 'Cập nhật' : 'Thêm tác giả'}
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
                confirmMeta.deactivate.desc(actionTarget.authorName, getAuthorBookCount(actionTarget))}
              {actionTarget && confirmAction === 'restore' &&
                confirmMeta.restore.desc(actionTarget.authorName)}
              {actionTarget && confirmAction === 'hardDelete' &&
                confirmMeta.hardDelete.desc(actionTarget.authorName)}
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