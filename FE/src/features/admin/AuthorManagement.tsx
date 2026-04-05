import React, { useState, useEffect } from 'react';
import { useAdmin, Author, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Switch } from '../../components/ui/switch';
import { Plus, Edit, Trash2, UserPen, Globe, Calendar } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const AuthorManagement: React.FC = () => {
  const { authors, books, addAuthor, updateAuthor, deleteAuthor } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingAuthor, setEditingAuthor] = useState<Author | null>(null);
  const [formData, setFormData] = useState({
    name: '',
    bio: '',
    status: ItemStatus.Active,
  });
  // pagination
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [authorToDelete, setAuthorToDelete] = useState<Author | null>(null);

  const handleOpenDialog = (author?: Author) => {
    if (author) {
      setEditingAuthor(author);
      setFormData({
        name: author.authorName,
        bio: author.bio,
        status: author.status ?? ItemStatus.Active,
      });
    } else {
      setEditingAuthor(null);
      setFormData({ name: '', bio: '', status: ItemStatus.Active });
    }
    setDialogOpen(true);
  };

    const handleSubmit = () => {
    if (!formData.name.trim()) return;

    const authorData = {
      authorName: formData.name.trim(),
      bio: formData.bio.trim(),
      status: formData.status,
    };

    if (editingAuthor) {
      updateAuthor(editingAuthor.id, authorData);
    } else {
      addAuthor({ ...authorData });
    }

    setDialogOpen(false);
    setFormData({ name: '', bio: '', status: ItemStatus.Active });
    setEditingAuthor(null);
  };

  const handleDelete = (author: Author) => {
    setAuthorToDelete(author);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (authorToDelete) {
      deleteAuthor(authorToDelete.id);
      setDeleteDialogOpen(false);
      setAuthorToDelete(null);
    }
  };

  // Get book count for each author
  const getAuthorBookCount = (authorName: string) => {
    return books.filter(book => book.author === authorName).length;
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý tác giả</CardTitle>
              <CardDescription>Thêm, sửa, xóa tác giả</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm tác giả
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {(() => {
              const sorted = [...authors].sort((a, b) => {
                const aActive = a.status === ItemStatus.Active;
                const bActive = b.status === ItemStatus.Active;
                if (aActive === bActive) return 0;
                return aActive ? -1 : 1;
              });
              const total = sorted.length;
              const totalPages = Math.max(1, Math.ceil(total / pageSize));
              const start = (currentPage - 1) * pageSize;
              const pageItems = sorted.slice(start, start + pageSize);

              return pageItems.map((author) => {
                const bookCount = getAuthorBookCount(author.authorName);
                const status = author.status === ItemStatus.Active;

                return (
                  <Card key={author.id} className={`relative cursor-pointer ${status ? '' : 'opacity-60 grayscale'}`} onClick={() => handleOpenDialog(author)}>
                    <CardHeader className="pb-3">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-2">
                          <UserPen className="h-5 w-5 text-primary" />
                          <CardTitle className="text-lg">{author.authorName}</CardTitle>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant={status ? 'secondary' : 'destructive'}>{status ? 'active' : 'deleted'}</Badge>
                          <>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleOpenDialog(author); }}
                            >
                              <Edit className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleDelete(author); }}
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
                        {author.bio && (
                          <p className="text-sm text-muted-foreground line-clamp-2">{author.bio}</p>
                        )}
                        <div className="flex flex-col gap-1">
                          {/* birthYear removed from UI - only name and bio shown */}
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                );
              });
            })()}
          </div>

          {/* Pagination controls */}
          <PaginationControls
            totalItems={authors.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(authors.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6,9,12,15]}
          />

          {authors.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              <UserPen className="h-12 w-12 mx-auto mb-4 opacity-20" />
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
              {editingAuthor 
                ? 'Cập nhật thông tin tác giả'
                : 'Nhập thông tin tác giả mới'
              }
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
              />
            </div>
            <div className="flex items-center gap-4">
              <div className="grid gap-2">
                <Label htmlFor="authorActive">Trạng thái</Label>
                <div className="flex items-center gap-2">
                  <Switch id="authorActive" checked={formData.status === ItemStatus.Active} onCheckedChange={(v) => setFormData({ ...formData, status: v ? ItemStatus.Active : ItemStatus.Deleted })} />
                    <span className="text-sm text-muted-foreground">{formData.status === ItemStatus.Active ? 'active' : 'deleted'}</span>
                </div>
              </div>
            </div>
            {/* birthYear removed from form - only name and bio are collected */}
            <div className="grid gap-2">
              <Label htmlFor="authorBio">Tiểu sử</Label>
              <Textarea
                id="authorBio"
                value={formData.bio}
                onChange={(e) => setFormData({ ...formData, bio: e.target.value })}
                placeholder="Tiểu sử ngắn về tác giả..."
                rows={3}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button onClick={handleSubmit} disabled={!formData.name.trim()}>
              {editingAuthor ? 'Cập nhật' : 'Thêm tác giả'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Xóa tác giả</DialogTitle>
            <DialogDescription>
              Bạn có chắc muốn xóa tác giả "{authorToDelete?.authorName}"?
              <br />
              <br />
              Thao tác này sẽ ẩn tác giả khỏi hệ thống. Các sách của tác giả này ({getAuthorBookCount(authorToDelete?.authorName || '')}) sẽ vẫn giữ nguyên.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
              Hủy
            </Button>
            <Button variant="destructive" onClick={confirmDelete}>
              Xóa tác giả
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};