import React, { useState, useEffect } from 'react';
import { useAdmin, Publisher, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Textarea } from '../../components/ui/textarea';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Switch } from '../../components/ui/switch';
import { Plus, Edit, Trash2, Building2, Globe } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const PublisherManagement: React.FC = () => {
  const { publishers, books, addPublisher, updatePublisher, deletePublisher } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingPublisher, setEditingPublisher] = useState<Publisher | null>(null);
  const [formData, setFormData] = useState({
    name: '',
    status: ItemStatus.Active,
  });
  // pagination
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [publisherToDelete, setPublisherToDelete] = useState<Publisher | null>(null);

  const handleOpenDialog = (publisher?: Publisher) => {
    if (publisher) {
      setEditingPublisher(publisher);
      setFormData({
        name: publisher.publisherName,
        status: publisher.status ?? ItemStatus.Active,
      });
    } else {
      setEditingPublisher(null);
      setFormData({ name: '', status: ItemStatus.Active });
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    if (!formData.name.trim()) return;

    if (editingPublisher) {
      updatePublisher(editingPublisher.id, {
        publisherName: formData.name.trim(),
        status: formData.status,
      });
    } else {
      addPublisher({
        publisherName: formData.name.trim(),
        status: formData.status,
      });
    }

    setDialogOpen(false);
    setFormData({ name: '', status: ItemStatus.Active });
    setEditingPublisher(null);
  };

  const handleDelete = (publisher: Publisher) => {
    setPublisherToDelete(publisher);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (publisherToDelete) {
      deletePublisher(publisherToDelete.id);
      setDeleteDialogOpen(false);
      setPublisherToDelete(null);
    }
  };

  // Get book count for each publisher (handle book.publisher as string or object)
  const getPublisherBookCount = (publisherName: string) => {
    return books.filter(book => {
      const pub: any = (book as any).publisher;
      if (!pub) return false;
      if (typeof pub === 'string') return pub === publisherName;
      if (typeof pub === 'object') return (pub.publisherName ?? pub.name ?? pub) === publisherName;
      return false;
    }).length;
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý nhà xuất bản</CardTitle>
              <CardDescription>Thêm, sửa, xóa nhà xuất bản</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm nhà xuất bản
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {(() => {
              const sorted = [...publishers].sort((a, b) => {
                const aActive = a.status === ItemStatus.Active;
                const bActive = b.status === ItemStatus.Active;
                if (aActive === bActive) return 0;
                return aActive ? -1 : 1;
              });
              const total = sorted.length;
              const totalPages = Math.max(1, Math.ceil(total / pageSize));
              const start = (currentPage - 1) * pageSize;
              const pageItems = sorted.slice(start, start + pageSize);

              return pageItems.map((publisher) => {
                const bookCount = getPublisherBookCount(publisher.publisherName);
                const status = publisher.status === ItemStatus.Active;

                return (
                  <Card key={publisher.id} className={`relative cursor-pointer ${status ? '' : 'opacity-60 grayscale'}`} onClick={() => handleOpenDialog(publisher)}>
                    <CardHeader className="pb-3">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-2">
                          <Building2 className="h-5 w-5 text-primary" />
                          <CardTitle className="text-lg">{publisher.publisherName}</CardTitle>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant={status ? 'secondary' : 'destructive'}>{status ? 'active' : 'deleted'}</Badge>
                          <>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleOpenDialog(publisher); }}
                            >
                              <Edit className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => { e.stopPropagation(); handleDelete(publisher); }}
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
                        {/* Only showing name and book count for publishers (DB stores id and name) */}
                      </div>
                    </CardContent>
                  </Card>
                );
              });
            })()}
          </div>

          {/* Pagination controls */}
          <PaginationControls
            totalItems={publishers.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(publishers.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6,9,12,15]}
          />


          {publishers.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              <Building2 className="h-12 w-12 mx-auto mb-4 opacity-20" />
              <p>Chưa có nhà xuất bản nào</p>
              <p className="text-sm">Nhấn nút "Thêm nhà xuất bản" để bắt đầu</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingPublisher ? 'Chỉnh sửa nhà xuất bản' : 'Thêm nhà xuất bản mới'}</DialogTitle>
            <DialogDescription>
              {editingPublisher 
                ? 'Cập nhật thông tin nhà xuất bản'
                : 'Nhập thông tin nhà xuất bản mới'
              }
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="publisherName">Tên nhà xuất bản *</Label>
              <Input
                id="publisherName"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="VD: NXB Kim Đồng"
                autoFocus
              />
            </div>
            <div className="flex items-center gap-4">
              <div className="grid gap-2">
                <Label htmlFor="publisherActive">Trạng thái</Label>
                <div className="flex items-center gap-2">
                  <Switch id="publisherActive" checked={formData.status === ItemStatus.Active} onCheckedChange={(v) => setFormData({ ...formData, status: v ? ItemStatus.Active : ItemStatus.Deleted })} />
                  <span className="text-sm text-muted-foreground">{formData.status === ItemStatus.Active ? 'active' : 'deleted'}</span>
                </div>
              </div>
            </div>
            {/* Only name is required for publisher (DB stores id + name) */}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button onClick={handleSubmit} disabled={!formData.name.trim()}>
              {editingPublisher ? 'Cập nhật' : 'Thêm nhà xuất bản'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Xóa nhà xuất bản</DialogTitle>
            <DialogDescription>
              Bạn có chắc muốn xóa nhà xuất bản "{publisherToDelete?.publisherName}"?
              <br />
              <br />
              Thao tác này sẽ ẩn nhà xuất bản khỏi hệ thống. Các sách của nhà xuất bản này ({getPublisherBookCount(publisherToDelete?.publisherName || '')}) sẽ vẫn giữ nguyên.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
              Hủy
            </Button>
            <Button variant="destructive" onClick={confirmDelete}>
              Xóa nhà xuất bản
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
