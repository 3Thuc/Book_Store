import React, { useState } from 'react';
import { useAdmin, Publisher, ItemStatus } from './AdminContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Badge } from '../../components/ui/badge';
import { Plus, Edit, Trash2, Building2, Search, PowerOff, RotateCcw, AlertTriangle } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const PublisherManagement: React.FC = () => {
  const { publishers, addPublisher, updatePublisher, deletePublisher, hardDeletePublisher } = useAdmin();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingPublisher, setEditingPublisher] = useState<Publisher | null>(null);
  const [formData, setFormData] = useState({ name: '', status: ItemStatus.Active });
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(9);
  const [searchTerm, setSearchTerm] = useState('');

  // Confirm dialog state
  type ConfirmAction = 'deactivate' | 'restore' | 'hardDelete';
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>('deactivate');
  const [actionTarget, setActionTarget] = useState<Publisher | null>(null);

  // Dùng bookCount từ server (được tính chính xác trong PublishersRes)
  const getPublisherBookCount = (publisher: Publisher): number =>
    (publisher as any).bookCount ?? 0;

  const handleOpenDialog = (publisher?: Publisher) => {
    if (publisher) {
      setEditingPublisher(publisher);
      setFormData({ name: publisher.publisherName, status: publisher.status ?? ItemStatus.Active });
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
      addPublisher({ publisherName: formData.name.trim(), status: formData.status });
    }
    setDialogOpen(false);
    setFormData({ name: '', status: ItemStatus.Active });
    setEditingPublisher(null);
  };

  const openConfirm = (action: ConfirmAction, publisher: Publisher, e: React.MouseEvent) => {
    e.stopPropagation();
    setConfirmAction(action);
    setActionTarget(publisher);
    setConfirmOpen(true);
  };

  const handleConfirm = async () => {
    if (!actionTarget) return;
    setConfirmOpen(false);

    if (confirmAction === 'deactivate') {
      await deletePublisher(actionTarget.id);
    } else if (confirmAction === 'restore') {
      await updatePublisher(actionTarget.id, {
        publisherName: actionTarget.publisherName,
        status: ItemStatus.Active,
      });
    } else if (confirmAction === 'hardDelete') {
      await hardDeletePublisher(actionTarget.id);
    }

    setActionTarget(null);
  };

  const confirmMeta = {
    deactivate: {
      title: 'Vô hiệu hóa nhà xuất bản',
      desc: (name: string, count: number) =>
        `Nhà xuất bản "${name}" có ${count} sách. Vô hiệu hóa sẽ ẩn NXB khỏi trang người dùng nhưng sách vẫn được giữ nguyên.`,
      btnLabel: 'Vô hiệu hóa',
      btnVariant: 'destructive' as const,
    },
    restore: {
      title: 'Kích hoạt lại nhà xuất bản',
      desc: (name: string) => `Nhà xuất bản "${name}" sẽ được kích hoạt và hiển thị lại trên hệ thống.`,
      btnLabel: 'Kích hoạt',
      btnVariant: 'default' as const,
    },
    hardDelete: {
      title: 'Xóa vĩnh viễn nhà xuất bản',
      desc: (name: string) =>
        `Bạn có chắc muốn xóa vĩnh viễn nhà xuất bản "${name}"? Thao tác này KHÔNG thể hoàn tác.`,
      btnLabel: 'Xóa vĩnh viễn',
      btnVariant: 'destructive' as const,
    },
  };

  const filtered = publishers.filter(p =>
    p.publisherName.toLowerCase().includes(searchTerm.toLowerCase())
  );
  const sorted = [...filtered].sort((a, b) => {
    const aActive = a.status === ItemStatus.Active;
    const bActive = b.status === ItemStatus.Active;
    if (aActive === bActive) return a.publisherName.localeCompare(b.publisherName, 'vi');
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
              <CardTitle>Quản lý nhà xuất bản</CardTitle>
              <CardDescription>Thêm, sửa, vô hiệu hóa hoặc xóa nhà xuất bản</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Thêm nhà xuất bản
            </Button>
          </div>
          {/* Search bar */}
          <div className="relative mt-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <input
              className="w-full pl-9 pr-4 py-2 text-sm border rounded-lg bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
              placeholder="Tìm kiếm nhà xuất bản..."
              value={searchTerm}
              onChange={(e) => { setSearchTerm(e.target.value); setCurrentPage(1); }}
            />
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {pageItems.map((publisher) => {
              const bookCount = getPublisherBookCount(publisher);
              const isActive = publisher.status === ItemStatus.Active;

              return (
                <Card
                  key={publisher.id}
                  className={`relative cursor-pointer transition-all ${isActive ? 'hover:shadow-md' : 'opacity-60 grayscale'}`}
                  onClick={() => handleOpenDialog(publisher)}
                >
                  <CardHeader className="pb-3">
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-2">
                        <Building2 className="h-5 w-5 text-primary shrink-0" />
                        <CardTitle className="text-base leading-snug">{publisher.publisherName}</CardTitle>
                      </div>
                      <div className="flex items-center gap-1 shrink-0 ml-2">
                        <Badge variant={isActive ? 'secondary' : 'destructive'} className="text-xs">
                          {isActive ? 'active' : 'deleted'}
                        </Badge>

                        {/* Nút Sửa */}
                        <Button
                          variant="ghost" size="sm"
                          onClick={(e) => { e.stopPropagation(); handleOpenDialog(publisher); }}
                          title="Chỉnh sửa"
                        >
                          <Edit className="h-4 w-4" />
                        </Button>

                        {isActive ? (
                          <>
                            {/* Vô hiệu hóa */}
                            <Button
                              variant="ghost" size="sm"
                              onClick={(e) => openConfirm('deactivate', publisher, e)}
                              title="Vô hiệu hóa"
                            >
                              <PowerOff className="h-4 w-4 text-amber-500" />
                            </Button>
                            {/* Xóa vĩnh viễn — chỉ hiện khi 0 sách */}
                            {bookCount === 0 && (
                              <Button
                                variant="ghost" size="sm"
                                onClick={(e) => openConfirm('hardDelete', publisher, e)}
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
                            onClick={(e) => openConfirm('restore', publisher, e)}
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
            totalItems={filtered.length}
            currentPage={currentPage}
            totalPages={Math.max(1, Math.ceil(filtered.length / pageSize))}
            pageSize={pageSize}
            onPageChange={(p: number) => setCurrentPage(p)}
            onPageSizeChange={(s: number) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[6, 9, 12, 15]}
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
              {editingPublisher ? 'Cập nhật thông tin nhà xuất bản' : 'Nhập thông tin nhà xuất bản mới'}
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
                onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>Hủy</Button>
            <Button onClick={handleSubmit} disabled={!formData.name.trim()}>
              {editingPublisher ? 'Cập nhật' : 'Thêm nhà xuất bản'}
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
                confirmMeta.deactivate.desc(actionTarget.publisherName, getPublisherBookCount(actionTarget))}
              {actionTarget && confirmAction === 'restore' &&
                confirmMeta.restore.desc(actionTarget.publisherName)}
              {actionTarget && confirmAction === 'hardDelete' &&
                confirmMeta.hardDelete.desc(actionTarget.publisherName)}
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
