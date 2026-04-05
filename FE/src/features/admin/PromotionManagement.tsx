import React, { useState } from 'react';
import { useAdmin, Promotion } from './AdminContext';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { formatVND } from '../../lib/formatters';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Percent, Plus, Edit, Trash2, Tag, Calendar } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';

export const PromotionManagement: React.FC = () => {
  const { promotions, addPromotion, updatePromotion, deletePromotion } = useAdmin();
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingPromotion, setEditingPromotion] = useState<Promotion | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [promoToDelete, setPromoToDelete] = useState<Promotion | null>(null);

  // Form state (match BE: discountPercent + dates + isActive/status)
  const [formData, setFormData] = useState({
    code: '',
    discountPercent: '',
    startDate: '',
    endDate: '',
    status: 'active' as 'active' | 'inactive' | 'deleted',

    //status: 'active' as 'active' | 'inactive' | 'expired',
  });

  // date helpers: prevent selecting dates earlier than today
  const todayStr = new Date().toISOString().split('T')[0];
  // keep only `todayStr` (min attributes enforce date picking); remove inline start-date note

  const resetForm = () => {
    setFormData({ code: '', discountPercent: '', startDate: '', endDate: '', status: 'active' });
    setEditingPromotion(null);
  };

  const handleOpenDialog = (promo?: Promotion) => {
    if (promo) {
      setEditingPromotion(promo);
      const startDate = typeof promo.startDate === 'string' ? promo.startDate : (promo.startDate ? new Date(String(promo.startDate)).toISOString() : '');
      const endDate = typeof promo.endDate === 'string' ? promo.endDate : (promo.endDate ? new Date(String(promo.endDate)).toISOString() : '');
      setFormData({
        code: promo.code,
        discountPercent: (promo.discountPercent ?? 0).toString(),
        startDate: startDate ? startDate.split('T')[0] : '',
        endDate: endDate ? endDate.split('T')[0] : '',
        status: promo.status ?? (promo.isActive ? 'active' : 'inactive'),
      });
    } else {
      resetForm();
    }
    setDialogOpen(true);
  };

  const handleSubmit = () => {
    const promoData = {
      code: formData.code.toUpperCase(),
      discountPercent: parseFloat(formData.discountPercent) || 0,
      startDate: formData.startDate,
      endDate: formData.endDate,
      status: formData.status,
      isActive: formData.status === 'active',
    };

    if (editingPromotion) {
      updatePromotion({ ...promoData, id: editingPromotion.id });
    } else {
      addPromotion(promoData);
    }

    setDialogOpen(false);
    resetForm();
  };

  const handleDelete = (promo: Promotion) => {
    setPromoToDelete(promo);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (promoToDelete) {
      deletePromotion(promoToDelete.id);
      setDeleteDialogOpen(false);
      setPromoToDelete(null);
    }
  };

  const formatCurrency = (amount: number) => {
    if (isNaN(amount) || amount === null || amount === undefined) {
      return '0 ₫';
    }
    return formatVND(amount);
  };

  const formatDate = (date: Date | string) => {
    if (!date) return 'N/A';
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(dateObj.getTime())) {
      return 'N/A';
    }
    return new Intl.DateTimeFormat('vi-VN').format(dateObj);
  };

  const getStatusBadge = (promo: Promotion) => {
    if (promo.status === 'deleted') return <Badge variant="destructive">Hết hạn</Badge>;
    if (promo.isActive) return <Badge variant="default">Đang hoạt động</Badge>;
    return <Badge variant="secondary">Không hoạt động</Badge>;
  };

  const getDiscountDisplay = (promo: Promotion) => {
    const value = promo.discountPercent ?? 0;
    return `${value}%`;
  };

  // Statistics
  const activePromos = promotions?.filter(p => p.status === 'active').length || 0;
  const totalUsage = 0;

  // Pagination for promotions (client-side, consistent with other admin pages)
  const allPromos = promotions ?? [];
  const totalPages = Math.max(1, Math.ceil(allPromos.length / pageSize));
  const startIndex = (currentPage - 1) * pageSize;
  const pageItems = allPromos.slice(startIndex, startIndex + pageSize);

  return (
    <div className="space-y-6">
      {/* Statistics Cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Tổng khuyến mãi</CardTitle>
            <Tag className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{promotions?.length || 0}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Đang hoạt động</CardTitle>
            <Percent className="h-4 w-4 text-green-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{activePromos}</div>
          </CardContent>
        </Card>
        {/* Usage stats removed - promotions are simple validity-based */}
      </div>

      {/* Promotions Table */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý khuyến mãi</CardTitle>
              <CardDescription>Tạo và quản lý mã giảm giá</CardDescription>
            </div>
            <Button onClick={() => handleOpenDialog()}>
              <Plus className="h-4 w-4 mr-2" />
              Tạo mã mới
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Mã khuyến mãi</TableHead>
                  <TableHead>Giảm giá</TableHead>
                  <TableHead>Hiệu lực</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead>Đã xóa</TableHead>
                  <TableHead className="text-right">Hành động</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {!promotions || promotions.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                      Chưa có mã khuyến mãi nào
                    </TableCell>
                  </TableRow>
                ) : (
                  pageItems.map((promo) => (
                    <TableRow key={promo.id}>
                      <TableCell>
                        <Badge variant="outline" className="font-mono">
                          {promo.code}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Percent className="h-4 w-4 text-green-600" />
                          {getDiscountDisplay(promo)}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm">
                          <div>{formatDate(promo.startDate ?? '')}</div>
                          <div className="text-muted-foreground">→ {formatDate(promo.endDate ?? '')}</div>
                        </div>
                      </TableCell>
                      <TableCell>{getStatusBadge(promo)}</TableCell>
                      <TableCell>
                        {promo.isDelete ? (
                          <Badge variant="destructive">Đã xóa</Badge>
                        ) : (
                          <Badge variant="secondary">Chưa</Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex gap-2 justify-end">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleOpenDialog(promo)}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="destructive"
                            size="sm"
                            onClick={() => handleDelete(promo)}
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
            <div className="px-4 py-2">
              <PaginationControls
                totalItems={allPromos.length}
                currentPage={currentPage}
                totalPages={totalPages}
                pageSize={pageSize}
                onPageChange={(p) => setCurrentPage(p)}
                onPageSizeChange={(s) => { setPageSize(s); setCurrentPage(1); }}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editingPromotion ? 'Chỉnh sửa khuyến mãi' : 'Tạo mã khuyến mãi mới'}</DialogTitle>
            <DialogDescription>
              {editingPromotion ? 'Cập nhật thông tin mã khuyến mãi' : 'Nhập thông tin mã khuyến mãi'}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="code">Mã khuyến mãi *</Label>
              <Input
                id="code"
                value={formData.code}
                onChange={(e) => setFormData({ ...formData, code: e.target.value.toUpperCase() })}
                placeholder="VD: SUMMER2024"
                className="font-mono"
              />
            </div>
            {/* Description removed to match backend schema */}
            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-2">
                <Label htmlFor="discountPercent">Phần trăm giảm (%) *</Label>
                <Input
                  id="discountPercent"
                  type="number"
                  value={formData.discountPercent}
                  onChange={(e) => setFormData({ ...formData, discountPercent: e.target.value })}
                  placeholder="10"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="_spacer">&nbsp;</Label>
                <div />
              </div>
            </div>
            {/* minPurchase/maxDiscount not supported by BE schema; omitted */}
            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-2">
                <Label htmlFor="startDate">Ngày bắt đầu *</Label>
                <Input
                  id="startDate"
                  type="date"
                  min={todayStr}
                  value={formData.startDate}
                  onChange={(e) => setFormData({ ...formData, startDate: e.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="endDate">Ngày kết thúc *</Label>
                <Input
                  id="endDate"
                  type="date"
                  min={formData.startDate && formData.startDate > todayStr ? formData.startDate : todayStr}
                  value={formData.endDate}
                  onChange={(e) => setFormData({ ...formData, endDate: e.target.value })}
                />
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="status">Hiệu lực *</Label>
              <Select
                value={formData.status}
                onValueChange={(value: any) => { setFormData({ ...formData, status: (value as Promotion['status']) || 'inactive' }); }}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="active">Đang hoạt động</SelectItem>
                  <SelectItem value="inactive">Không hoạt động</SelectItem>
                  <SelectItem value="deleted">Hết hạn</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button
              onClick={handleSubmit}
              disabled={!formData.code || !formData.discountPercent || !formData.startDate || !formData.endDate}
            >
              {editingPromotion ? 'Cập nhật' : 'Tạo mã'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Xóa mã khuyến mãi</DialogTitle>
            <DialogDescription>
              Bạn có chắc muốn xóa mã khuyến mãi "{promoToDelete?.code}"? Hành động này không thể hoàn tác.
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
