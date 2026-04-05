import React, { useState } from 'react';
import { useAdmin, InventoryItem } from './AdminContext';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Package, AlertTriangle, Search, TrendingUp, TrendingDown } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';

export const InventoryManagement: React.FC = () => {
  const { inventory, books, updateInventory, updateStock } = useAdmin();
  const [searchTerm, setSearchTerm] = useState('');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState<InventoryItem | null>(null);
  const [newStock, setNewStock] = useState('');
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);

  const handleUpdateStock = (item: InventoryItem) => {
    setSelectedItem(item);
    setNewStock(String(item.stock ?? item.quantity ?? 0));
    setDialogOpen(true);
  };

  // Safe accessors with fallbacks to avoid `possibly undefined` errors
  const getStock = (i: InventoryItem | null | undefined) => Number(i?.stock ?? i?.quantity ?? 0);
  const getReserved = (i: InventoryItem | null | undefined) => Number(i?.reserved ?? i?.orderedQuantity ?? 0);
  const getAvailable = (i: InventoryItem | null | undefined) => {
    if (!i) return 0;
    return Number(i.available ?? i.availableQuantity ?? Math.max(0, getStock(i) - getReserved(i)));
  };
  const getThreshold = (i: InventoryItem | null | undefined) => Number(i?.lowStockThreshold ?? i?.threshold ?? i?.reorderLevel ?? 0);

  const confirmUpdate = () => {
    if (selectedItem) {
      // Use server-backed update (optimistic + refresh) when available
      if (updateStock) {
        updateStock(selectedItem.bookId, { stockQuantity: parseInt(newStock) });
      } else {
        updateInventory(selectedItem.bookId, parseInt(newStock));
      }
      setDialogOpen(false);
      setSelectedItem(null);
      setNewStock('');
    }
  };

  // Get book details for inventory item
  const getBookDetails = (bookId: string) => {
    return books.find(b => String(b.bookId) === String(bookId));
  };

  // Filter inventory
    const filteredInventory = inventory.filter(item => {
      const book = getBookDetails(item.bookId);
      if (!book) return false;
      const titleMatch = (book.title ?? '').toLowerCase().includes(searchTerm.toLowerCase());
      const authorMatch = (book.author ?? '').toLowerCase().includes(searchTerm.toLowerCase());
      return titleMatch || authorMatch;
    });

  // Pagination
  const totalPages = Math.max(1, Math.ceil(filteredInventory.length / pageSize));
  const handlePageChange = (page: number) => {
    const p = Math.max(1, Math.min(page, totalPages));
    setCurrentPage(p);
  };

  const paginatedInventory = filteredInventory.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  // Statistics
  const lowStockItems = inventory.filter(i => getAvailable(i) <= getThreshold(i)).length;
  const outOfStockItems = inventory.filter(i => getAvailable(i) === 0).length;
  const totalStock = inventory.reduce((sum, i) => sum + getStock(i), 0);
  const totalAvailable = inventory.reduce((sum, i) => sum + getAvailable(i), 0);

  const getStockStatus = (item: InventoryItem) => {
    const availableVal = getAvailable(item);
    const thresholdVal = getThreshold(item);
    if (availableVal === 0) {
      return <Badge variant="destructive">Hết hàng</Badge>;
    } else if (availableVal <= thresholdVal) {
      return <Badge variant="secondary" className="bg-yellow-100 text-yellow-800">Sắp hết</Badge>;
    } else {
      return <Badge variant="default">Đủ hàng</Badge>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Statistics Cards */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Tổng tồn kho</CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{totalStock}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Có sẵn: {totalAvailable}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Hết hàng</CardTitle>
            <TrendingDown className="h-4 w-4 text-red-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{outOfStockItems}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Cần nhập hàng
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Sắp hết hàng</CardTitle>
            <AlertTriangle className="h-4 w-4 text-yellow-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{lowStockItems}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Dưới ngưỡng tối thiểu
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm">Sản phẩm</CardTitle>
            <TrendingUp className="h-4 w-4 text-green-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{inventory.length}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Tổng số SKU
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Inventory Table */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Quản lý kho</CardTitle>
              <CardDescription>Theo dõi và cập nhật số lượng tồn kho</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {/* Search */}
          <div className="mb-4 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Tìm kiếm sách..."
              value={searchTerm}
              onChange={(e) => { setSearchTerm(e.target.value); setCurrentPage(1); }}
              className="pl-9"
            />
          </div>

          

          {/* Table */}
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Sách</TableHead>
                  <TableHead>Tồn kho</TableHead>
                  <TableHead>Đã đặt</TableHead>
                  <TableHead>Có sẵn</TableHead>
                  <TableHead>Ngưỡng</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead className="text-right">Hành động</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredInventory.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                      Không tìm thấy sản phẩm
                    </TableCell>
                  </TableRow>
                ) : (
                  paginatedInventory.map((item) => {
                    const book = getBookDetails(item.bookId);
                    if (!book) return null;

                    return (
                      <TableRow key={item.bookId}>
                        <TableCell>
                          <div className="flex items-center gap-3">
                            <ImageWithFallback
                              src={(book as any).images ?? (book as any).imageUrl ?? ''}
                              alt={book.title}
                              className="w-12 h-16 object-cover rounded"
                            />
                            <div>
                              <div className="max-w-[200px] truncate">{book.title}</div>
                              <div className="text-sm text-muted-foreground">{book.author}</div>
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Package className="h-4 w-4 text-muted-foreground" />
                            {getStock(item)}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant="secondary">{getReserved(item)}</Badge>
                        </TableCell>
                        <TableCell>
                          <span className={getAvailable(item) <= getThreshold(item) ? 'text-red-600' : ''}>
                            {getAvailable(item)}
                          </span>
                        </TableCell>
                        <TableCell>{getThreshold(item)}</TableCell>
                        <TableCell>{getStockStatus(item)}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleUpdateStock(item)}
                          >
                            Cập nhật
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })
                )}
              </TableBody>
            </Table>
          </div>
          <PaginationControls
            totalItems={filteredInventory.length}
            currentPage={currentPage}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={(p) => setCurrentPage(p)}
            onPageSizeChange={(s) => { setPageSize(s); setCurrentPage(1); }}
            loading={false}
            pageSizeOptions={[5, 10, 15, 20]}
          />
        </CardContent>
      </Card>

      {/* Update Stock Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
                  <DialogContent>
          <DialogHeader>
            <DialogTitle>Cập nhật tồn kho</DialogTitle>
            <DialogDescription>
              {selectedItem ? getBookDetails(selectedItem.bookId)?.title || 'Cập nhật số lượng tồn kho cho sản phẩm' : 'Cập nhật số lượng tồn kho cho sản phẩm'}
            </DialogDescription>
          </DialogHeader>
          {selectedItem && (
            <div className="grid gap-4 py-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-sm text-muted-foreground">Tồn kho hiện tại</p>
                  <p className="text-2xl">{getStock(selectedItem)}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Có sẵn</p>
                  <p className="text-2xl">{getAvailable(selectedItem)}</p>
                </div>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="newStock">Số lượng tồn kho mới *</Label>
                <Input
                  id="newStock"
                  type="number"
                  min="0"
                  value={newStock}
                  onChange={(e) => setNewStock(e.target.value)}
                  placeholder="Nhập số lượng"
                />
                <p className="text-sm text-muted-foreground">
                  Đã đặt: {getReserved(selectedItem)} | Có sẵn sẽ là: {Math.max(0, parseInt(newStock || '0') - getReserved(selectedItem))}
                </p>
              </div>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button 
              onClick={confirmUpdate}
              disabled={!newStock || parseInt(newStock) < 0}
            >
              Cập nhật
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
