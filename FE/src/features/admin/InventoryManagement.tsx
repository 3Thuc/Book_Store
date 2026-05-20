import React, { useState, useEffect, useMemo } from 'react';
import { useAdmin, InventoryItem } from './AdminContext';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Package, AlertTriangle, Search, TrendingUp, TrendingDown, Loader2 } from 'lucide-react';
import PaginationControls from '../../components/admin/PaginationControls';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';

export const InventoryManagement: React.FC = () => {
  const { inventory, books, updateInventory, updateStock, loadInventory, reloadInventory, isInventoryLoading } = useAdmin();
  const [searchTerm, setSearchTerm] = useState('');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState<InventoryItem | null>(null);
  const [newStock, setNewStock] = useState('');
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);

  // Smart load:
  // - Nếu inventory đã có data (từ localStorage hoặc fetch trước): dùng cache, không gọi API lại.
  // - Nếu inventory trống (chưa load, lỗi server, hoặc server restart): buộc fetch lại ngay.
  useEffect(() => {
    if (inventory.length === 0) {
      reloadInventory(); // force, bypass ref guards
    } else {
      loadInventory();   // no-op if inventoryLoadedRef = true (cache hit)
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps


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

  // O(1) book lookup — build once, tham chiếu nhiều lần.
  // Trước: mỗi row gọi books.find() = O(N) → 500×500 = 250,000 so sánh/render.
  // Sau:  mỗi row gọi bookMap.get() = O(1) → 500 lookups/render.
  const bookMap = useMemo(
    () => new Map(books.map(b => [String(b.bookId), b])),
    [books]
  );

  const getBookDetails = (bookId: string) => bookMap.get(String(bookId));

  // Mênh giá filteredInventory chỉ khi inventory/searchTerm thay đổi —
  // không tính lại khi dialog mở/đóng, chọn item, v.v.
  const filteredInventory = useMemo(() => {
    if (!searchTerm.trim()) return inventory;
    const lower = searchTerm.toLowerCase();
    return inventory.filter(item => {
      // Ưu tiên bookTitle từ inventory item (luôn có), bổ sung author từ bookMap (nếu có)
      const title = (item as any).bookTitle ?? bookMap.get(String(item.bookId))?.title ?? '';
      const author = bookMap.get(String(item.bookId))?.author ?? '';
      return title.toLowerCase().includes(lower)
          || author.toLowerCase().includes(lower);
    });
  }, [inventory, searchTerm, bookMap]);

  // Thống kê kho — chỉ tính lại khi inventory thay đổi
  const inventoryStats = useMemo(() => ({
    lowStockItems:  inventory.filter(i => getAvailable(i) > 0 && getAvailable(i) <= getThreshold(i)).length,
    outOfStockItems: inventory.filter(i => getAvailable(i) <= 0).length,
    totalStock:     inventory.reduce((s, i) => s + getStock(i), 0),
    totalAvailable: inventory.reduce((s, i) => s + getAvailable(i), 0),
  }), [inventory]);


  // Pagination
  const totalPages = Math.max(1, Math.ceil(filteredInventory.length / pageSize));
  const handlePageChange = (page: number) => {
    const p = Math.max(1, Math.min(page, totalPages));
    setCurrentPage(p);
  };

  const paginatedInventory = filteredInventory.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  const { lowStockItems, outOfStockItems, totalStock, totalAvailable } = inventoryStats;

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
                {isInventoryLoading ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-12">
                      <div className="flex items-center justify-center gap-2 text-muted-foreground">
                        <Loader2 className="h-5 w-5 animate-spin" />
                        <span>Đang tải dữ liệu kho...</span>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : filteredInventory.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-10 text-muted-foreground">
                      {searchTerm ? 'Không tìm thấy sách phù hợp' : 'Không có sản phẩm nào'}
                    </TableCell>
                  </TableRow>
                ) : (
                  paginatedInventory.map((item) => {
                    // Ưu tiên imageUrl từ API inventory (backend đã trả về proxy path)
                    // Fallback: tìm trong books context nếu có
                    const book = getBookDetails(item.bookId);
                    const displayTitle = (item as any).bookTitle ?? book?.title ?? '';
                    const displayAuthor = book?.author ?? '';
                    const displayImage = (item as any).imageUrl
                      ?? (book as any)?.imageUrl
                      ?? (book as any)?.images
                      ?? '';

                    return (
                      <TableRow key={item.bookId}>
                        <TableCell>
                          <div className="flex items-center gap-3">
                            <ImageWithFallback
                              src={displayImage}
                              alt={displayTitle}
                              className="w-12 h-16 object-cover rounded"
                            />
                            <div>
                              <div className="max-w-[200px] truncate">{displayTitle}</div>
                              <div className="text-sm text-muted-foreground">{displayAuthor}</div>
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
