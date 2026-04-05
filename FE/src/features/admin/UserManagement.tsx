import React, { useState } from 'react';
import { useAdmin, User } from './AdminContext';
import { useAuth } from '../../context/AuthContext';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/card';
import { Shield, Lock, Unlock, Search, Users, Key } from 'lucide-react';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { toast } from 'sonner';
import adminService from '../../services/adminService';
import { formatVND } from '../../lib/formatters';
import PaginationControls from '../../components/admin/PaginationControls';
import { useEffect } from 'react';

export const UserManagement: React.FC = () => {
  const { users, updateUserStatus, updateUserRole, deleteUser, createUser } = useAdmin();
  const { user: currentUser } = useAuth();
  
  // DEBUG: Log key values
  console.log('[UserManagement] users from context:', users);
  console.log('[UserManagement] users.length:', users.length);
  console.log('[UserManagement] currentUser:', currentUser);
  
  const [searchTerm, setSearchTerm] = useState('');
  const [filterRole, setFilterRole] = useState<'all' | 'admin' | 'staff' | 'customer'>('all');
  const [filterStatus, setFilterStatus] = useState<'all' | 'active' | 'locked' | 'unverified'>('all');
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogAction, setDialogAction] = useState<'lock' | 'unlock' | 'role'>('lock');
  const [selectedRole, setSelectedRole] = useState<User['role']>('customer');
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [newEmail, setNewEmail] = useState('');
  const [newFullName, setNewFullName] = useState('');
  const [newRole, setNewRole] = useState<User['role']>('customer');

  // Filter users
  const filteredUsers = users.filter(user => {
    const matchesSearch = user.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      user.email.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesRole = filterRole === 'all' || user.role === filterRole;
    const matchesStatus = filterStatus === 'all' || user.status === filterStatus;
    return matchesSearch && matchesRole && matchesStatus;
  });

  type Role = 'admin' | 'staff' | 'customer';

  const currentRole: Role =
    currentUser && ['admin', 'staff', 'customer'].includes(currentUser.role)
      ? (currentUser.role as Role)
      : 'customer';

  const visibleUsers = filteredUsers.filter(u => {
    if (currentRole === 'admin') return true;
    if (currentRole === 'staff') return u.role !== 'admin';
    return u.id === (currentUser?.id ?? '');
  });

  // DEBUG: Log filtering results
  console.log('[UserManagement] currentRole:', currentRole);
  console.log('[UserManagement] filteredUsers:', filteredUsers);
  console.log('[UserManagement] filteredUsers.length:', filteredUsers.length);
  console.log('[UserManagement] visibleUsers:', visibleUsers);
  console.log('[UserManagement] visibleUsers.length:', visibleUsers.length);

  // Pagination state
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);

  const totalItems = visibleUsers.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));

  useEffect(() => {
    // When filters/search/users change, reset to first page
    setCurrentPage(1);
  }, [searchTerm, filterRole, filterStatus, users.length]);

  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages);
  }, [totalPages]);

  const pagedUsers = visibleUsers.slice((currentPage - 1) * pageSize, (currentPage - 1) * pageSize + pageSize);

  const handleAction = (user: User, action: 'lock' | 'unlock' | 'role') => {
    setSelectedUser(user);
    setDialogAction(action);
    if (action === 'role') setSelectedRole(user.role);
    setDialogOpen(true);
  };

  const confirmAction = () => {
    if (!selectedUser) return;

    if (dialogAction === 'lock') {
      updateUserStatus(selectedUser.id, 'locked');
    } else if (dialogAction === 'unlock') {
      updateUserStatus(selectedUser.id, 'active');
    } else if (dialogAction === 'role') {
      updateUserRole(selectedUser.id, selectedRole);
    }

    setDialogOpen(false);
    setSelectedUser(null);
  };

  const formatCurrency = (amount: number) => {
    return formatVND(amount);
  };

  const formatDate = (date?: Date | string | null) => {
    if (!date) return 'N/A';

    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (!dateObj || isNaN(dateObj.getTime())) {
      return 'N/A';
    }
    return new Intl.DateTimeFormat('vi-VN').format(dateObj);
  };

  // Statistics
  const totalUsers = users.length;
  const activeUsers = users.filter(u => u.status === 'active').length;
  const adminUsers = users.filter(u => u.role === 'admin').length;
  const unverifiedUsers = users.filter(u => u.status === 'unverified').length;

  return (
    <div className="space-y-6">
      {/* Statistics Cards */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Tổng người dùng</CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{totalUsers}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Đang hoạt động</CardTitle>
            <Unlock className="h-4 w-4 text-green-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{activeUsers}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Chưa xác thực</CardTitle>
            <Users className="h-4 w-4 text-yellow-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{unverifiedUsers}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Quản trị viên</CardTitle>
            <Shield className="h-4 w-4 text-blue-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl">{adminUsers}</div>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Quản lý người dùng</CardTitle>
          <CardDescription>Quản lý tài khoản, phân quyền và khóa người dùng</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4 mb-6">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Tìm kiếm theo tên hoặc email..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
            <Select value={filterRole} onValueChange={(value: any) => setFilterRole(value)}>
              <SelectTrigger className="w-full md:w-[180px]">
                <SelectValue placeholder="Vai trò" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tất cả vai trò</SelectItem>
                <SelectItem value="admin">Quản trị viên</SelectItem>
                <SelectItem value="staff">Nhân viên</SelectItem>
                <SelectItem value="customer">Khách hàng</SelectItem>
              </SelectContent>
            </Select>
            <Select value={filterStatus} onValueChange={(value: any) => setFilterStatus(value)}>
              <SelectTrigger className="w-full md:w-[180px]">
                <SelectValue placeholder="Trạng thái" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tất cả trạng thái</SelectItem>
                <SelectItem value="active">Hoạt động</SelectItem>
                <SelectItem value="locked">Đã khóa</SelectItem>
                <SelectItem value="unverified">Chưa xác thực</SelectItem>
              </SelectContent>
            </Select>
            {currentRole === 'admin' && (
              <div className="ml-auto">
                <Button onClick={() => setCreateDialogOpen(true)}>
                  Thêm người dùng
                </Button>
              </div>
            )}
          </div>

          {/* Users Table */}
          <div id="user-management-table" className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Người dùng</TableHead>
                  <TableHead>Vai trò</TableHead>
                  <TableHead>Trạng thái</TableHead>
                  <TableHead>Ngày đăng ký</TableHead>
                  <TableHead className="text-right">Hành động</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pagedUsers.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                      Không tìm thấy người dùng
                    </TableCell>
                  </TableRow>
                ) : (
                  pagedUsers.map((user) => (
                    <TableRow key={user.id}>
                      <TableCell>
                        <div>
                          <div>{user.name}</div>
                          <div className="text-sm text-muted-foreground">{user.email}</div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant={user.role === 'admin' ? 'default' : (user.role === 'staff' ? 'outline' : 'secondary')}>
                          {user.role === 'admin' ? (
                            <span className="flex items-center gap-1">
                              <Shield className="h-3 w-3" />
                              Admin
                            </span>
                          ) : user.role === 'staff' ? (
                            'Nhân viên'
                          ) : (
                            'Khách hàng'
                          )}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant={
                          user.status === 'active' ? 'default' : 
                          user.status === 'unverified' ? 'secondary' : 'destructive'
                        }>
                          {user.status === 'active' ? 'Hoạt động' : 
                           user.status === 'unverified' ? 'Chưa xác thực' : 'Đã khóa'}
                        </Badge>
                      </TableCell>
                      <TableCell>{formatDate(user.createdAt)}</TableCell>
                      <TableCell className="text-right">
                        <div className="flex gap-2 justify-end">
                          {(() => {
                            const canModify = currentRole === 'admin' || (currentRole === 'staff' && user.role === 'customer');
                            // Disable modification for unverified users - they cannot be locked/unlocked
                            const canModifyStatus = canModify && user.status !== 'unverified';
                            
                            const lockBtn = user.status === 'active' ? (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleAction(user, 'lock')}
                                disabled={!canModifyStatus}
                                title={canModifyStatus ? 'Khóa tài khoản' : 'Bạn không có quyền'}
                                aria-label="Khóa tài khoản"
                              >
                                <Lock className="h-4 w-4" />
                              </Button>
                            ) : user.status === 'locked' ? (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleAction(user, 'unlock')}
                                disabled={!canModifyStatus}
                                title="Mở khóa tài khoản"
                                aria-label="Mở khóa tài khoản"
                              >
                                <Unlock className="h-4 w-4" />
                              </Button>
                            ) : (
                              // For unverified status, show disabled lock button
                              <Button
                                variant="outline"
                                size="sm"
                                disabled={true}
                                title="Tài khoản chưa xác thực - không thể thay đổi"
                                aria-label="Không thể thay đổi"
                              >
                                <Lock className="h-4 w-4" />
                              </Button>
                            );

                            const canReset = canModify && user.status !== 'unverified';

                            const roleBtn = (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleAction(user, 'role')}
                                disabled={!canModify}
                                title={user.role === 'admin' ? 'Hủy Admin' : 'Cấp quyền'}
                                aria-label={user.role === 'admin' ? 'Hủy Admin' : 'Cấp quyền'}
                              >
                                <Shield className="h-4 w-4" />
                              </Button>
                            );

                            const resetBtn = (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={async () => {
                                  try {
                                    await adminService.resetUserPassword(user.id);
                                    toast.success('Yêu cầu đặt lại mật khẩu đã được thực hiện');
                                  } catch (err: any) {
                                    console.error('Failed to request admin password reset', err);
                                    toast.error(err?.message ?? 'Thực hiện đặt lại mật khẩu thất bại');
                                  }
                                }}
                                disabled={!canReset}
                                title={user.status === 'unverified' ? 'Không thể đặt lại mật khẩu cho tài khoản chưa xác thực' : 'Đặt lại mật khẩu'}
                                aria-label="Đặt lại mật khẩu"
                              >
                                <Key className="h-4 w-4" />
                              </Button>
                            );

                            return (
                              <>
                                {lockBtn}
                                {resetBtn}
                                {roleBtn}
                              </>
                            );
                          })()}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>

          <PaginationControls
            totalItems={totalItems}
            currentPage={currentPage}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={(p) => setCurrentPage(p)}
            onPageSizeChange={(s) => { setPageSize(s); setCurrentPage(1); }}
            pageSizeOptions={[5, 10, 15, 20]}
            containerId="user-management-table"
          />
        </CardContent>
      </Card>

        {/* Create User Dialog */}
        <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Thêm người dùng mới</DialogTitle>
              <DialogDescription>Nhập email Gmail thực để hệ thống gửi mật khẩu tạm thời cho người dùng.</DialogDescription>
            </DialogHeader>
            <div className="px-6 pb-4 space-y-3">
              <div>
                <label className="block text-sm mb-1">Họ và tên</label>
                <Input value={newFullName} onChange={(e) => setNewFullName(e.target.value)} placeholder="Tên đầy đủ" />
              </div>
              <div>
                <label className="block text-sm mb-1">Email (chỉ Gmail)</label>
                <Input value={newEmail} onChange={(e) => setNewEmail(e.target.value)} placeholder="example@gmail.com" />
              </div>
              <div>
                <label className="block text-sm mb-1">Vai trò</label>
                <Select value={newRole} onValueChange={(v: any) => setNewRole(v)}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Chọn vai trò" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="customer">Khách hàng</SelectItem>
                    <SelectItem value="staff">Nhân viên</SelectItem>
                    <SelectItem value="admin">Quản trị viên</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>Hủy</Button>
              <Button onClick={async () => {
                const email = (newEmail || '').trim();
                if (!email || !email.toLowerCase().endsWith('@gmail.com')) {
                  toast.error('Vui lòng nhập địa chỉ Gmail hợp lệ');
                  return;
                }
                try {
                  await createUser({ email, fullName: newFullName, role: newRole });
                  setCreateDialogOpen(false);
                  setNewEmail('');
                  setNewFullName('');
                  setNewRole('customer');
                } catch (err) {
                  // createUser already shows toast for failures
                }
              }}>Tạo</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

      {/* Confirmation Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {dialogAction === 'lock' && 'Khóa tài khoản'}
              {dialogAction === 'unlock' && 'Mở khóa tài khoản'}
              {dialogAction === 'role' && 'Thay đổi vai trò'}
            </DialogTitle>
            <DialogDescription>
              {dialogAction === 'lock' && `Bạn có chắc muốn khóa tài khoản "${selectedUser?.name}"? Người dùng sẽ không thể đăng nhập.`}
              {dialogAction === 'unlock' && `Bạn có chắc muốn mở khóa tài khoản "${selectedUser?.name}"?`}
              {dialogAction === 'role' && (
                <>
                  <div className="mb-2">Chọn vai trò mới cho "{selectedUser?.name}"</div>
                </>
              )}
            </DialogDescription>
          </DialogHeader>
          {dialogAction === 'role' && (
            <div className="px-6 pb-4">
              {/* Determine allowed roles based on current user */}
              {(() => {
                const currentRoleLocal = (currentUser && (currentUser.role as any)) ? (currentUser.role as 'admin' | 'staff' | 'customer') : 'customer';
                let allowed: User['role'][] = ['customer'];
                if (currentRoleLocal === 'admin') allowed = ['admin', 'staff', 'customer'];
                else if (currentRoleLocal === 'staff') allowed = ['staff', 'customer'];
                return (
                  <Select value={selectedRole} onValueChange={(v: any) => setSelectedRole(v)}>
                    <SelectTrigger className="w-full">
                      <SelectValue placeholder="Chọn vai trò" />
                    </SelectTrigger>
                    <SelectContent>
                      {allowed.map(r => (
                        <SelectItem key={r} value={r}>{r === 'admin' ? 'Admin' : (r === 'staff' ? 'Nhân viên' : 'Khách hàng')}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                );
              })()}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Hủy
            </Button>
            <Button onClick={confirmAction}>
              Xác nhận
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
