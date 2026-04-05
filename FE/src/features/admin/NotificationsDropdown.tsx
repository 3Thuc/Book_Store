import React, { useState, useEffect, useCallback } from 'react';
import { Bell, Package, ShoppingCart, CheckCheck, X, Loader2, XCircle, Truck, RotateCcw } from 'lucide-react';
import { Button } from '../../components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '../../components/ui/dropdown-menu';
import { Badge } from '../../components/ui/badge';
import { ScrollArea } from '../../components/ui/scroll-area';
import { Separator } from '../../components/ui/separator';
import { webSocketService, Notification } from '../../services/websocketService';
import notificationService from '../../services/notificationService';
import { toast } from 'sonner';

interface NotificationsDropdownProps {
  onNavigateToOrders?: () => void;
  onNavigateToInventory?: () => void;
}

export const NotificationsDropdown: React.FC<NotificationsDropdownProps> = ({ 
  onNavigateToOrders,
  onNavigateToInventory 
}) => {
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  // Helper function to format time
  const formatTime = (timeString: string): string => {
    const date = new Date(timeString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Vừa xong';
    if (diffMins < 60) return `${diffMins} phút trước`;
    if (diffHours < 24) return `${diffHours} giờ trước`;
    return `${diffDays} ngày trước`;
  };

  // Handle incoming notification from WebSocket
  const handleNewNotification = useCallback((notification: Notification) => {
    // Format time before adding to state
    const formattedNotification = {
      ...notification,
      time: formatTime(notification.time),
    };
    setNotifications(prev => [formattedNotification, ...prev]);
    
    // Show toast notification
    toast.success(notification.title, {
      description: notification.message,
      duration: 5000,
    });

    // Play notification sound (optional)
    try {
      const audio = new Audio('/notification.mp3');
      audio.volume = 0.5;
      audio.play().catch(() => {
        // Ignore audio play errors
      });
    } catch (error) {
      // Ignore audio errors
    }
  }, []);

  // Load notifications from API and connect to WebSocket
  useEffect(() => {
    const loadNotifications = async () => {
      try {
        setIsLoading(true);
        // Load all notifications (both read and unread)
        const response = await notificationService.getAllNotifications(0, 50);
        
        // Convert time format for display
        const formattedNotifications = response.content.map((n: any) => ({
          ...n,
          time: formatTime(n.time),
        }));
        
        setNotifications(formattedNotifications);
      } catch (error) {
        console.error('Failed to load notifications:', error);
        toast.error('Không thể tải thông báo');
      } finally {
        setIsLoading(false);
      }
    };

    loadNotifications();

    // Connect to WebSocket
    webSocketService.connect(handleNewNotification);
    setIsConnected(true);

    // Cleanup on unmount
    return () => {
      webSocketService.disconnect();
      setIsConnected(false);
    };
  }, [handleNewNotification]);

  const unreadCount = notifications.filter(n => !n.isRead).length;

  const markAsRead = async (id: string) => {
    try {
      await notificationService.markAsRead(id);
      setNotifications(prev =>
        prev.map(n => (n.id === id ? { ...n, isRead: true } : n))
      );
    } catch (error) {
      console.error('Failed to mark as read:', error);
      toast.error('Không thể đánh dấu đã đọc');
    }
  };

  const markAllAsRead = async () => {
    try {
      await notificationService.markAllAsRead();
      setNotifications(prev => prev.map(n => ({ ...n, isRead: true })));
      toast.success('Đã đánh dấu tất cả là đã đọc');
    } catch (error) {
      console.error('Failed to mark all as read:', error);
      toast.error('Không thể đánh dấu tất cả');
    }
  };

  const deleteNotification = async (id: string) => {
    try {
      await notificationService.deleteNotification(id);
      setNotifications(prev => prev.filter(n => n.id !== id));
      toast.success('Đã xóa thông báo');
    } catch (error) {
      console.error('Failed to delete notification:', error);
      toast.error('Không thể xóa thông báo');
    }
  };

  const handleNotificationClick = (notification: Notification) => {
    markAsRead(notification.id);
    
    // Navigate based on notification type
    if (notification.type === 'low_stock' && onNavigateToInventory) {
      onNavigateToInventory();
      setIsOpen(false);
    } else if ((notification.type === 'new_order' || notification.type === 'order_completed' || notification.type === 'return_requested' || notification.type === 'order_cancelled' || notification.type === 'delivery_completed') && onNavigateToOrders) {
      onNavigateToOrders();
      setIsOpen(false);
    }
  };

  const getIcon = (type: Notification['type']) => {
    switch (type) {
      case 'new_order':
        return <ShoppingCart className="h-4 w-4" />;
      case 'low_stock':
        return <Package className="h-4 w-4" />;
      case 'order_completed':
        return <CheckCheck className="h-4 w-4" />;
      case 'return_requested':
        return <RotateCcw className="h-4 w-4" />;
      case 'order_cancelled':
        return <XCircle className="h-4 w-4" />;
      case 'delivery_completed':
        return <Truck className="h-4 w-4" />;
      default:
        return <Bell className="h-4 w-4" />;
    }
  };

  const getIconColor = (type: Notification['type']) => {
    switch (type) {
      case 'new_order':
        return 'bg-blue-100 text-blue-600';
      case 'low_stock':
        return 'bg-orange-100 text-orange-600';
      case 'order_completed':
        return 'bg-green-100 text-green-600';
      case 'return_requested':
        return 'bg-purple-100 text-purple-600';
      case 'order_cancelled':
        return 'bg-red-100 text-red-600';
      case 'delivery_completed':
        return 'bg-green-100 text-green-600';
      default:
        return 'bg-gray-100 text-gray-600';
    }
  };

  return (
    <DropdownMenu open={isOpen} onOpenChange={setIsOpen}>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" className="relative hover:bg-accent">
          <Bell className="h-5 w-5 text-muted-foreground" />
          {unreadCount > 0 && (
            <Badge 
              variant="destructive" 
              className="absolute -top-1 -right-1 h-5 w-5 flex items-center justify-center p-0 text-xs"
            >
              {unreadCount > 9 ? '9+' : unreadCount}
            </Badge>
          )}
          {isConnected && (
            <span className="absolute bottom-0 right-0 h-2 w-2 rounded-full bg-green-500 border-2 border-background"></span>
          )}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-[380px] p-0">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b">
          <div>
            <h3 className="font-semibold">Thông báo</h3>
            {unreadCount > 0 && (
              <p className="text-xs text-muted-foreground">
                Bạn có {unreadCount} thông báo chưa đọc
              </p>
            )}
          </div>
          {unreadCount > 0 && (
            <Button
              variant="ghost"
              size="sm"
              onClick={markAllAsRead}
              className="h-8 text-xs"
            >
              Đánh dấu tất cả đã đọc
            </Button>
          )}
        </div>

        {/* Notifications List */}
        <ScrollArea className="h-[400px]">
          {isLoading ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Loader2 className="h-12 w-12 text-muted-foreground/40 mb-3 animate-spin" />
              <p className="text-sm text-muted-foreground">Đang tải thông báo...</p>
            </div>
          ) : notifications.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Bell className="h-12 w-12 text-muted-foreground/40 mb-3" />
              <p className="text-sm text-muted-foreground">Chưa có thông báo nào</p>
            </div>
          ) : (
            <div className="divide-y">
              {notifications.map((notification) => (
                <div
                  key={notification.id}
                  className={`
                    group relative p-4 transition-colors cursor-pointer
                    ${notification.isRead ? 'bg-background' : 'bg-accent/50'}
                    hover:bg-accent
                  `}
                  onClick={() => handleNotificationClick(notification)}
                >
                  <div className="flex gap-3">
                    {/* Icon */}
                    <div className={`flex-shrink-0 h-10 w-10 rounded-full flex items-center justify-center ${getIconColor(notification.type)}`}>
                      {getIcon(notification.type)}
                    </div>

                    {/* Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between gap-2 mb-1">
                        <p className="font-semibold text-sm">{notification.title}</p>
                        {!notification.isRead && (
                          <div className="flex-shrink-0 h-2 w-2 rounded-full bg-blue-600 mt-1"></div>
                        )}
                      </div>
                      <p className="text-sm text-muted-foreground line-clamp-2 mb-1">
                        {notification.message}
                      </p>
                      <p className="text-xs text-muted-foreground">{notification.time}</p>
                    </div>

                    {/* Delete button */}
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
                      onClick={(e) => {
                        e.stopPropagation();
                        deleteNotification(notification.id);
                      }}
                    >
                      <X className="h-3 w-3" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </ScrollArea>

        {/* Footer */}
        {notifications.length > 0 && (
          <>
            <Separator />
            <div className="p-2">
              <Button
                variant="ghost"
                className="w-full"
                size="sm"
                onClick={() => {
                  if (onNavigateToOrders) {
                    onNavigateToOrders();
                    setIsOpen(false);
                  }
                }}
              >
                Xem tất cả đơn hàng
              </Button>
            </div>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
};
