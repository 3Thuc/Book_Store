import { Client, IMessage } from '@stomp/stompjs';
import SockJS from 'sockjs-client';

export interface Notification {
  id: string;
  type: 'new_order' | 'low_stock' | 'order_completed' | 'return_requested' | 'order_cancelled' | 'delivery_completed';
  title: string;
  message: string;
  time: string;
  isRead: boolean;
  orderId?: string;
  userName?: string;
}

class WebSocketService {
  private client: Client | null = null;
  private connected: boolean = false;
  private reconnectAttempts: number = 0;
  private maxReconnectAttempts: number = 5;
  private reconnectDelay: number = 3000;

  constructor(private baseUrl: string = 'https://localhost:8443') {}

  connect(onNotification: (notification: Notification) => void): void {
    if (this.connected) {
      console.log('WebSocket already connected');
      return;
    }

    this.client = new Client({
      webSocketFactory: () => new SockJS(`${this.baseUrl}/bookdb/ws`),
      debug: (str) => {
        console.log('STOMP Debug:', str);
      },
      reconnectDelay: this.reconnectDelay,
      heartbeatIncoming: 4000,
      heartbeatOutgoing: 4000,
      onConnect: () => {
        console.log('Connected to WebSocket');
        this.connected = true;
        this.reconnectAttempts = 0;

        // Subscribe to admin notifications
        this.client?.subscribe('/topic/admin-notifications', (message: IMessage) => {
          try {
            const notification = JSON.parse(message.body);
            // Convert LocalDateTime to readable format
            if (notification.time) {
              const date = new Date(notification.time);
              notification.time = this.getRelativeTime(date);
            }
            onNotification(notification);
          } catch (error) {
            console.error('Error parsing notification:', error);
          }
        });
      },
      onDisconnect: () => {
        console.log('Disconnected from WebSocket');
        this.connected = false;
      },
      onStompError: (frame) => {
        console.error('STOMP error:', frame.headers['message']);
        console.error('Details:', frame.body);
        this.handleReconnect(onNotification);
      },
      onWebSocketError: (error) => {
        console.error('WebSocket error:', error);
        this.handleReconnect(onNotification);
      },
    });

    this.client.activate();
  }

  private handleReconnect(onNotification: (notification: Notification) => void): void {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(`Attempting to reconnect... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
      setTimeout(() => {
        this.connect(onNotification);
      }, this.reconnectDelay * this.reconnectAttempts);
    } else {
      console.error('Max reconnection attempts reached');
    }
  }

  disconnect(): void {
    if (this.client) {
      this.client.deactivate();
      this.connected = false;
      console.log('WebSocket disconnected');
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  private getRelativeTime(date: Date): string {
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Vừa xong';
    if (diffMins < 60) return `${diffMins} phút trước`;
    if (diffHours < 24) return `${diffHours} giờ trước`;
    return `${diffDays} ngày trước`;
  }
}

// Export singleton instance
export const webSocketService = new WebSocketService(
  (import.meta as any).env.VITE_API_URL || 'https://localhost:8443'
);
