import { useNavigate, useSearchParams } from 'react-router-dom';
import { XCircle } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';

/**
 * Payment Cancel Page
 * Handles cancelled payment callback from PayOS
 */
export const PaymentCancelPage = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  // Support both PayOS format (orderCode) and backend format (orderId)
  const orderId = searchParams.get('orderId') || searchParams.get('orderCode');

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <Card className="w-full max-w-md mx-4">
        <CardHeader className="text-center">
          <XCircle className="w-16 h-16 mx-auto mb-4 text-orange-500" />
          <CardTitle className="text-2xl">Đã hủy thanh toán</CardTitle>
          {orderId && (
            <CardDescription>
              Mã đơn hàng: <span className="font-semibold">#{orderId}</span>
            </CardDescription>
          )}
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-center text-gray-600">
            Bạn đã hủy giao dịch thanh toán. Giỏ hàng vẫn được giữ nguyên, bạn có thể quay lại thanh toán hoặc tiếp tục mua sắm.
          </p>
          <div className="flex gap-3">
            <Button
              variant="outline"
              className="flex-1"
              onClick={() => navigate('/')}
            >
              Về trang chủ
            </Button>
            <Button
              className="flex-1"
              onClick={() => navigate('/checkout')}
            >
              Quay lại thanh toán
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};
