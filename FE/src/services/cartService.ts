import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';

export interface CartItemPayload {
    bookId: string;
    quantity: number;
}

export interface UpdateCartItemPayload {
    quantity: number;
}

const cartService = {
    // Get user's cart
    getCart: () => apiRequest.get(API_ENDPOINTS.CART.GET),

    // Add item to cart (silent mode to handle toast in CartContext)
    addItem: (payload: CartItemPayload) =>
        apiRequest.post(API_ENDPOINTS.CART.ADD, payload, { headers: { 'X-SILENT': 'true' } }),

    // Update cart item quantity (silent mode)
    updateItem: (itemId: string, payload: UpdateCartItemPayload) =>
        apiRequest.put(API_ENDPOINTS.CART.UPDATE(itemId), payload, { headers: { 'X-SILENT': 'true' } }),

    // Remove item from cart (silent mode)
    removeItem: (itemId: string) =>
        apiRequest.delete(API_ENDPOINTS.CART.REMOVE(itemId), { headers: { 'X-SILENT': 'true' } }),

    // Clear cart (silent mode)
    clearCart: () =>
        apiRequest.delete(API_ENDPOINTS.CART.CLEAR, { headers: { 'X-SILENT': 'true' } }),

    // Get cart item count
    getCartCount: () => apiRequest.get(API_ENDPOINTS.CART.COUNT),
};

export default cartService;
