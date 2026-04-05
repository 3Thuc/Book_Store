import React, { createContext, useContext, useState, useEffect, ReactNode, useRef } from 'react';
import { User, Address, CreateAddressData, UpdateAddressData } from '../types/user';
import { AuthService, UserService } from '../services';
import { setAuthToken, getAuthToken } from '../lib/api-client';
import { toast } from 'sonner';
interface AuthContextType {
  user: User | null;
  isLoggedIn: boolean;
  initializing: boolean;
  addresses: Address[];
  orders: any[];
  isLoadingOrders: boolean;
  login: (email: string, password: string) => Promise<boolean>;
  signup: (fullName: string, email: string, password: string) => Promise<boolean>;
  logout: () => void;
  changePass: (newPassword: string, currentPassword: string) => Promise<boolean>;
  updateProfile: (data: Partial<User>) => Promise<boolean>;
  addAddress: (data: CreateAddressData) => Promise<boolean>;
  updateAddress: (data: UpdateAddressData) => Promise<boolean>;
  deleteAddress: (addressId: string) => Promise<boolean>;
  setDefaultAddress: (addressId: string) => Promise<boolean>;
  getDefaultAddress: () => Address | null;
  refreshOrders: () => Promise<void>;
  getOrderDetail: (orderId: string) => Promise<any>;
  loginWithGoogle: (code: string) => Promise<boolean>;
  redirectToGoogle: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [addresses, setAddresses] = useState<Address[]>([]);
  const [orders, setOrders] = useState<any[]>([]);
  const [isLoadingOrders, setIsLoadingOrders] = useState<boolean>(false);
  const [initializing, setInitializing] = useState<boolean>(true);

  // Persist only minimal user info to reduce PII in localStorage
  const minimalUserForStorage = (u: Partial<User> | null) => {
    if (!u) return null;
    return {
      id: u.id || (u as any).userId || '',
      userName: u.userName ?? '',
      fullName: u.fullName ?? '',
      role: (u.role as any) ?? 'customer'
    } as Partial<User>;
  };


  const mountedRef = useRef(true);


  const loadCurrentUser = async (opts?: { silent?: boolean }) => {
    const silent = opts?.silent ?? false;
    try {
      const resp = silent ? await AuthService.getCurrentUserSilent() : await AuthService.getCurrentUser();
      if (resp && (resp as any).result) {
        const userData = (resp as any).result as User;
        const normalizeRole = (r: any) => {
          if (!r) return 'customer';
          const s = String(r).toLowerCase();
          const stripped = s.startsWith('role_') ? s.replace('role_', '') : s;
          if (stripped === 'administrator') return 'admin';
          if (stripped === 'admin') return 'admin';
          if (stripped === 'staff') return 'staff';
          return 'customer';
        };

        setUser({
          ...userData,
          role: normalizeRole((userData as any).role) as User['role'],
          createdAt: userData.createdAt ? new Date(userData.createdAt) : new Date(),
          updatedAt: userData.updatedAt ? new Date(userData.updatedAt) : new Date()
        });
        localStorage.setItem('currentUser', JSON.stringify(minimalUserForStorage(userData)));
      }
    } catch (e: any) {
      // If token is expired (401), silently clear auth and return
      if (e?.response?.status === 401 || e?.response?.data?.code === 1012) {
        localStorage.removeItem('currentUser');
        localStorage.clear();
        setUser(null);
        return false;
      }
      console.debug('Could not fetch current user on init:', e);
      return false;
    }

    try {
      const addrs = await UserService.getAddresses();
      if (Array.isArray(addrs)) {
        const normalized = addrs.map((addr: any) => {
          const id = addr.id ?? addr.idAddress ?? addr.ID ?? addr.addressId ?? String(addr.id ?? addr.idAddress ?? '');
          return {
            ...addr,
            id,
            createdAt: addr.createdAt ? new Date(addr.createdAt) : new Date(),
            updatedAt: addr.updatedAt ? new Date(addr.updatedAt) : new Date()
          };
        });
        normalized.sort((a: any, b: any) => Number(b.isDefault) - Number(a.isDefault));
        setAddresses(normalized);
      }
    } catch (e) {
      console.debug('Failed to load addresses from server:', e);
    }

    try {
      const ordersData = await UserService.getMyOrders();
      if (Array.isArray(ordersData)) {
        setOrders(ordersData);
      }
    } catch (e) {
      console.debug('Failed to load orders from server:', e);
    }
    return true;
  };

  useEffect(() => {
    mountedRef.current = true;

    (async () => {
      let hasToken = false;
      try {
        const tok = getAuthToken();
        if (tok) {
          setAuthToken(tok);
          hasToken = true;
        }
      } catch (e) {}

      try {
        // Check if we have a token (either from storage or from OAuth flow)
        if (hasToken) {
          // only attempt to load current user if we have a token
          await loadCurrentUser();
        } else {
          // no token: skip loading current user
          // OAuth callback will be handled by GoogleCallbackPage, which will set token and trigger 'auth:tokenChanged' event
        }
      } catch (e) {
        console.debug('Auth bootstrap error', e);
      } finally {
        if (mountedRef.current) setInitializing(false);
      }
    })();

    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    const handler = async (ev: Event) => {
      const custom = ev as CustomEvent;
      const token = custom?.detail?.token;
      if (token) {
        // token set/updated -> reload user, addresses and orders
        await loadCurrentUser();
      } else {
        // token removed -> clear user
        setUser(null);
        setAddresses([]);
        setOrders([]);
      }
    };
    window.addEventListener('auth:tokenChanged', handler as EventListener);
    return () => window.removeEventListener('auth:tokenChanged', handler as EventListener);
  }, []);

  const refreshAddresses = async () => {
    try {
      const addrs = await UserService.getAddresses();
      if (Array.isArray(addrs)) {
        const normalized = addrs.map((addr: any) => {
          const id = addr.id ?? addr.idAddress ?? addr.ID ?? addr.addressId ?? String(addr.id ?? addr.idAddress ?? '');
          return {
            ...addr,
            id,
            createdAt: addr.createdAt ? new Date(addr.createdAt) : new Date(),
            updatedAt: addr.updatedAt ? new Date(addr.updatedAt) : new Date()
          };
        });

        
  normalized.sort((a: any, b: any) => Number(b.isDefault) - Number(a.isDefault));
  setAddresses(normalized);
      }
    } catch (err) {
      console.warn('Failed to refresh addresses:', err);
    }
  };

  const refreshOrders = async () => {
    if (!user) return;
    
    setIsLoadingOrders(true);
    try {
      const ordersData = await UserService.getMyOrders();
      console.log('Fetched orders from API:', ordersData);
      if (Array.isArray(ordersData)) {
        setOrders(ordersData);
      }
    } catch (err) {
      console.warn('Failed to refresh orders:', err);
    } finally {
      setIsLoadingOrders(false);
    }
  };

  const getOrderDetail = async (orderId: string): Promise<any> => {
    try {
      const orderDetail = await UserService.getOrderDetail(orderId);
      console.log('Fetched order detail:', orderDetail);
      return orderDetail;
    } catch (err) {
      console.error('Failed to get order detail:', err);
      throw err;
    }
  };

  const login = async (email: string, password: string): Promise<boolean> => {
    try {
      const response = await AuthService.login({ email, password });
      if (response && response.token) {
        const normalizeRole = (r: any) => {
          if (!r) return 'customer';
          const s = String(r).toLowerCase();
          const stripped = s.startsWith('role_') ? s.replace('role_', '') : s;
          if (stripped === 'administrator') return 'admin';
          if (stripped === 'admin') return 'admin';
          if (stripped === 'staff') return 'staff';
          return 'customer';
        };

        const userData: User = {
          id: response.id !== undefined ? String(response.id) : '',
          userName: response.userName || '',
          email: response.email || email,
          fullName: response.fullName || '',
          phoneNumber: response.phoneNumber || '',
          status: (response.status as User['status']) || 'active',
          role: normalizeRole(response.role) as User['role'],
          createdAt: new Date(),
          updatedAt: new Date(),
        };

        setUser(userData);
        localStorage.setItem('currentUser', JSON.stringify(minimalUserForStorage(userData)));

        try {
          await refreshAddresses();
          await refreshOrders();
        } catch (e) {
          console.warn('Failed to refresh addresses/orders after login', e);
        }

        return true;
      }
      return false;
    } catch (error: any) {
      console.error('Login failed:', error);
      throw error;
    }
  };

  const redirectToGoogle = (): void => {
    try {
      AuthService.redirectToGoogle();
    } catch (error) {
      console.error('Google redirect failed:', error);
    }
  };

  const loginWithGoogle = async (code: string): Promise<boolean> => {
    try {
      const response = await AuthService.loginWithGoogle(code);

      if (response && response.token) {
        const normalizeRole = (r: any) => {
          if (!r) return 'customer';
          const s = String(r).toLowerCase();
          const stripped = s.startsWith('role_') ? s.replace('role_', '') : s;
          if (stripped === 'administrator') return 'admin';
          if (stripped === 'admin') return 'admin';
          if (stripped === 'staff') return 'staff';
          return 'customer';
        };

        const userData: User = {
          id: response.id !== undefined ? String(response.id) : '',
          userName: response.userName || '',
          email: response.email || '',
          fullName: response.fullName || '',
          phoneNumber: response.phoneNumber || '',
          status: (response.status as User['status']) || 'active',
          role: normalizeRole(response.role) as User['role'],
          createdAt: new Date(),
          updatedAt: new Date(),
        };

        setUser(userData);
        localStorage.setItem('currentUser', JSON.stringify(minimalUserForStorage(userData)));

        // Refresh addresses and orders after successful OAuth login
        try {
          await refreshAddresses();
          await refreshOrders();
        } catch (e) {
          console.warn('Failed to refresh addresses/orders after Google login', e);
        }

        return true;
      }

      toast.error('Không thể đăng nhập bằng Google');
      return false;
    } catch (error: any) {
      console.error('Google login failed:', error);
      toast.error('Đăng nhập Google thất bại');
      return false;
    }
  };


  const signup = async (fullName: string, email: string, password: string): Promise<boolean> => {
    try {
      await AuthService.register({ fullName, email, password });
      return true;
    } catch (error) {
      console.error('Signup failed:', error);
      return false;
    }
  };

  const logout = async () => {
    try {
      await AuthService.logout();
    } catch (error) {
      console.error('Logout failed:', error);
    }
    setUser(null);
    setAddresses([]);
    setOrders([]);
    // Clear all localStorage data
    localStorage.clear();
  };

  const updateProfile = async (data: Partial<User>): Promise<boolean> => {
    if (!user) return false;

    try {
      const { email, ...safeData } = data;

      const response = await UserService.updateProfile(safeData);

      const updatedUser: User = {
        ...user,
        fullName: safeData.fullName ?? user.fullName,
        userName: safeData.userName ?? response.userName ?? "",
        phoneNumber: response.phoneNumber,
        updatedAt: new Date(),
      };

      const usersJson = localStorage.getItem("users");
      const users = usersJson ? JSON.parse(usersJson) : [];
      const userIndex = users.findIndex((u: any) => u.id === user.id);

      if (userIndex !== -1) {
        users[userIndex] = { ...users[userIndex], ...updatedUser };
        localStorage.setItem("users", JSON.stringify(users));
      }

      setUser(updatedUser);
      localStorage.setItem("currentUser", JSON.stringify(minimalUserForStorage(updatedUser)));

      return true;
    } catch (err: any) {
      const serverMessage = err?.response?.data?.message || err?.message;
      console.error("updateProfile error", err);
      return false;
    }
  };

  const changePass = async (newPassword: string, currentPassword: string): Promise<boolean> => {
    try {
      await UserService.changePass({ newPassword, currentPassword });
      return true;
    } catch (err: any) {
      const serverMessage = err?.response?.data?.message || err?.message;
      console.error("changePass error", serverMessage);
      return false;
    }
  };


  const addAddress = async (data: CreateAddressData): Promise<boolean> => {
    try {
      await UserService.addAddress(data);
      await refreshAddresses();
      return true;
    } catch (error) {
      console.error('Adding address failed:', error);
      return false;
    }
  };



  const updateAddress = async (data: UpdateAddressData): Promise<boolean> => {
    if (!user) return false;
    try {
      await UserService.updateAddress(data.id!, {
        recipientName: data.recipientName || '',
        recipientPhone: data.recipientPhone || '',
        address: data.address || '',
        district: data.district || '',
        city: data.city || '',
        isDefault: data.isDefault
      });
      await refreshAddresses();
      return true;
    } catch (error) {
      console.error('Failed to update address:', error);
      return false;
    }
  };



  const deleteAddress = async (addressId: string): Promise<boolean> => {
    if (!user) return false;

    try {
      await UserService.deleteAddress(addressId);
      await refreshAddresses();
      return true;
    } catch (error) {
      console.error('Failed to delete address:', error);
      return false;
    }
  };

  const setDefaultAddress = async (addressId: string): Promise<boolean> => {
    if (!user) return false;

    try {
      await UserService.setDefaultAddress(addressId);
      await refreshAddresses();
      return true;
    } catch (error) {
      console.error('Failed to set default address:', error);
      return false;
    }
  };

  const getDefaultAddress = (): Address | null => {
    return addresses.find(addr => addr.isDefault) || addresses[0] || null;
  };

  return (
    <AuthContext.Provider value={{
      user,
      isLoggedIn: !!user,
      initializing,
      addresses,
      orders,
      isLoadingOrders,
      login,
      signup,
      logout,
      changePass,
      updateProfile,
      addAddress,
      updateAddress,
      deleteAddress,
      setDefaultAddress,
      getDefaultAddress,
      refreshOrders,
      getOrderDetail,
      redirectToGoogle,
      loginWithGoogle
    }}>
      {children}
    </AuthContext.Provider>
  );
};
