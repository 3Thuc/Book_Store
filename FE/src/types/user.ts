export type UserStatus = 'active' | 'delete';
export type UserRole = 'customer' | 'admin' | "staff";

export interface User {
  id: string;
  userName: string;
  email: string;
  fullName: string;
  phoneNumber?: string;
  status: UserStatus;
  role: UserRole;
  createdAt: Date;
  updatedAt: Date;
}

export interface Address {
  id: string;
  userId: string;
  recipientName: string;
  recipientPhone: string;
  address: string;
  district?: string;
  city?: string;
  isDefault: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateAddressData {
  recipientName: string;
  recipientPhone: string;
  address: string;
  district?: string;
  city?: string;
  isDefault?: boolean;
}

export interface UpdateAddressData extends Partial<CreateAddressData> {
  id: string;
}
