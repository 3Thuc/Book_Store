package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.CreateUserRequest;
import com.be.book.BookStorage.dto.Request.Admin.UpdateUserRequest;
import com.be.book.BookStorage.dto.Request.User.AddAddressReq;
import com.be.book.BookStorage.dto.Request.User.UpdateAddressReq;
import com.be.book.BookStorage.dto.Request.User.UserReq;
import com.be.book.BookStorage.dto.Response.Order.OrderItemRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.User.AddressRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.entity.AddressEntity;
import com.be.book.BookStorage.entity.OrderDetailEntity;
import com.be.book.BookStorage.entity.OrderEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.events.GoogleUserForgotPasswordEvent;
import com.be.book.BookStorage.events.UserRegisteredEvent;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.AddressRepository;
import com.be.book.BookStorage.repository.OrderRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.core.userdetails.*;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class UserService implements UserDetailsService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final AddressRepository addressRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final OrderRepository orderRepository;
    private final MinioService minioService;

    private UserRes mapToUserRes(UserEntity u) {
        return UserRes.builder()
                .id(u.getUserId())
                .email(u.getEmail())
                .fullName(u.getFullName())
                .role(u.getRole())
                .status(u.getStatus())
                .createdAt(u.getCreatedAt())
                .build();
    }

    @Override
    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
        UserEntity user = getUserByEmail(email);

        return org.springframework.security.core.userdetails.User.builder()
                .username(user.getEmail())
                .password(user.getPasswordHash())
                .authorities(java.util.Collections.emptyList())
                .build();
    }

    private UserEntity getUserByEmail(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
    }

    private AddressEntity getAddressForUser(String email, int id) {
        UserEntity user = getUserByEmail(email);
        AddressEntity address = addressRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.ADDRESS_NOT_FOUND));
        if (!address.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.ADDRESS_NOT_FOUND);
        }
        return address;
    }

    private AddressRes mapToAddressRes(AddressEntity address) {
        return AddressRes.builder()
                .idAddress(address.getAddressId())
                .recipientName(address.getRecipientName())
                .recipientPhone(address.getRecipientPhone())
                .address(address.getAddressText())
                .district(address.getDistrict())
                .city(address.getCity())
                .isDefault(address.getIsDefault())
                .build();
    }

    private void resetDefaultAddress(AddressEntity address) {
        List<AddressEntity> addresses = addressRepository.findByUserUserId(address.getUser().getUserId());
        addresses.forEach(a -> {
            if (!a.getAddressId().equals(address.getAddressId()) && Boolean.TRUE.equals(a.getIsDefault())) {
                a.setIsDefault(false);
                a.setUpdatedAt(LocalDateTime.now());
            }
        });
        addressRepository.saveAll(addresses);
    }

    public UserRes updateProfile(String email, UserReq request) {
        UserEntity user = getUserByEmail(email);
        user.setFullName(request.getFullName());
        user.setPhone(request.getPhoneNumber());
        user.setUsername(request.getUserName());
        user.setUpdatedAt(LocalDateTime.now());

        try {
            UserEntity saved = userRepository.save(user);
            return UserRes.builder()
                    .id(saved.getUserId())
                    .fullName(saved.getFullName())
                    .phoneNumber(saved.getPhone())
                    .userName(saved.getUsername())
                    .email(saved.getEmail())
                    .role(saved.getRole())
                    .status(saved.getStatus())
                    .createdAt(saved.getCreatedAt())
                    .build();
        } catch (Exception e) {
            throw new AppException(ErrorCode.UPDATE_FAILED);
        }
    }

    public boolean verifyPassword(String email, String currentPassword) {
        UserEntity user = getUserByEmail(email);
        return passwordEncoder.matches(currentPassword, user.getPasswordHash());
    }

    public boolean changePass(String email, String newPassword) {
        UserEntity user = getUserByEmail(email);
        user.setPasswordHash(passwordEncoder.encode(newPassword));
        user.setUpdatedAt(LocalDateTime.now());
        userRepository.save(user);
        return true;
    }

    public AddressRes addAddress(String email, AddAddressReq req) {
        UserEntity user = getUserByEmail(email);

        if (Boolean.TRUE.equals(req.getIsDefault())) {
            List<AddressEntity> addresses = addressRepository.findByUserUserId(user.getUserId());
            addresses.forEach(a -> {
                if (Boolean.TRUE.equals(a.getIsDefault())) {
                    a.setIsDefault(false);
                    a.setUpdatedAt(LocalDateTime.now());
                    addressRepository.save(a);
                }
            });
        }

        AddressEntity address = AddressEntity.builder()
                .user(user)
                .recipientName(req.getRecipientName())
                .recipientPhone(req.getRecipientPhone())
                .addressText(req.getAddress())
                .district(req.getDistrict())
                .city(req.getCity())
                .status(Status.active)
                .isDefault(req.getIsDefault())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        addressRepository.save(address);
        return mapToAddressRes(address);
    }

    @Transactional
    public AddressRes updateAddress(String email, int id, UpdateAddressReq req) {
        AddressEntity address = getAddressForUser(email, id);

        if (Boolean.TRUE.equals(req.getIsDefault())) {
            resetDefaultAddress(address);
        }

        address.setRecipientName(req.getRecipientName());
        address.setRecipientPhone(req.getRecipientPhone());
        address.setAddressText(req.getAddress());
        address.setDistrict(req.getDistrict());
        address.setCity(req.getCity());
        address.setIsDefault(req.getIsDefault());
        address.setUpdatedAt(LocalDateTime.now());

        addressRepository.save(address);
        return mapToAddressRes(address);
    }

    public List<AddressRes> getListAddress(String email) {
        UserEntity user = getUserByEmail(email);

        return addressRepository.findByUserUserIdAndStatus(user.getUserId(), Status.active).stream()
                .map(this::mapToAddressRes)
                .collect(Collectors.toList());
    }

    @Transactional
    public AddressRes deleteAddressById(String email, int id) {
        AddressEntity address = getAddressForUser(email, id);

        if (Boolean.TRUE.equals(address.getIsDefault())) {
            List<AddressEntity> addresses = addressRepository.findByUserUserId(address.getUser().getUserId());
            for (AddressEntity a : addresses) {
                if (!a.getAddressId().equals(id) && a.getStatus() == Status.active) {
                    a.setIsDefault(true);
                    a.setUpdatedAt(LocalDateTime.now());
                    addressRepository.save(a);
                    break;
                }
            }
        }

        address.setStatus(Status.locked);
        address.setUpdatedAt(LocalDateTime.now());
        addressRepository.save(address);

        return mapToAddressRes(address);
    }

    @Transactional
    public AddressRes updateDefaultAddress(String email, int id) {
        AddressEntity address = getAddressForUser(email, id);
        resetDefaultAddress(address);

        address.setIsDefault(true);
        address.setUpdatedAt(LocalDateTime.now());
        addressRepository.save(address);

        return mapToAddressRes(address);
    }

    public List<UserRes> getAllUsers(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        return userRepository.findAll().stream()
                .map(this::mapToUserRes)
                .collect(Collectors.toList());
    }

    public UserRes updateUser(String email, int id, UpdateUserRequest request) {
        if (request.getRole() == null && request.getStatus() == null) {
            throw new AppException(ErrorCode.INVALID_REQUEST);
        }

        UserEntity admin = getUserByEmail(email);
        if (admin.getRole() != Role.admin) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        UserEntity user = userRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (request.getRole() != null) {
            user.setRole(request.getRole());
        }
        if (request.getStatus() != null) {
            user.setStatus(request.getStatus());
        }

        user.setUpdatedAt(LocalDateTime.now());

        try {
            UserEntity saved = userRepository.save(user);
            return UserRes.builder()
                    .id(saved.getUserId())
                    .fullName(saved.getFullName())
                    .phoneNumber(saved.getPhone())
                    .userName(saved.getUsername())
                    .email(saved.getEmail())
                    .role(saved.getRole())
                    .status(saved.getStatus())
                    .createdAt(saved.getCreatedAt())
                    .build();
        } catch (Exception e) {
            throw new AppException(ErrorCode.UPDATE_FAILED);
        }
    }
    public UserRes adminResetPassUser(String email, Integer id) {
        UserEntity admin = getUserByEmail(email);
        if (admin.getRole() != Role.admin) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        UserEntity user = userRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
        String newPassword = UUID.randomUUID().toString().substring(0, 10);

        user.setPasswordHash(passwordEncoder.encode(newPassword));
        userRepository.save(user);

        String requestTime = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));

        eventPublisher.publishEvent(
                new GoogleUserForgotPasswordEvent(this, user, newPassword, requestTime)
        );
        return UserRes.builder()
                .id(user.getUserId())
                .fullName(user.getFullName())
                .phoneNumber(user.getPhone())
                .userName(user.getUsername())
                .email(user.getEmail())
                .role(user.getRole())
                .status(user.getStatus())
                .createdAt(user.getCreatedAt())
                .build();
    }

    public UserRes createUser(String email, CreateUserRequest request) {
        UserEntity userAdmin = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (userAdmin.getRole() != Role.admin ) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }
        String rawPassword = UUID.randomUUID().toString().substring(0, 10);

        UserEntity user = userRepository.findByEmail(request.getEmail())
                .map(u -> {
                    if (u.getStatus() == Status.active) {
                        throw new AppException(ErrorCode.USER_EXISTED);
                    }

                    u.setFullName(request.getFullName());
                    u.setPasswordHash(passwordEncoder.encode(rawPassword));
                    u.setStatus(Status.active);
                    u.setUpdatedAt(LocalDateTime.now());
                    u.setRole(request.getRole());
                    return u;
                })
                .orElseGet(() -> UserEntity.builder()
                        .email(request.getEmail())
                        .fullName(request.getFullName())
                        .username(request.getEmail())
                        .passwordHash(passwordEncoder.encode(rawPassword))
                        .status(Status.active)
                        .role(request.getRole())
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .build()
                );

        UserEntity savedUser = userRepository.save(user);

        eventPublisher.publishEvent(new UserRegisteredEvent(this, savedUser));

        return mapToUserRes(savedUser);
    }

    public List<OrderRes> getListOrders(String email) {
        UserEntity user = getUserByEmail(email);

        List<OrderEntity> orders = orderRepository.findAllWithDetails().stream()
            .filter(order -> order.getUser() != null && order.getUser().getUserId() != null && order.getUser().getUserId().equals(user.getUserId()))
            .collect(Collectors.toList());

        return orders.stream()
                .map(this::mapToOrderRes)
                .collect(Collectors.toList());
    }

    private OrderRes mapToOrderRes(OrderEntity order) {
        Map<String, String> presignCache = new HashMap<>();

        List<OrderItemRes> items = order.getDetails().stream()
                .map(detail -> {
                    boolean isReviewed = orderRepository.hasUserReviewedBook(
                            order.getUser().getUserId(),
                            detail.getBook().getBookId()
                    );

                    String rawImage = detail.getBook().getImage() != null
                            ? detail.getBook().getImage().getImageUrl()
                            : null;

                    String imageUrl = (rawImage != null && !rawImage.isEmpty())
                            ? rawImage.split(",")[0].trim()
                            : null;

                    String imageUrlPresign = null;
                    if (imageUrl != null) {
                        imageUrlPresign = presignCache.computeIfAbsent(imageUrl, url -> {
                            try {
                                return minioService.getPresignedUrl(url);
                            } catch (Exception e) {
                                return null;
                            }
                        });
                    }

                    String authorName = detail.getBook().getAuthor() != null
                            ? detail.getBook().getAuthor().getAuthorName()
                            : "Unknown";

                    return OrderItemRes.builder()
                            .id(detail.getId().getOrderId() + "-" + detail.getId().getBookId())
                            .bookId(detail.getBook().getBookId().toString())
                            .title(detail.getBook().getTitle())
                            .author(authorName)
                            .price(detail.getTotalPrice())
                            .quantity(detail.getQuantity())
                            .imageUrl(imageUrlPresign)
                            .isReviewed(isReviewed)
                            .build();
                })
                .collect(Collectors.toList());


        AddressEntity address = order.getAddress();
        String shippingAddress = "";
        String customerName = "";
        String customerPhone = "";

        if (address != null) {
            shippingAddress = formatAddress(address);
            customerName = address.getRecipientName();
            customerPhone = address.getRecipientPhone();
        }

        boolean isPaid = order.getPaymentStatus() == PaymentStatus.paid;

        return OrderRes.builder()
                .id(order.getOrderId().toString())
                .userId(order.getUser().getUserId().toString())
                .items(items)
                .totalAmount(order.getTotalAmount())
                .orderDate(order.getOrderDate())
                .status(order.getStatus())
                .deliveryDate(order.getUpdatedAt())
                .paymentMethod(order.getPaymentMethod())
                .shippingAddress(shippingAddress)
                .customerName(customerName)
                .customerPhone(customerPhone)
                .note(order.getNote())
                .isPaid(isPaid)
                .build();
    }

    private String formatAddress(AddressEntity address) {
        if (address == null) return null;

        StringBuilder sb = new StringBuilder();

        if (address.getAddressText() != null) {
            sb.append(address.getAddressText());
        }

        if (address.getDistrict() != null) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(address.getDistrict());
        }

        if (address.getCity() != null) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(address.getCity());
        }

        return sb.toString();
    }

}
