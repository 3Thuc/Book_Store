package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.AddressEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Status;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AddressRepository extends JpaRepository<AddressEntity, Integer> {
    List<AddressEntity> findByUserUserId(Integer userId);
    List<AddressEntity> findByUserUserIdAndStatus(Integer userId, Status status);
    AddressEntity findByUserUserIdAndIsDefaultTrue(Integer userId);
    Optional<AddressEntity> findByAddressId(Integer addressId);
}
