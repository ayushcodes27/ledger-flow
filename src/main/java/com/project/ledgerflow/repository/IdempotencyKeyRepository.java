package com.project.ledgerflow.repository;

import com.project.ledgerflow.entity.IdempotencyKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, String> {
    // We get existsById(String id) and save(IdempotencyKey key)
}
