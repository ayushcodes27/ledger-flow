package com.project.ledgerflow.repository;

import com.project.ledgerflow.entity.LedgerEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Repository
public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    Page<LedgerEntry> findByWalletIdOrderByCreatedAtDesc(UUID walletId, Pageable pageable);

    // generates the SQL : SELECT * FROM ledger_entries WHERE wallet_id = ? ORDER BY created_at DESC
    List<LedgerEntry> findByWalletIdOrderByCreatedAtDesc(UUID walletId);

    List<LedgerEntry> findByWalletIdAndCreatedAtBetweenOrderByCreatedAtAsc(
            UUID walletId,
            java.time.LocalDateTime start,
            java.time.LocalDateTime end
    );

    @Query("""
        SELECT COALESCE(
            SUM(CASE WHEN e.type = 'CREDIT' THEN e.amount ELSE -e.amount END), 
            0
        ) 
        FROM LedgerEntry e WHERE e.walletId = :walletId
    """)
    BigDecimal calculateProvableBalance(@Param("walletId") UUID walletId);

}
