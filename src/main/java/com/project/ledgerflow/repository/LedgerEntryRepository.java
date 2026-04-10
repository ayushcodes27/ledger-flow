package com.project.ledgerflow.repository;

import com.project.ledgerflow.entity.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    // generates the SQL : SELECT * FROM ledger_entries WHERE wallet_id = ? ORDER BY created_at DESC
    List<LedgerEntry> findByWalletIdOrderByCreatedAtDesc(UUID walletId);
}