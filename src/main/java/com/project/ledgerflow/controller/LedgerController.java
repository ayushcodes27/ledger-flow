package com.project.ledgerflow.controller;

import com.project.ledgerflow.entity.LedgerEntry;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.service.ReconciliationService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/wallets/{id}")
public class LedgerController {
    private final LedgerEntryRepository ledgerEntryRepository;
    private final ReconciliationService reconciliationService;

    public LedgerController(LedgerEntryRepository ledgerEntryRepository, ReconciliationService reconciliationService) {
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.reconciliationService = reconciliationService;
    }

    @GetMapping("/ledger")
    public ResponseEntity<Page<LedgerEntry>> getLedger(
            @PathVariable UUID id,
            Pageable pageable) {
        return ResponseEntity.ok(ledgerEntryRepository.findByWalletIdOrderByCreatedAtDesc(id, pageable));
    }

    // // SELECT * FROM ledger_entry WHERE wallet_id = ? AND created_at BETWEEN ? AND ? ORDER BY created_at ASC;
    @GetMapping("/statement")
    public ResponseEntity<List<LedgerEntry>> getStatement(
            @PathVariable UUID id,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end){
        return ResponseEntity.ok(ledgerEntryRepository.findByWalletIdAndCreatedAtBetweenOrderByCreatedAtAsc(id, start, end));
    }

    @GetMapping("/reconcile")
    public ResponseEntity<ReconciliationService.ReconciliationResult> reconcile(@PathVariable UUID id) {
        return ResponseEntity.ok(reconciliationService.reconcileWallet(id));
    }
}
