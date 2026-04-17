package com.project.ledgerflow.service;


import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.WalletRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ReconciliationService {
    private final WalletRepository walletRepository;
    private final LedgerEntryRepository ledgerEntryRepository;

    public ReconciliationService(WalletRepository walletRepository, LedgerEntryRepository ledgerEntryRepository) {
        this.walletRepository = walletRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    public record ReconciliationResult(
            UUID walletId,
            BigDecimal currentBalance,
            BigDecimal calculatedBalance,
            boolean isConsistent
    ) {}

    @Transactional(readOnly = true)
    public ReconciliationResult reconcileWallet(UUID walletId) {
        Wallet wallet = walletRepository.findById(walletId)
                .orElseThrow(() -> new IllegalArgumentException("Wallet not found: " + walletId));

        BigDecimal calculatedBalance = ledgerEntryRepository.calculateProvableBalance(walletId);

        boolean isConsistent = wallet.getBalance().compareTo(calculatedBalance) == 0;

        return new ReconciliationResult(
                wallet.getId(),
                wallet.getBalance(),
                calculatedBalance,
                isConsistent
        );
    }
}
