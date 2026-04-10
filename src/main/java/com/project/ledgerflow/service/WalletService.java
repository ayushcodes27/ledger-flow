package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.IdempotencyKey;
import com.project.ledgerflow.entity.LedgerEntry;
import com.project.ledgerflow.entity.TransactionType;
import com.project.ledgerflow.exception.IdempotencyException;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.IdempotencyKeyRepository;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.WalletRepository;
//import jakarta.transaction.Transactional;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WalletService {

    private final WalletRepository walletRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;

    @Transactional
    public Wallet createWallet(String currency){
        Wallet wallet = Wallet.builder()
                .balance(BigDecimal.ZERO)
                .currency(currency.toUpperCase())
                .build();
        return walletRepository.save(wallet);
    }

    @Transactional(readOnly = true)
    public Wallet getWallet(UUID walletId){
        return walletRepository.findById(walletId)
                .orElseThrow(() -> new RuntimeException("Wallet not found with ID: " + walletId));
    }

    @Transactional
    public Wallet credit(UUID walletId, BigDecimal amount, String idempotencyKey){
        processIdempotency(idempotencyKey);

        if(amount.compareTo(BigDecimal.ZERO) <= 0){
            throw new IllegalArgumentException("Credit amount must be greater than zero");
        }

        Wallet wallet = getWallet(walletId);
        wallet.setBalance(wallet.getBalance().add(amount));
        Wallet savedWallet = walletRepository.save(wallet);

        LedgerEntry entry = LedgerEntry.builder()
                .walletId(savedWallet.getId())
                .amount(amount)
                .type(TransactionType.CREDIT)
                .build();
        ledgerEntryRepository.save(entry);

        return savedWallet;
    }

    @Transactional
    public Wallet debit(UUID walletId,  BigDecimal amount, String idempotencyKey){
        processIdempotency(idempotencyKey);

        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be greater than zero");
        }

        Wallet wallet = getWallet(walletId);

        // Check for sufficient funds
        if (wallet.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient funds. Current balance: " + wallet.getBalance());
        }

        wallet.setBalance(wallet.getBalance().subtract(amount));
        Wallet savedWallet = walletRepository.save(wallet);

        LedgerEntry entry = LedgerEntry.builder()
                .walletId(savedWallet.getId())
                .amount(amount)
                .type(TransactionType.DEBIT)
                .build();
        ledgerEntryRepository.save(entry);

        return savedWallet;
    }

    private void processIdempotency(String idempotencyKey) {
        if (idempotencyKeyRepository.existsById(idempotencyKey)) {
            // In a fully mature system, we would return the exact cached response here.
            // For our scope, throwing a 409 Conflict equivalent currently.
            throw new IdempotencyException("Transaction with Idempotency Key [" + idempotencyKey + "] has already been processed.");
        }

        // If it doesn't exist, save it so it can never be used again
        idempotencyKeyRepository.save(new IdempotencyKey(idempotencyKey, null));
    }
}
