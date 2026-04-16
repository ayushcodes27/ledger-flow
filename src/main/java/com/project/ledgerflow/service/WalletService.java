package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.IdempotencyKey;
import com.project.ledgerflow.entity.LedgerEntry;
import com.project.ledgerflow.entity.OutboxEvent;
import com.project.ledgerflow.entity.TransactionType;
import com.project.ledgerflow.exception.IdempotencyException;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.IdempotencyKeyRepository;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.OutboxEventRepository;
import com.project.ledgerflow.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.jpa.domain.AbstractPersistable_.id;

@Service
@RequiredArgsConstructor
public class WalletService {

    private final WalletRepository walletRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final RedissonClient redissonClient;

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

        saveOutboxEvent(
                wallet.getId(),
                "wallet.credited",
                buildWalletEventPayload(wallet.getId(), amount, "CREDIT")
        );

        return savedWallet;
    }

    @Transactional
    public Wallet debit(UUID walletId,  BigDecimal amount, String idempotencyKey){
        processIdempotency(idempotencyKey);


        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be greater than zero");
        }
        String lockKey = "wallet-lock:" + walletId;
        RLock lock = redissonClient.getLock(lockKey);

        try{
            boolean isLocked = lock.tryLock(5, TimeUnit.SECONDS);
            if (!isLocked) {
                throw new RuntimeException("System busy: Could not acquire lock for wallet " + id);
            }
            try{
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

                saveOutboxEvent(
                        wallet.getId(),
                        "wallet.debited",
                        buildWalletEventPayload(wallet.getId(), amount, "DEBIT")
                );

                return savedWallet;
            }
            finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        catch (InterruptedException e){
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting for lock", e);
        }
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

    private void saveOutboxEvent(UUID walletId, String eventType, String payload){
        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(walletId.toString());
        event.setAggregateType("WALLET");
        event.setEventType(eventType);
        event.setPayload(payload);
        outboxEventRepository.save(event);
    }

    private String buildWalletEventPayload(UUID walletId, BigDecimal amount, String action) {
        return String.format(
                "{\"walletId\":\"%s\",\"amount\":%s,\"action\":\"%s\"}",
                walletId,
                amount.toPlainString(),
                action
        );
    }
}
