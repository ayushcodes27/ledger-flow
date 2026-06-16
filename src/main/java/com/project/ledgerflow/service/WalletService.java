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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    // RED FLAG 1 FIX: REQUIRES_NEW decouples this from the Saga Orchestrator transaction.
    // If this fails (e.g. Insufficient Funds), ONLY this transaction rolls back.
    // The Orchestrator can then catch the error and successfully update the SagaState to FAILED.
    @Transactional(propagation = Propagation.REQUIRES_NEW, noRollbackFor = IdempotencyException.class)
    public Wallet credit(UUID walletId, BigDecimal amount, String idempotencyKey, UUID transactionId){
        processIdempotency(idempotencyKey);

        if(amount.compareTo(BigDecimal.ZERO) <= 0){
            throw new IllegalArgumentException("Credit amount must be greater than zero");
        }
        return withWalletLock(walletId, () -> {
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
                    buildWalletEventPayload(wallet.getId(), amount, "CREDIT", transactionId)
            );

            return savedWallet;
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, noRollbackFor = IdempotencyException.class)
    public Wallet debit(UUID walletId,  BigDecimal amount, String idempotencyKey, UUID transactionId){
        processIdempotency(idempotencyKey);

        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be greater than zero");
        }
        return withWalletLock(walletId, () -> {
            Wallet wallet = getWallet(walletId);

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
                    buildWalletEventPayload(wallet.getId(), amount, "DEBIT", transactionId)
            );

            return savedWallet;
        });
    }

    // RED FLAG 2 FIX: Enforce atomicity via DB unique constraint (PK).
    // existsById + save is prone to race conditions.
    private void processIdempotency(String idempotencyKey) {
        try {
            idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(idempotencyKey, null));
        } catch (DataIntegrityViolationException e) {
            throw new IdempotencyException("Transaction with Idempotency Key [" + idempotencyKey + "] has already been processed.");
        }
    }

    private void saveOutboxEvent(UUID walletId, String eventType, String payload){
        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(walletId.toString());
        event.setAggregateType("WALLET");
        event.setEventType(eventType);
        event.setPayload(payload);
        outboxEventRepository.save(event);
    }

    private String buildWalletEventPayload(UUID walletId, BigDecimal amount, String action, UUID transactionId) {
        return String.format(
                "{\"walletId\":\"%s\",\"amount\":%s,\"action\":\"%s\",\"transactionId\":%s}",
                walletId,
                amount.toPlainString(),
                action,
                transactionId == null ? "null" : "\"" + transactionId + "\""
        );
    }

    private <T> T withWalletLock(UUID walletId, LockedOperation<T> operation) {
        String lockKey = "wallet-lock:" + walletId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean unlockOnCompletion = false;

        try {
            boolean isLocked = lock.tryLock(5, TimeUnit.SECONDS);
            if (!isLocked) {
                throw new RuntimeException("System busy: Could not acquire lock for wallet " + walletId);
            }

            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                unlockOnCompletion = true;
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int status) {
                        if (lock.isHeldByCurrentThread()) {
                            lock.unlock();
                        }
                    }
                });
            }

            return operation.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting for lock", e);
        } finally {
            if (!unlockOnCompletion && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @FunctionalInterface
    private interface LockedOperation<T> {
        T run();
    }
}

