package com.project.ledgerflow.ledger;

import com.project.ledgerflow.service.AbstractIntegrationTest;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.IdempotencyKeyRepository;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.OutboxEventRepository;
import com.project.ledgerflow.repository.SagaStateRepository;
import com.project.ledgerflow.repository.TransactionRepository;
import com.project.ledgerflow.repository.WalletRepository;
import com.project.ledgerflow.service.ReconciliationService;
import com.project.ledgerflow.service.TransferSagaOrchestrator;
import com.project.ledgerflow.service.WalletService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LedgerConcurrencyIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TransferSagaOrchestrator transferSagaOrchestrator;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private WalletService walletService;

    private UUID walletA_Id;
    private UUID walletB_Id;

    @BeforeEach
    void setUp() {
        sagaStateRepository.deleteAll();
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
        idempotencyKeyRepository.deleteAll();
        ledgerEntryRepository.deleteAll();
        walletRepository.deleteAll();

        Wallet walletA = walletService.createWallet("USD");
        walletService.credit(walletA.getId(), new BigDecimal("1000.00"), "seed-wallet-a", null);

        Wallet walletB = walletService.createWallet("USD");

        walletA_Id = walletA.getId();
        walletB_Id = walletB.getId();
    }

    @Test
    void testConcurrentTransfersMaintainLedgerIntegrity() throws InterruptedException {
        int threadCount = 20; // Reduced for faster test execution in async simulation
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        AtomicInteger successfulTransfers = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); 

                    UUID transactionId = transferSagaOrchestrator.initiateTransfer(
                            walletA_Id,
                            walletB_Id,
                            new BigDecimal("10.00")
                    );
                    
                    // Simulate async flow with retries (like Kafka consumer would)
                    executeWithRetry(() -> transferSagaOrchestrator.handleTransferInitiated(transactionId));
                    executeWithRetry(() -> transferSagaOrchestrator.handleDebitCompleted(transactionId));
                    executeWithRetry(() -> transferSagaOrchestrator.handleCreditCompleted(transactionId));

                    successfulTransfers.incrementAndGet();
                } catch (Exception e) {
                    System.err.println("Transfer failed: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        ReconciliationService.ReconciliationResult resultA = reconciliationService.reconcileWallet(walletA_Id);
        ReconciliationService.ReconciliationResult resultB = reconciliationService.reconcileWallet(walletB_Id);

        assertTrue(resultA.isConsistent());
        assertTrue(resultB.isConsistent());

        BigDecimal systemTotal = resultA.currentBalance().add(resultB.currentBalance());
        assertEquals(0, systemTotal.compareTo(new BigDecimal("1000.00")));
    }

    private void executeWithRetry(Runnable action) {
        int maxRetries = 15;
        for (int i = 0; i < maxRetries; i++) {
            try {
                action.run();
                return;
            } catch (Exception e) {
                if (i == maxRetries - 1) throw e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

