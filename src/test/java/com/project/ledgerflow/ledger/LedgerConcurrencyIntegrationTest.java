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
import com.project.ledgerflow.service.TransferExecutionResult;
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

        // Create wallets through the service path so the opening balance is ledger-backed.
        Wallet walletA = walletService.createWallet("USD");
        walletService.credit(walletA.getId(), new BigDecimal("1000.00"), "seed-wallet-a");

        Wallet walletB = walletService.createWallet("USD");

        walletA_Id = walletA.getId();
        walletB_Id = walletB.getId();
    }

    @Test
    void testConcurrentTransfersMaintainLedgerIntegrity() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // The "starting gun" - prevents threads from running until we say go
        CountDownLatch startLatch = new CountDownLatch(1);
        // The "finish line" - tracks when all threads have completed
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        AtomicInteger successfulTransfers = new AtomicInteger(0);
        AtomicInteger failedTransfers = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait at the starting line

                    // Attempt to transfer $10 from A to B
                    UUID transactionId = transferSagaOrchestrator.initiateTransfer(
                            walletA_Id,
                            walletB_Id,
                            new BigDecimal("10.00")
                    );
                    TransferExecutionResult result = transferSagaOrchestrator.executeTransfer(transactionId);

                    if (result.completed()) {
                        successfulTransfers.incrementAndGet();
                    } else {
                        failedTransfers.incrementAndGet();
                        if (failedTransfers.get() <= 5) {
                            System.err.println("Transfer failed: " + result.errorMessage());
                        }
                    }
                } catch (Exception e) {
                    failedTransfers.incrementAndGet();
                    if (failedTransfers.get() <= 5) {
                        System.err.println("Transfer failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                    }
                } finally {
                    endLatch.countDown(); // Mark this thread as finished
                }
            });
        }

        // Fire the starting gun!
        startLatch.countDown();

        // Wait for all 100 threads to cross the finish line
        endLatch.await();
        executor.shutdown();

        System.out.println("Successful transfers: " + successfulTransfers.get());
        System.out.println("Failed transfers (Lock rejections): " + failedTransfers.get());

        // --- VERIFICATION ---

        // 1. Reconcile both wallets to prove mathematical integrity
        ReconciliationService.ReconciliationResult resultA = reconciliationService.reconcileWallet(walletA_Id);
        ReconciliationService.ReconciliationResult resultB = reconciliationService.reconcileWallet(walletB_Id);

        assertTrue(resultA.isConsistent(), "Wallet A ledger is corrupted!");
        assertTrue(resultB.isConsistent(), "Wallet B ledger is corrupted!");

        // 2. Verify money was neither created nor destroyed
        BigDecimal expectedFinalBalanceA = new BigDecimal("1000.00")
                .subtract(new BigDecimal("10.00").multiply(new BigDecimal(successfulTransfers.get())));

        BigDecimal expectedFinalBalanceB = new BigDecimal("10.00")
                .multiply(new BigDecimal(successfulTransfers.get()));

        assertEquals(0, resultA.currentBalance().compareTo(expectedFinalBalanceA));
        assertEquals(0, resultB.currentBalance().compareTo(expectedFinalBalanceB));

        // Total money in the system MUST still equal $1000
        BigDecimal systemTotal = resultA.currentBalance().add(resultB.currentBalance());
        assertEquals(0, systemTotal.compareTo(new BigDecimal("1000.00")));
    }
}
