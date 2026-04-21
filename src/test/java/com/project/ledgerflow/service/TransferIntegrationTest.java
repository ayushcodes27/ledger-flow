package com.project.ledgerflow.service;

import com.project.ledgerflow.controller.TransferController;
import com.project.ledgerflow.dto.TransferRequest;
import com.project.ledgerflow.entity.SagaState;
import com.project.ledgerflow.entity.Transaction;
import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.IdempotencyKeyRepository;
import com.project.ledgerflow.repository.LedgerEntryRepository;
import com.project.ledgerflow.repository.OutboxEventRepository;
import com.project.ledgerflow.repository.SagaStateRepository;
import com.project.ledgerflow.repository.TransactionRepository;
import com.project.ledgerflow.repository.WalletRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TransferIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TransferController transferController;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private IdempotencyKeyRepository idempotencyKeyRepository;

    @Autowired
    private WalletService walletService;

    @Autowired
    private TransferSagaOrchestrator transferSagaOrchestrator;

    @BeforeEach
    void setUp() {
        sagaStateRepository.deleteAll();
        transactionRepository.deleteAll();
        outboxEventRepository.deleteAll();
        idempotencyKeyRepository.deleteAll();
        ledgerEntryRepository.deleteAll();
        walletRepository.deleteAll();
    }

    @Test
    void completeTransferLifecycle_ShouldSucceedAndUpdateBalances() {
        Wallet sender = walletService.createWallet("USD");
        walletService.credit(sender.getId(), new BigDecimal("1000.00"), "seed-sender-wallet");

        Wallet receiver = walletService.createWallet("USD");
        walletService.credit(receiver.getId(), new BigDecimal("500.00"), "seed-receiver-wallet");

        Map<String, Object> transferPayload = Map.of(
                "sourceWalletId", sender.getId(),
                "targetWalletId", receiver.getId(),
                "amount", new BigDecimal("200.00")
        );

        var response = transferController.initiateTransfer(new TransferRequest(
                sender.getId(),
                receiver.getId(),
                (BigDecimal) transferPayload.get("amount")
        ));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();

        Map<String, Object> responseBody = response.getBody();
        assertThat(responseBody).containsKeys("transactionId", "message");

        UUID transactionId = UUID.fromString(responseBody.get("transactionId").toString());
        TransferExecutionResult executionResult = transferSagaOrchestrator.executeTransfer(transactionId);

        assertThat(executionResult.completed()).isTrue();

        Wallet updatedSender = walletRepository.findById(sender.getId()).orElseThrow();
        Wallet updatedReceiver = walletRepository.findById(receiver.getId()).orElseThrow();
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow();
        SagaState sagaState = sagaStateRepository.findByTransactionId(transactionId).orElseThrow();

        assertThat(updatedSender.getBalance()).isEqualByComparingTo("800.00");
        assertThat(updatedReceiver.getBalance()).isEqualByComparingTo("700.00");
        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(sagaState.getCurrentStep()).isEqualTo(SagaStepStatus.COMPLETED);
    }

    @Test
    void concurrentTransfers_ShouldMaintainDataIntegrity() throws InterruptedException {

        Wallet sender = walletService.createWallet("USD");
        walletService.credit(sender.getId(), new BigDecimal("1000.00"), "seed-sender-wallet");

        Wallet receiver = walletService.createWallet("USD");

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger completedTransfers = new AtomicInteger();

        // Record how many transactions existed BEFORE the stress test
        // (because walletService.credit() likely created some setup transactions)
        long initialTxCount = transactionRepository.count();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();

                    var response = transferController.initiateTransfer(new TransferRequest(
                            sender.getId(),
                            receiver.getId(),
                            new BigDecimal("50.00")
                    ));

                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
                    assertThat(response.getBody()).isNotNull();

                    UUID transactionId = UUID.fromString(response.getBody().get("transactionId").toString());
                    TransferExecutionResult result = transferSagaOrchestrator.executeTransfer(transactionId);
                    if (result.completed()) {
                        completedTransfers.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    long totalProcessed = transactionRepository.count() - initialTxCount;
                    assertThat(totalProcessed).isEqualTo(50);
                });

        Wallet finalSender = walletRepository.findById(sender.getId()).orElseThrow();
        Wallet finalReceiver = walletRepository.findById(receiver.getId()).orElseThrow();

        BigDecimal totalMoneyInSystem = finalSender.getBalance().add(finalReceiver.getBalance());
        assertThat(totalMoneyInSystem).isEqualByComparingTo("1000.00");

        assertThat(finalSender.getBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        assertThat(finalReceiver.getBalance())
                .isEqualByComparingTo(new BigDecimal("50.00").multiply(BigDecimal.valueOf(completedTransfers.get())));
    }
}
