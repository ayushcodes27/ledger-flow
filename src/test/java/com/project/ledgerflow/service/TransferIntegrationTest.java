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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.util.UUID;

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
        walletService.credit(sender.getId(), new BigDecimal("1000.00"), "seed-sender-wallet", null);

        Wallet receiver = walletService.createWallet("USD");
        walletService.credit(receiver.getId(), new BigDecimal("500.00"), "seed-receiver-wallet", null);

        var response = transferController.initiateTransfer(new TransferRequest(
                sender.getId(),
                receiver.getId(),
                new BigDecimal("200.00")
        ));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID transactionId = UUID.fromString(response.getBody().get("transactionId").toString());

        // Manually drive the async saga
        transferSagaOrchestrator.handleTransferInitiated(transactionId);
        transferSagaOrchestrator.handleDebitCompleted(transactionId);
        transferSagaOrchestrator.handleCreditCompleted(transactionId);

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
    void retryAfterIdempotency_ShouldNotFailSaga() {
        Wallet sender = walletService.createWallet("USD");
        walletService.credit(sender.getId(), new BigDecimal("1000.00"), "seed-sender", null);
        Wallet receiver = walletService.createWallet("USD");

        UUID transactionId = transferSagaOrchestrator.initiateTransfer(sender.getId(), receiver.getId(), new BigDecimal("100.00"));

        // First attempt for debit
        transferSagaOrchestrator.handleTransferInitiated(transactionId);
        
        // Simulate a retry of handleTransferInitiated (e.g. Kafka re-delivery after crash)
        // walletService.debit will throw IdempotencyException
        // Orchestrator should catch it and NOT fail
        transferSagaOrchestrator.handleTransferInitiated(transactionId);

        SagaState state = sagaStateRepository.findByTransactionId(transactionId).orElseThrow();
        assertThat(state.getCurrentStep()).isEqualTo(SagaStepStatus.INITIATED); // Still INITIATED because handleDebitCompleted hasn't run
        assertThat(state.getErrorMessage()).isNull();

        // Continue the saga
        transferSagaOrchestrator.handleDebitCompleted(transactionId);
        transferSagaOrchestrator.handleCreditCompleted(transactionId);

        assertThat(transactionRepository.findById(transactionId).orElseThrow().getStatus()).isEqualTo(TransactionStatus.COMPLETED);
    }
}

