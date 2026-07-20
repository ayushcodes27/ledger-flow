package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.model.Wallet;
import com.project.ledgerflow.repository.SagaStateRepository;
import com.project.ledgerflow.repository.TransactionRepository;
import com.project.ledgerflow.repository.WalletRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ChaosIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private WalletService walletService;

    @Autowired
    private TransferSagaOrchestrator transferSagaOrchestrator;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private WalletRepository walletRepository;

    @BeforeEach
    void setUp() {
        sagaStateRepository.deleteAll();
        transactionRepository.deleteAll();
        walletRepository.deleteAll();
        
        // Ensure Redis proxy is open at the start of each test
        redisProxy.setConnectionCut(false);
    }

    @Test
    void simulateRedisCrash_ShouldThrowTransientErrorAndRecover() {
        // 1. Setup wallets
        Wallet sender = walletService.createWallet("USD");
        walletService.credit(sender.getId(), new BigDecimal("1000.00"), "seed-sender", null);
        Wallet receiver = walletService.createWallet("USD");

        // 2. Initiate Transfer
        UUID transactionId = transferSagaOrchestrator.initiateTransfer(sender.getId(), receiver.getId(), new BigDecimal("100.00"));

        // 3. CHAOS: Cut the connection to Redis
        redisProxy.setConnectionCut(true);

        // 4. Attempt to process debit
        // Since Redis is down, it can't acquire the distributed lock in WalletService
        // It should throw a RedisConnectionFailureException, which the Orchestrator will re-throw as transient.
        assertThatThrownBy(() -> transferSagaOrchestrator.handleTransferInitiated(transactionId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Redis");

        // Verify the transaction did NOT fail permanently in the DB
        var state = sagaStateRepository.findByTransactionId(transactionId).orElseThrow();
        assertThat(state.getCurrentStep()).isEqualTo(SagaStepStatus.INITIATED);
        assertThat(state.getErrorMessage()).isNull();

        // 5. RECOVERY: Restore the Redis connection
        redisProxy.setConnectionCut(false);

        // 6. Retry the processing (simulating Kafka consumer redelivery)
        transferSagaOrchestrator.handleTransferInitiated(transactionId);
        transferSagaOrchestrator.handleDebitCompleted(transactionId);
        transferSagaOrchestrator.handleCreditCompleted(transactionId);

        // 7. Assert success
        var transaction = transactionRepository.findById(transactionId).orElseThrow();
        assertThat(transaction.getStatus()).isEqualTo(TransactionStatus.COMPLETED);

        Wallet updatedSender = walletRepository.findById(sender.getId()).orElseThrow();
        Wallet updatedReceiver = walletRepository.findById(receiver.getId()).orElseThrow();
        assertThat(updatedSender.getBalance()).isEqualByComparingTo("900.00");
        assertThat(updatedReceiver.getBalance()).isEqualByComparingTo("100.00");
    }
}
