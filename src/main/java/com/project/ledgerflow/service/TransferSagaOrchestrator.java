package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.SagaState;
import com.project.ledgerflow.entity.Transaction;
import com.project.ledgerflow.entity.OutboxEvent;
import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.exception.IdempotencyException;
import com.project.ledgerflow.repository.OutboxEventRepository;
import com.project.ledgerflow.repository.SagaStateRepository;
import com.project.ledgerflow.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class TransferSagaOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(TransferSagaOrchestrator.class);
    private final TransactionRepository transactionRepository;
    private final SagaStateRepository sagaStateRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final WalletService walletService;

    public TransferSagaOrchestrator(TransactionRepository transactionRepository,
            SagaStateRepository sagaStateRepository,
            OutboxEventRepository outboxEventRepository,
            WalletService walletService) {
        this.transactionRepository = transactionRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.walletService = walletService;
    }

    @Transactional
    public UUID initiateTransfer(UUID sourceWalletId, UUID targetWalletId, BigDecimal amount) {
        log.info("Initiating transfer: sourceWalletId={}, targetWalletId={}, amount={}",
                sourceWalletId, targetWalletId, amount);

        Transaction tx = new Transaction();
        tx.setSourceWalletId(sourceWalletId);
        tx.setTargetWalletId(targetWalletId);
        tx.setAmount(amount);
        tx.setStatus(TransactionStatus.PENDING);
        transactionRepository.save(tx);

        SagaState state = new SagaState();
        state.setTransaction(tx);
        state.setCurrentStep(SagaStepStatus.INITIATED);
        sagaStateRepository.save(state);

        saveOutboxEvent(tx.getId(), "transfer.initiated", tx.getId().toString());
        return tx.getId();
    }

    @Transactional(noRollbackFor = IdempotencyException.class)
    public void handleTransferInitiated(UUID txId) {
        log.info("Saga [{}]: Handling transfer.initiated", txId);
        SagaState state = getSagaState(txId);
        if (state.getCurrentStep() != SagaStepStatus.INITIATED) {
            log.info("Saga [{}]: Already past INITIATED step (current: {})", txId, state.getCurrentStep());
            return;
        }

        Transaction tx = state.getTransaction();
        try {
            String debitKey = txId.toString() + "-debit";
            walletService.debit(tx.getSourceWalletId(), tx.getAmount(), debitKey, txId);
        } catch (IdempotencyException e) {
            log.warn(
                    "Saga [{}]: Debit already processed (IdempotencyException). Money is safe. Waiting for wallet.debited event.",
                    txId);
        } catch (Exception e) {
            if (isTransient(e)) {
                log.warn("Saga [{}]: Debit encountered transient error, throwing for retry", txId, e);
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
            log.error("Saga [{}]: Debit failed permanently", txId, e);
            failSaga(state, "Debit failed: " + e.getMessage());
        }
    }

    @Transactional(noRollbackFor = IdempotencyException.class)
    public void handleDebitCompleted(UUID txId) {
        log.info("Saga [{}]: Handling wallet.debited", txId);
        SagaState state = getSagaState(txId);

        if (state.getCurrentStep().ordinal() >= SagaStepStatus.DEBIT_COMPLETED.ordinal()) {
            log.info("Saga [{}]: Already processed debit completion, current step: {}", txId, state.getCurrentStep());
            return;
        }

        updateState(state, SagaStepStatus.DEBIT_COMPLETED, null);
        Transaction tx = state.getTransaction();

        try {
            String creditKey = txId.toString() + "-credit";
            walletService.credit(tx.getTargetWalletId(), tx.getAmount(), creditKey, txId);
        } catch (IdempotencyException e) {
            log.warn("Saga [{}]: Credit already processed (IdempotencyException). Waiting for wallet.credited event.",
                    txId);
        } catch (Exception e) {
            if (isTransient(e)) {
                log.warn("Saga [{}]: Credit encountered transient error, throwing for retry", txId, e);
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
            log.error("Saga [{}]: Credit failed permanently, initiating compensation", txId, e);
            startCompensation(state, "Credit failed: " + e.getMessage());
        }
    }

    @Transactional(noRollbackFor = IdempotencyException.class)
    public void handleCreditCompleted(UUID txId) {
        log.info("Saga [{}]: Handling wallet.credited", txId);
        SagaState state = getSagaState(txId);

        if (state.getCurrentStep() == SagaStepStatus.COMPENSATING) {
            handleRefundCompleted(state);
            return;
        }

        if (state.getCurrentStep().ordinal() >= SagaStepStatus.COMPLETED.ordinal()) {
            log.info("Saga [{}]: Already completed, current step: {}", txId, state.getCurrentStep());
            return;
        }

        updateState(state, SagaStepStatus.COMPLETED, null);
        transactionRepository.updateStatusById(txId, TransactionStatus.COMPLETED);
        log.info("Saga [{}]: SUCCESSFULLY COMPLETED", txId);
    }

    public boolean isStepCompleted(UUID txId, SagaStepStatus step) {
        return sagaStateRepository.findByTransactionId(txId)
                .map(state -> {
                    if (step == SagaStepStatus.INITIATED)
                        return state.getCurrentStep().ordinal() > SagaStepStatus.INITIATED.ordinal();
                    return state.getCurrentStep().ordinal() >= step.ordinal();
                })
                .orElse(false);
    }

    private void handleRefundCompleted(SagaState state) {
        UUID txId = state.getTransaction().getId();
        log.info("Saga [{}]: Compensation (refund) completed", txId);
        updateState(state, SagaStepStatus.FAILED, "Compensated: " + state.getErrorMessage());
        transactionRepository.updateStatusById(txId, TransactionStatus.FAILED);
    }

    private void startCompensation(SagaState state, String reason) {
        UUID txId = state.getTransaction().getId();
        updateState(state, SagaStepStatus.COMPENSATING, reason);
        Transaction tx = state.getTransaction();

        try {
            String refundKey = txId.toString() + "-refund";
            walletService.credit(tx.getSourceWalletId(), tx.getAmount(), refundKey, txId);
        } catch (Exception e) {
            if (isTransient(e)) {
                log.warn("Saga [{}]: Compensation encountered transient error, throwing for retry", txId, e);
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
            log.error("Saga [{}]: Fatal error - Compensation failed permanently!", txId, e);
            failSaga(state, "Compensation failed: " + e.getMessage());
        }
    }

    private void failSaga(SagaState state, String reason) {
        updateState(state, SagaStepStatus.FAILED, reason);
        transactionRepository.updateStatusById(state.getTransaction().getId(), TransactionStatus.FAILED);
    }

    private SagaState getSagaState(UUID txId) {
        return sagaStateRepository.findByTransactionId(txId)
                .orElseThrow(() -> new RuntimeException("Saga State not found for transaction: " + txId));
    }

    private void updateState(SagaState state, SagaStepStatus step, String errorMessage) {
        state.setCurrentStep(step);
        if (errorMessage != null) {
            state.setErrorMessage(truncate(errorMessage, 1000));
        }
        sagaStateRepository.save(state);
    }

    private void saveOutboxEvent(UUID aggregateId, String eventType, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(aggregateId.toString());
        event.setAggregateType("TRANSFER");
        event.setEventType(eventType);
        event.setPayload(payload);
        outboxEventRepository.save(event);
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private boolean isTransient(Exception e) {
        if (e instanceof IllegalArgumentException) return false;
        if (e.getMessage() != null && e.getMessage().contains("Permanent")) return false;
        return true;
    }
}
