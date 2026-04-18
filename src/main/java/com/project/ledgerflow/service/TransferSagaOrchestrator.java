package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.SagaState;
import com.project.ledgerflow.entity.Transaction;
import com.project.ledgerflow.entity.OutboxEvent;
import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.entity.enums.TransactionStatus;
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
        this.walletService =  walletService;
    }

    @Transactional
    public UUID initiateTransfer(UUID sourceWalletId, UUID targetWalletId, BigDecimal amount){
        log.info("Initiating transfer: sourceWalletId={}, targetWalletId={}, amount={}",
                sourceWalletId, targetWalletId, amount);

        // 1. Create the permanent Transaction record
        Transaction tx = new Transaction();
        tx.setSourceWalletId(sourceWalletId);
        tx.setTargetWalletId(targetWalletId);
        tx.setAmount(amount);
        tx.setStatus(TransactionStatus.PENDING);
        transactionRepository.save(tx);
        log.info("Transfer transaction persisted: transactionId={}, status={}", tx.getId(), tx.getStatus());

        SagaState state = new SagaState();
        state.setTransaction(tx);
        state.setCurrentStep(SagaStepStatus.STARTED);
        sagaStateRepository.save(state);
        log.info("Saga state persisted: transactionId={}, sagaStateId={}, step={}",
                tx.getId(), state.getId(), state.getCurrentStep());

        saveTransferInitiatedEvent(tx.getId());
        log.info("Transfer initiation complete: transactionId={}", tx.getId());

        return tx.getId();
    }
    public TransferExecutionResult executeTransfer(UUID transactionId) {
        log.info("Executing transfer saga for transactionId={}", transactionId);
        TransferExecutionCommand command = transactionRepository.findExecutionCommandById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));
        SagaState state = sagaStateRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new RuntimeException("Saga State not found for transaction: " + transactionId));

        try {
            //1: Debit Source Wallet
            state = saveState(state, SagaStepStatus.DEBIT_PENDING, null);

            String debitKey = command.transactionId().toString() + "-debit";
            walletService.debit(command.sourceWalletId(), command.amount(), debitKey);

            state = saveState(state, SagaStepStatus.DEBIT_SUCCESS, null);

            //2: Credit Target Wallet
            try {
                state = saveState(state, SagaStepStatus.CREDIT_PENDING, null);

                String creditKey = command.transactionId().toString() + "-credit";
                walletService.credit(command.targetWalletId(), command.amount(), creditKey);

                // SAGA Succes
                state = saveState(state, SagaStepStatus.COMPLETED, null);
                transactionRepository.updateStatusById(command.transactionId(), TransactionStatus.COMPLETED);
                log.info("Transfer saga completed successfully for transactionId={}", command.transactionId());
                return new TransferExecutionResult(command.transactionId(), TransactionStatus.COMPLETED, null);

            } catch (Exception e) {
                // 3: Compensating transaction (Refund Source)
                // If crediting the target fails , we must refund the source
                state = saveState(
                        state,
                        SagaStepStatus.COMPENSATING_DEBIT,
                        "Credit failed: " + e.getMessage() + ". Initiating refund."
                );

                String refundKey = command.transactionId().toString() + "-refund";
                walletService.credit(command.sourceWalletId(), command.amount(), refundKey); // Refund

                state = saveState(state, SagaStepStatus.COMPENSATION_SUCCESS, null);
                transactionRepository.updateStatusById(command.transactionId(), TransactionStatus.FAILED);
                log.error("Transfer saga failed during credit. Compensation succeeded for transactionId={}",
                        command.transactionId(), e);
                return new TransferExecutionResult(command.transactionId(), TransactionStatus.FAILED, state.getErrorMessage());
            }

        } catch (Exception e) {
            // If the initial debit fails, the whole transaction simply fails. No refund needed.
            state = saveState(state, SagaStepStatus.FAILED, "Debit failed: " + e.getMessage());
            transactionRepository.updateStatusById(command.transactionId(), TransactionStatus.FAILED);
            log.error("Transfer saga failed during debit for transactionId={}", command.transactionId(), e);
            return new TransferExecutionResult(command.transactionId(), TransactionStatus.FAILED, state.getErrorMessage());
        }
    }

    private SagaState saveState(SagaState state, SagaStepStatus step, String errorMessage) {
        state.setCurrentStep(step);
        state.setErrorMessage(truncate(errorMessage, 1000));
        return sagaStateRepository.save(state);
    }

    private void saveTransferInitiatedEvent(UUID transactionId) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(transactionId.toString());
        event.setAggregateType("TRANSFER");
        event.setEventType("transfer.initiated");
        event.setPayload(transactionId.toString());
        outboxEventRepository.save(event);
        log.info("Outbox event persisted: eventId={}, aggregateId={}, eventType={}, processed={}",
                event.getId(), event.getAggregateId(), event.getEventType(), event.isProcessed());
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
