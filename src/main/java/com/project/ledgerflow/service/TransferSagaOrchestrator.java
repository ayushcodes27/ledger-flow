package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.SagaState;
import com.project.ledgerflow.entity.Transaction;
import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.repository.SagaStateRepository;
import com.project.ledgerflow.repository.TransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class TransferSagaOrchestrator {
    private final TransactionRepository transactionRepository;
    private final SagaStateRepository sagaStateRepository;
    private final WalletService walletService;

    public TransferSagaOrchestrator(TransactionRepository transactionRepository,
                                    SagaStateRepository sagaStateRepository,
                                    WalletService walletService) {
        this.transactionRepository = transactionRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.walletService =  walletService;
    }

    @Transactional
    public UUID initiateTransfer(UUID sourceWalletId, UUID targetWalletId, BigDecimal amount){

        // 1. Create the permanent Transaction record
        Transaction tx = new Transaction();
        tx.setSourceWalletId(sourceWalletId);
        tx.setTargetWalletId(targetWalletId);
        tx.setAmount(amount);
        tx.setStatus(TransactionStatus.PENDING);
        transactionRepository.save(tx);

        SagaState state = new SagaState();
        state.setTransaction(tx);
        state.setCurrentStep(SagaStepStatus.STARTED);
        sagaStateRepository.save(state);

        return tx.getId();
    }
    public TransferExecutionResult executeTransfer(UUID transactionId) {
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
                return new TransferExecutionResult(command.transactionId(), TransactionStatus.FAILED, state.getErrorMessage());
            }

        } catch (Exception e) {
            // If the initial debit fails, the whole transaction simply fails. No refund needed.
            state = saveState(state, SagaStepStatus.FAILED, "Debit failed: " + e.getMessage());
            transactionRepository.updateStatusById(command.transactionId(), TransactionStatus.FAILED);
            return new TransferExecutionResult(command.transactionId(), TransactionStatus.FAILED, state.getErrorMessage());
        }
    }

    private SagaState saveState(SagaState state, SagaStepStatus step, String errorMessage) {
        state.setCurrentStep(step);
        state.setErrorMessage(truncate(errorMessage, 1000));
        return sagaStateRepository.save(state);
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
