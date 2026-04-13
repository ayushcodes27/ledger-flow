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
    public void executeTransfer(UUID transactionId) {
        SagaState state = sagaStateRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new RuntimeException("Saga State not found"));
        Transaction tx = state.getTransaction();

        try {
            //1: Debit Source Wallet
            state.setCurrentStep(SagaStepStatus.DEBIT_PENDING);
            sagaStateRepository.save(state);

            String debitKey = tx.getId().toString() + "-debit";
            walletService.debit(tx.getSourceWalletId(), tx.getAmount(), debitKey);

            state.setCurrentStep(SagaStepStatus.DEBIT_SUCCESS);
            sagaStateRepository.save(state);

            //2: Credit Target Wallet
            try {
                state.setCurrentStep(SagaStepStatus.CREDIT_PENDING);
                sagaStateRepository.save(state);

                String creditKey = tx.getId().toString() + "-credit";
                walletService.credit(tx.getTargetWalletId(), tx.getAmount(), creditKey);

                // SAGA Succes
                state.setCurrentStep(SagaStepStatus.COMPLETED);
                tx.setStatus(TransactionStatus.COMPLETED);
                sagaStateRepository.save(state);
                transactionRepository.save(tx);

            } catch (Exception e) {
                // 3: Compensating transaction (Refund Source)
                // If crediting the target fails , we must refund the source
                state.setCurrentStep(SagaStepStatus.COMPENSATING_DEBIT);
                state.setErrorMessage("Credit failed: " + e.getMessage() + ". Initiating refund.");
                sagaStateRepository.save(state);

                String refundKey = tx.getId().toString() + "-refund";
                walletService.credit(tx.getSourceWalletId(), tx.getAmount(), refundKey); // Refund

                state.setCurrentStep(SagaStepStatus.COMPENSATION_SUCCESS);
                tx.setStatus(TransactionStatus.FAILED);
                sagaStateRepository.save(state);
                transactionRepository.save(tx);
            }

        } catch (Exception e) {
            // If the initial debit fails, the whole transaction simply fails. No refund needed.
            state.setCurrentStep(SagaStepStatus.FAILED);
            state.setErrorMessage("Debit failed: " + e.getMessage());
            tx.setStatus(TransactionStatus.FAILED);
            sagaStateRepository.save(state);
            transactionRepository.save(tx);
        }
    }
}
