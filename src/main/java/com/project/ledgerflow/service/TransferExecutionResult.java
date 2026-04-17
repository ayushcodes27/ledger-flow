package com.project.ledgerflow.service;

import com.project.ledgerflow.entity.enums.TransactionStatus;

import java.util.UUID;

public record TransferExecutionResult(
        UUID transactionId,
        TransactionStatus status,
        String errorMessage
) {
    public boolean completed() {
        return status == TransactionStatus.COMPLETED;
    }
}
