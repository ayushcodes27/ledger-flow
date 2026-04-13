package com.project.ledgerflow.entity.enums;

public enum SagaStepStatus {
    STARTED,
    DEBIT_PENDING,
    DEBIT_SUCCESS,
    CREDIT_PENDING,
    CREDIT_SUCCESS,
    COMPENSATING_DEBIT,
    COMPENSATION_SUCCESS,
    COMPLETED,
    FAILED
}