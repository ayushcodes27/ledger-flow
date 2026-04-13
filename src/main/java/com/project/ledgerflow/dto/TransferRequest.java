package com.project.ledgerflow.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferRequest(
        @NotNull(message = "Source Wallet ID is required")
        UUID sourceWalletId,

        @NotNull(message = "Target Wallet ID is required")
        UUID targetWalletId,

        @NotNull(message = "Amount is required")
        @DecimalMin(value = "0.01", message = "Transfer amount must be greater than zero")
        BigDecimal amount
){
}
