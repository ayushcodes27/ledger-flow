package com.project.ledgerflow.service;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferExecutionCommand(
        UUID transactionId,
        UUID sourceWalletId,
        UUID targetWalletId,
        BigDecimal amount
) {
}
