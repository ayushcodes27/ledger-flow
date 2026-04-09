package com.project.ledgerflow.dto;

import com.project.ledgerflow.model.Wallet;

import java.math.BigDecimal;
import java.util.UUID;

public record WalletResponse(
        UUID id,
        BigDecimal balance,
        String currency
) {
    public static WalletResponse fromEntity(Wallet wallet) {
        return new WalletResponse(
                wallet.getId(),
                wallet.getBalance(),
                wallet.getCurrency()
        );
    }
}