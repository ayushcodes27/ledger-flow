package com.project.ledgerflow.dto;

import com.project.ledgerflow.entity.LedgerEntry;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public record LedgerResponse(
        UUID id,
        BigDecimal amount,
        String type,
        LocalDateTime createdAt
) {
    public static LedgerResponse fromEntity(LedgerEntry entry) {
        return new LedgerResponse(
                entry.getId(),
                entry.getAmount(),
                entry.getType().name(),
                entry.getCreatedAt()
        );
    }
}