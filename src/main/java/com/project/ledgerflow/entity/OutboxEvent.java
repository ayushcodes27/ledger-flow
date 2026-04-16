package com.project.ledgerflow.entity;


import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // ID of the thing being affected (e.g., the Wallet ID or Transaction ID)
    @Column(nullable = false)
    private String aggregateId;

    // kind of entity(e.g., "WALLET", "TRANSFER")
    @Column(nullable = false)
    private String aggregateType;

    //(e.g., "WALLET_CREDITED", "TRANSFER_FAILED")
    @Column(nullable = false)
    private String eventType;

    //data we want to send to Kafka, stored as a JSON string
    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    // to be used later
    @Column(nullable = false)
    private boolean processed = false;
}
