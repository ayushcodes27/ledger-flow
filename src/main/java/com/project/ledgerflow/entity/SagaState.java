package com.project.ledgerflow.entity;

import com.project.ledgerflow.entity.enums.SagaStepStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "saga_state")
@Getter
@Setter
@NoArgsConstructor
public class SagaState {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "transaction_id", nullable = false, unique = true)
    private Transaction transaction;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaStepStatus currentStep;

    @Column(length = 1000)
    private String errorMessage;

    @Version
    private Long version; // Optimistic locking for the SAGA state itself
}
