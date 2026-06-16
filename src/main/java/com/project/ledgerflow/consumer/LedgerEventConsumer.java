package com.project.ledgerflow.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.ledgerflow.entity.enums.SagaStepStatus;
import com.project.ledgerflow.service.IdempotencyService;
import com.project.ledgerflow.service.TransferSagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.function.Supplier;

@Component
@ConditionalOnProperty(name = "ledger.kafka.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class LedgerEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(LedgerEventConsumer.class);
    private final IdempotencyService idempotencyService;
    private final TransferSagaOrchestrator transferSagaOrchestrator;
    private final ObjectMapper objectMapper;

    public LedgerEventConsumer(IdempotencyService idempotencyService,
                               TransferSagaOrchestrator transferSagaOrchestrator,
                               ObjectMapper objectMapper) {
        this.idempotencyService = idempotencyService;
        this.transferSagaOrchestrator = transferSagaOrchestrator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "transfer.initiated", containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeTransferInitiated(@Payload String payload, @Header(name = "eventId") String eventId, Acknowledgment ack) {
        UUID txId = UUID.fromString(payload.trim());
        processEvent(eventId, payload, ack, 
            () -> transferSagaOrchestrator.handleTransferInitiated(txId),
            () -> transferSagaOrchestrator.isStepCompleted(txId, SagaStepStatus.INITIATED)
        );
    }

    @KafkaListener(topics = "wallet.debited", containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeWalletDebited(@Payload String payload, @Header(name = "eventId") String eventId, Acknowledgment ack) {
        UUID txId = extractTransactionId(payload);
        if (txId != null) {
            processEvent(eventId, payload, ack, 
                () -> transferSagaOrchestrator.handleDebitCompleted(txId),
                () -> transferSagaOrchestrator.isStepCompleted(txId, SagaStepStatus.DEBIT_COMPLETED)
            );
        } else {
            log.info("Skipping wallet.debited event with no transactionId");
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = "wallet.credited", containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeWalletCredited(@Payload String payload, @Header(name = "eventId") String eventId, Acknowledgment ack) {
        UUID txId = extractTransactionId(payload);
        if (txId != null) {
            processEvent(eventId, payload, ack, 
                () -> transferSagaOrchestrator.handleCreditCompleted(txId),
                () -> transferSagaOrchestrator.isStepCompleted(txId, SagaStepStatus.CREDIT_COMPLETED)
            );
        } else {
            log.info("Skipping wallet.credited event with no transactionId");
            ack.acknowledge();
        }
    }

    private void processEvent(String eventId, String payload, Acknowledgment ack, Runnable action, Supplier<Boolean> isAlreadyDone) {
        log.info("Received event [{}] with payload [{}]", eventId, payload);

        if (!idempotencyService.checkAndSetEvent(eventId)) {
            if (isAlreadyDone.get()) {
                log.info("Duplicate event [{}] detected but work is already confirmed in DB. Acknowledging.", eventId);
                ack.acknowledge();
                return;
            }
            log.warn("Duplicate event detected [{}]. Work NOT yet completed in DB. Potential crash recovery needed. Rejecting for retry.", eventId);
            return;
        }

        try {
            action.run();
            ack.acknowledge();
            log.info("Successfully processed event [{}]", eventId);
        } catch (Exception e) {
            log.error("Failed to process event [{}].", eventId, e);
            
            if (isPermanentFailure(e)) {
                log.error("Permanent failure detected for event [{}]. ACKing to stop poison pill loop.", eventId);
                ack.acknowledge(); 
            } else {
                idempotencyService.removeEvent(eventId);
                throw e; 
            }
        }
    }

    private boolean isPermanentFailure(Exception e) {
        return e instanceof IllegalArgumentException || e.getMessage().contains("Permanent");
    }

    private UUID extractTransactionId(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            JsonNode txNode = node.get("transactionId");
            if (txNode != null && !txNode.isNull()) {
                return UUID.fromString(txNode.asText());
            }
        } catch (Exception e) {
            log.error("Failed to extract transactionId from payload: {}", payload, e);
        }
        return null;
    }
}

