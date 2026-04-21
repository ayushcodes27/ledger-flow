package com.project.ledgerflow.consumer;

import com.project.ledgerflow.entity.enums.TransactionStatus;
import com.project.ledgerflow.service.IdempotencyService;
import com.project.ledgerflow.service.TransferExecutionResult;
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

@Component
@ConditionalOnProperty(name = "ledger.kafka.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class LedgerEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(LedgerEventConsumer.class);
    private final IdempotencyService idempotencyService;
    private final TransferSagaOrchestrator transferSagaOrchestrator;

    public LedgerEventConsumer(IdempotencyService idempotencyService,
                               TransferSagaOrchestrator transferSagaOrchestrator) {
        this.idempotencyService = idempotencyService;
        this.transferSagaOrchestrator = transferSagaOrchestrator;
    }

    @KafkaListener(
            topics = "transfer.initiated",
            containerFactory = "manualAckKafkaListenerContainerFactory" // Bind to our custom factory
    )
    public void consumeLedgerEvent(
            @Payload String eventPayload,
            @Header(name = "eventId", required = false) String eventId,
            Acknowledgment ack) {

        String effectiveEventId = (eventId == null || eventId.isBlank()) ? eventPayload : eventId;
        log.info("Received event [{}] with payload [{}]", effectiveEventId, eventPayload);

        // Check Idempotency
        if (!idempotencyService.checkAndSetEvent(effectiveEventId)) {
            log.warn("Duplicate event detected [{}]. Skipping processing.", effectiveEventId);
            ack.acknowledge(); // Tell Kafka we are done with it so it doesn't retry
            return;
        }

        try {
            TransferExecutionResult result =
                    transferSagaOrchestrator.executeTransfer(UUID.fromString(eventPayload.trim()));

            log.info("Transfer execution result: transactionId={}, status={}, errorMessage={}",
                    result.transactionId(), result.status(), result.errorMessage());

            if (result.status() != TransactionStatus.COMPLETED) {
                throw new RuntimeException("Transfer execution failed for transactionId="
                        + result.transactionId() + ": " + result.errorMessage());
            }

            //  Acknowledge ONLY on success
            ack.acknowledge();
            log.info("Successfully processed and acknowledged event [{}]", effectiveEventId);

        } catch (Exception e) {
            log.error("Failed to process event [{}]. Removing idempotency key and rejecting.", effectiveEventId, e);
            // Rollback Idempotency Key so a subsequent retry can attempt processing again
            idempotencyService.removeEvent(effectiveEventId);

            // DO NOT call ack.acknowledge() here.
            // By not acknowledging, Kafka will eventually re-deliver this message.
            throw e;
        }
    }
}
