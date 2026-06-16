package com.project.ledgerflow.scheduler;

import com.project.ledgerflow.entity.OutboxEvent;
import com.project.ledgerflow.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@ConditionalOnProperty(name = "ledger.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelayJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayJob.class);
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayJob(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // RED FLAG 3 & 4 FIX: Removed @Transactional from the main relay method.
    // This prevents holding a DB connection hostage during network I/O (Kafka send).
    // Also switches to per-message processing to prevent "Resend Storms".
    @Scheduled(fixedDelay = 5000)
    public void relayEventsToKafka() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findTop50ByProcessedFalseOrderByCreatedAtAsc();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Relaying {} outbox events to Kafka...", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                // Process each event in its own small, independent transaction
                relaySingleEvent(event);
            } catch (Exception e) {
                log.error("Failed to relay outbox event [{}]. Stopping batch to maintain order.", event.getId(), e);
                break; // Stop processing this batch to preserve strict ordering
            }
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void relaySingleEvent(OutboxEvent event) {
        String topic = event.getEventType();
        String key = event.getAggregateId();
        String value = event.getPayload();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));

        // Synchronous send inside the message-specific transaction.
        // This ensures the DB is updated ONLY if Kafka ACKs the message.
        try {
            kafkaTemplate.send(record).get(); // Block until ACK
            event.setProcessed(true);
            outboxEventRepository.save(event);
            log.debug("Successfully relayed event [{}] to Kafka", event.getId());
        } catch (Exception e) {
            throw new RuntimeException("Kafka send failed for event: " + event.getId(), e);
        }
    }
}

