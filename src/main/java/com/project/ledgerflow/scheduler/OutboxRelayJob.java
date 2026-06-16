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
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletionException;
import java.util.ArrayList;

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

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayEventsToKafka() {
        // Fetching in batches of 50 to prevent JVM OutOfMemory errors
        List<OutboxEvent> pendingEvents = outboxEventRepository.findTop50ByProcessedFalseOrderByCreatedAtAsc();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Relaying {} outbox events to Kafka...", pendingEvents.size());

        List<CompletableFuture<SendResult<String, String>>> publishFutures = new ArrayList<>();

        for (OutboxEvent event : pendingEvents) {
            String topic = event.getEventType();
            String key = event.getAggregateId();
            String value = event.getPayload();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Inject the Idempotency Key
            record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));

            // Fire asynchronously
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            publishFutures.add(future);

            // Update JPA entity state in memory. 
            // The DB transaction won't commit until the method exits successfully.
            event.setProcessed(true); 
        }

        try {
            // Wait for the entire batch of network I/O to complete concurrently.
            // This blocks the scheduler thread ONCE per batch, rather than ONCE per message.
            CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();
            
            // If we reach here, ALL messages in the batch were successfully acked by the KRaft cluster.
            log.debug("Successfully published batch of {} events.", pendingEvents.size());

        } catch (CompletionException e) {
            // A CompletionException wraps the actual Kafka producer exception.
            log.error("Failed to publish outbox batch. Halting and triggering DB rollback to maintain strict ordering.", e.getCause());
            
            // Throwing this RuntimeException triggers the Spring @Transactional rollback.
            // The events remain 'PENDING' in the DB and will be retried in strict order.
            throw new RuntimeException("Kafka batch publish failed, triggering rollback", e.getCause());
        }

        // Save the successful ones back to the DB
        outboxEventRepository.saveAll(pendingEvents);
        log.info("Successfully relayed {} outbox events.", pendingEvents.size());
    }
}
