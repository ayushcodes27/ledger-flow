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

        for (OutboxEvent event : pendingEvents) {
            try {
                String topic = event.getEventType();

                // The Kafka KEY: aggregateId keeps ordering stable per aggregate
                String key = event.getAggregateId();

                // The Kafka VALUE is the event-specific payload contract
                String value = event.getPayload();

                // Construct the record with Topic, Key, and Value
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // Inject the Idempotency Key
                record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
                record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));

                kafkaTemplate.send(record).get();

                event.setProcessed(true);
            } catch (Exception e) {
                log.error("Failed to publish outbox event [{}] to Kafka. Halting batch.", event.getId(), e);
                // Break the loop. The DB transaction will rollback, and we will retry in 5 seconds.
                // This guarantees strict ordering is maintained.
                throw new RuntimeException("Kafka publish failed, triggering rollback", e);
            }
        }

        // Save the successful ones back to the DB
        outboxEventRepository.saveAll(pendingEvents);
        log.info("Successfully relayed {} outbox events.", pendingEvents.size());
    }
}
