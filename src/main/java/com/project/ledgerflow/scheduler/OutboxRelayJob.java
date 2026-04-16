package com.project.ledgerflow.scheduler;

import com.project.ledgerflow.entity.OutboxEvent;
import com.project.ledgerflow.repository.OutboxEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Component
public class OutboxRelayJob {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayJob(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Runs every 5000 milliseconds (5 seconds).
     * The @Transactional ensures that if Kafka is down and throws an exception,
     * the database updates (event.setProcessed(true)) will roll back automatically!
     */
    /*
    SELECT *
    FROM outbox_event
    WHERE processed = FALSE
    ORDER BY created_at ASC;
    */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayEventsToKafka(){
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByProcessedFalseOrderByCreatedAtAsc();

        if (pendingEvents.isEmpty()) {
            return;
        }

        for (OutboxEvent event : pendingEvents) {

            // We use the event_type as the Kafka Topic (e.g., "wallet.debited")
            // We use the aggregate_id (Wallet ID) as the Kafka Key to guarantee ordering
            // We send the JSON payload as the message
            kafkaTemplate.send(event.getEventType(), event.getAggregateId(), event.getPayload());

            // Mark as processed
            event.setProcessed(true);
        }
        outboxEventRepository.saveAll(pendingEvents);

        System.out.println("Relayed " + pendingEvents.size() + " outbox events to Kafka.");
    }
}
