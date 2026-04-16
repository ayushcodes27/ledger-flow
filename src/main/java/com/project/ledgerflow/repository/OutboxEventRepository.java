package com.project.ledgerflow.repository;

import com.project.ledgerflow.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // will use this in the next step to fetch events that need to go to Kafka
    // SELECT *
    // FROM outbox_events
    // WHERE processed = false
    // ORDER BY created_at ASC;
    List<OutboxEvent> findByProcessedFalseOrderByCreatedAtAsc();
}
