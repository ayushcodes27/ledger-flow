package com.project.ledgerflow.service;

import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

@Service
public class IdempotencyService {
    private final StringRedisTemplate redisTemplate;
    private static final String IDEMPOTENCY_PREFIX = "ledger:processed_event:";
    private static final Duration TTL = Duration.ofDays(7);

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Atomically checks if an event ID exists, and if not, sets it.
     * @return true if the event is NEW and should be processed. false if it's a DUPLICATE.
     */
    public boolean checkAndSetEvent(String eventId) {
        String key = IDEMPOTENCY_PREFIX + eventId;
        // setIfAbsent is atomic. It prevents race conditions if two consumer threads
        // read the exact same duplicate message simultaneously.
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(key, "PROCESSED", TTL);
        return Boolean.TRUE.equals(isNew);
    }

    public void removeEvent(String eventId) {
        redisTemplate.delete(IDEMPOTENCY_PREFIX + eventId);
    }
}
