package com.project.ledgerflow.service;

import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

@Service
public class IdempotencyService {
    private final StringRedisTemplate redisTemplate;
    private static final String IDEMPOTENCY_PREFIX = "ledger:processed_event:";
    private static final Duration TTL = Duration.ofMinutes(30); // Reduced for faster crash recovery

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean checkAndSetEvent(String eventId) {
        String key = IDEMPOTENCY_PREFIX + eventId;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(key, "PROCESSING", TTL);
        return Boolean.TRUE.equals(isNew);
    }

    public void removeEvent(String eventId) {
        redisTemplate.delete(IDEMPOTENCY_PREFIX + eventId);
    }
}

