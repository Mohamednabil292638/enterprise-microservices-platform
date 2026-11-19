package com.sangiya.gateway.ratelimit;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * A per-instance token-bucket rate limiter. Deliberately in-memory rather
 * than Spring Cloud Gateway's built-in {@code RedisRateLimiter} -- this
 * repo has no Redis instance to test against (see README "What was and
 * wasn't verified"). The tradeoff is real and stated up front: this limit
 * is per-gateway-instance, not shared across a multi-instance deployment.
 * A production deployment behind more than one gateway replica needs a
 * shared store (Redis, as Spring Cloud Gateway's own default assumes) for
 * the limit to mean anything cluster-wide -- swapping this class for
 * {@code RedisRateLimiter} is a drop-in change, not a redesign.
 */
@Component
public class InMemoryRateLimiter {

    private record Bucket(int tokens, Instant windowStart) {
    }

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
    private final int capacity;
    private final Duration window;

    public InMemoryRateLimiter() {
        this(20, Duration.ofSeconds(10));
    }

    public InMemoryRateLimiter(int capacity, Duration window) {
        this.capacity = capacity;
        this.window = window;
    }

    public boolean tryConsume(String key) {
        Instant now = Instant.now();
        Bucket updated = buckets.compute(key, (k, existing) -> {
            if (existing == null || Duration.between(existing.windowStart(), now).compareTo(window) >= 0) {
                return new Bucket(capacity - 1, now);
            }
            return new Bucket(existing.tokens() - 1, existing.windowStart());
        });
        return updated.tokens() >= 0;
    }
}
