package com.sangiya.payments.payment;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Simulates an external payment gateway with two distinct failure modes,
 * handled differently on purpose:
 *
 * <p>1. A transient gateway timeout ({@link PaymentGatewayUnavailableException}) --
 * the kind of failure a retry can plausibly fix, so {@code @Retry} wraps this
 * method. 2. A business decline (amount exceeds the processing limit) --
 * retrying a declined payment would just get declined again, so it's a
 * normal return value, not an exception, and never triggers a retry.
 *
 * <p>{@code @CircuitBreaker} sits alongside {@code @Retry} so that if the
 * gateway is genuinely down (not just one flaky call), repeated timeouts
 * trip the breaker and fail fast instead of retrying into a dead service.
 */
@Component
public class PaymentProcessor {

    private static final BigDecimal DECLINE_THRESHOLD = new BigDecimal("10000");

    private final Map<UUID, AtomicInteger> attemptsByOrder = new ConcurrentHashMap<>();

    @Retry(name = "paymentGateway")
    @CircuitBreaker(name = "paymentGateway")
    public PaymentOutcome charge(UUID orderId, BigDecimal amount) {
        if (amount.compareTo(DECLINE_THRESHOLD) >= 0) {
            return PaymentOutcome.declined("amount exceeds processing limit");
        }

        int attempt = attemptsByOrder.computeIfAbsent(orderId, id -> new AtomicInteger(0)).incrementAndGet();
        if (attempt == 1) {
            throw new PaymentGatewayUnavailableException("simulated transient gateway timeout");
        }
        return PaymentOutcome.success();
    }
}
