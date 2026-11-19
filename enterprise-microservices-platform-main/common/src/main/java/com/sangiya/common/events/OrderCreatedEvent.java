package com.sangiya.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published by order-service (via the outbox) when an order is placed.
 * Consumed by payment-service to begin the saga's payment step.
 */
public record OrderCreatedEvent(
        UUID eventId, UUID orderId, String tenantId, BigDecimal amount, String currency, Instant occurredAt) {
}
