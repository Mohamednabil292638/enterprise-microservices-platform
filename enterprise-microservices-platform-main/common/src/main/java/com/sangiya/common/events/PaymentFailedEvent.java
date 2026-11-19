package com.sangiya.common.events;

import java.time.Instant;
import java.util.UUID;

/** Published by payment-service; consumed by order-service to trigger the saga's compensating action. */
public record PaymentFailedEvent(UUID eventId, UUID orderId, String reason, Instant occurredAt) {
}
