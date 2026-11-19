package com.sangiya.common.events;

import java.time.Instant;
import java.util.UUID;

/** Published by payment-service; consumed by order-service to complete the saga's forward path. */
public record PaymentSucceededEvent(UUID eventId, UUID orderId, UUID paymentId, Instant occurredAt) {
}
