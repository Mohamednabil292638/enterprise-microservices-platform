package com.sangiya.orders.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id, String tenantId, BigDecimal amount, String currency, OrderStatus status, Instant updatedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(), order.getTenantId(), order.getAmount(), order.getCurrency(), order.getStatus(),
                order.getUpdatedAt());
    }
}
