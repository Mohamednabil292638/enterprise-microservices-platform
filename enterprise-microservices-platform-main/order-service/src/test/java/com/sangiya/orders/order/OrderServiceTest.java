package com.sangiya.orders.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sangiya.orders.outbox.OutboxEvent;
import com.sangiya.orders.outbox.OutboxEventRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        orderService = new OrderService(orderRepository, outboxEventRepository, objectMapper);
    }

    @Test
    void createOrderSavesBothTheOrderAndAnOutboxEventInOneCall() {
        CreateOrderRequest request = new CreateOrderRequest(new BigDecimal("49.99"), "USD");

        OrderResponse response = orderService.createOrder("tenant-a", request);

        assertThat(response.tenantId()).isEqualTo("tenant-a");
        assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
        verify(orderRepository).save(any(Order.class));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent event = captor.getValue();
        assertThat(event.getTopic()).isEqualTo("order.created");
        assertThat(event.getEventType()).isEqualTo("OrderCreated");
        assertThat(event.getPayload()).contains("tenant-a").contains("49.99");
    }

    @Test
    void getOrderThrowsWhenNoOrderMatchesTheTenant() {
        when(orderRepository.findByIdAndTenantId(any(), any())).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder("tenant-a", UUID.randomUUID()))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void confirmOrderTransitionsAnExistingOrderToConfirmed() {
        Order order = new Order(UUID.randomUUID(), "tenant-a", new BigDecimal("10.00"), "USD");
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

        orderService.confirmOrder(order.getId());

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void cancelOrderTransitionsAnExistingOrderToCancelled() {
        Order order = new Order(UUID.randomUUID(), "tenant-a", new BigDecimal("10.00"), "USD");
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

        orderService.cancelOrder(order.getId());

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }
}
