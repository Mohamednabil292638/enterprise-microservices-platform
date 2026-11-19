package com.sangiya.orders.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sangiya.common.events.PaymentSucceededEvent;
import com.sangiya.orders.outbox.ProcessedEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"order.created", "payment.succeeded", "payment.failed"})
@DirtiesContext
class OrderSagaIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private Jwt testJwt() {
        return org.springframework.security.oauth2.jwt.Jwt.withTokenValue("test-token")
                .header("alg", "none")
                .claim("tid", "tenant-a")
                .claim("sub", "test-user")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .build();
    }

    private KafkaTemplate<String, String> paymentEventsProducer() {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(pf);
    }

    @Test
    void confirmingAnOrderViaAPaymentSucceededEventCompletesTheSaga() throws Exception {
        String body = """
                {"amount": 25.00, "currency": "USD"}""";

        String createdJson = mockMvc.perform(post("/api/orders")
                        .with(jwt().jwt(testJwt()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

        UUID orderId = UUID.fromString(objectMapper.readTree(createdJson).get("id").asText());

        // The order starts PENDING -- the saga hasn't completed yet.
        mockMvc.perform(get("/api/orders/" + orderId).with(jwt().jwt(testJwt())))
                .andExpect(status().isOk());

        // Simulate payment-service publishing the success event that
        // completes the saga's forward path.
        UUID eventId = UUID.randomUUID();
        PaymentSucceededEvent event = new PaymentSucceededEvent(eventId, orderId, UUID.randomUUID(), Instant.now());
        KafkaTemplate<String, String> producer = paymentEventsProducer();
        producer.send("payment.succeeded", eventId.toString(), objectMapper.writeValueAsString(event)).get();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> mockMvc.perform(
                        get("/api/orders/" + orderId).with(jwt().jwt(testJwt())))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.status")
                        .value("CONFIRMED")));

        // Redelivering the exact same event (at-least-once semantics) must
        // not double-process -- assert exactly one ProcessedEvent row.
        producer.send("payment.succeeded", eventId.toString(), objectMapper.writeValueAsString(event)).get();
        await().pollDelay(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(processedEventRepository.count()).isEqualTo(1));
    }

    @Test
    void createOrderIsRejectedWithoutAValidToken() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"amount": 10.00, "currency": "USD"}"""))
                .andExpect(status().isUnauthorized());
    }
}
