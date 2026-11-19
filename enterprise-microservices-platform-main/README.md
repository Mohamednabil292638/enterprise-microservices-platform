# enterprise-microservices-platform

A distributed-systems reference: two Spring Boot services (Java 21) coordinating an order/payment saga over Kafka, an API gateway with OAuth2/OIDC JWT security and rate limiting, the transactional outbox pattern, idempotent event consumption, and Resilience4j-guarded external calls — 17 tests, all exercising real Kafka (via an embedded in-process broker, no Docker required).

## Problem and motivation

Most "microservices reference" repos show one service and call it done. This one shows the actual hard part: two services that must agree on an outcome (was the order paid or not?) without a distributed transaction — via the choreography-based saga, outbox, and idempotency patterns real systems use instead.

## Independent, non-proprietary disclaimer

`order-service` and `payment-service` are a fabricated e-commerce domain built solely to demonstrate these patterns; no employer code or data.

## Architecture

```
enterprise-microservices-platform/
├── common/                    Shared Kafka event contracts (Java records)
├── order-service/ (:8081)         Owns orders; outbox → Kafka; OAuth2-secured API
├── payment-service/ (:8082)           Consumes orders; Resilience4j-guarded "gateway"; own outbox
├── api-gateway/ (:8080)                   Spring Cloud Gateway; JWT auth; in-memory rate limiter
└── .github/workflows/ci.yml                   mvn test across all 4 modules
```

```
POST /api/orders (via gateway, JWT required)
        │
   order-service: save Order(PENDING) + OutboxEvent(OrderCreated) — one transaction
        │
   OutboxPublisher (poller) ──▶ Kafka "order.created"
                                      │
                            payment-service: PaymentProcessor.charge()
                             (Resilience4j @Retry + @CircuitBreaker)
                                      │
                        save Payment + OutboxEvent(Succeeded/Failed)
                                      │
                     OutboxPublisher ──▶ Kafka "payment.succeeded" / "payment.failed"
                                      │
                     order-service: PaymentEventListener (idempotent)
                              confirms or cancels the order
```

## Key design decisions

- **The outbox pattern is the only way either service talks to Kafka for its own domain events.** `OrderService.createOrder` and `OrderEventListener.onOrderCreated` write their event to a local outbox table in the *same* transaction as the domain change; a separate `OutboxPublisher` poller is the only thing that calls `KafkaTemplate.send`. A crash between "save the order" and "publish the event" is impossible by construction — the event's durability is exactly the order's.
- **Idempotency via a processed-events table, not Kafka's own dedup.** At-least-once delivery means every consumer can see the same event twice; `PaymentEventListener` and `OrderEventListener` both check a `processed_events` table before acting, verified in tests by redelivering the identical event and asserting the side effect happened exactly once.
- **Two distinct payment failure modes, handled differently on purpose.** `PaymentProcessor` distinguishes a transient gateway timeout (`@Retry`-eligible) from a business decline (amount over the processing limit — a normal return value, never retried, since retrying a declined payment just gets declined again).
- **Multi-tenancy is a `tenant_id` column plus tenant-scoped queries, not a separate schema per tenant** — the cheapest model that still makes cross-tenant leakage a query bug, not an architectural gap. The tenant comes from the verified JWT's `tid` claim, never a client-supplied header.
- **The gateway's rate limiter is in-memory, not Redis-backed, and the README says so.** See "What was and wasn't verified."

## Security

Both `order-service` and `api-gateway` are OAuth2 resource servers validating JWTs — `OAUTH2_ISSUER_URI` must point at a real OIDC provider (Keycloak, Auth0, Okta) in any real deployment; there is no default, so misconfiguration fails loudly at startup rather than silently skipping validation. Tests never need a live IdP: `SecurityMockMvcRequestPostProcessors.jwt()` (order-service) and a bare `jwk-set-uri` that's never dialed (api-gateway, since `@Retry`-free JWT decoding is lazy) both avoid any network dependency.

## Testing pyramid

- **Unit tests**: `OrderServiceTest` (Mockito), `PaymentProcessorTest` (verifies retry-then-succeed and decline-without-retry), `InMemoryRateLimiterTest` (capacity, per-key isolation, window reset).
- **Integration tests against a real embedded Kafka broker** (`spring-kafka-test`'s `@EmbeddedKafka`, no Docker): `OrderSagaIntegrationTest` runs the full saga end-to-end — create an order via MockMvc, publish a real `PaymentSucceededEvent` to Kafka, assert the order transitions to `CONFIRMED`, then redeliver the same event and assert exactly one `ProcessedEvent` row. `OrderEventListenerIntegrationTest` covers the payment side symmetrically, plus the large-order decline path.
- **Gateway tests**: `GatewaySecurityTest` (health is public, API routes require auth), `RouteConfigTest` (the configured route actually exists).

## What was and wasn't verified

- All 17 tests pass locally against a real embedded Kafka broker (not mocked) — confirmed multiple times while fixing real bugs found along the way (see below).
- **Real bugs found and fixed during development, not just hypothetical:** a record accessor/static-factory name collision (`PaymentOutcome.succeeded()` couldn't coexist with the record's own `succeeded()` accessor); `KafkaTestUtils.producerProps`/`consumerProps` defaulting to `IntegerSerializer`/`Deserializer` for keys when the code sends `String` keys; a genuine test-isolation flake where two test methods sharing one embedded-broker Spring context let a later test's fresh consumer group read an *earlier* test's leftover message (fixed with `@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)`); a missing actuator dependency causing `/actuator/health` to 404 through the gateway.
- **Not run against real infrastructure:** no real Kafka cluster, no real Redis, no real OIDC provider, no real PostgreSQL (H2 stands in) — this environment has no Docker daemon available. The gateway's rate limiter is documented as in-memory (per-instance) specifically because there was no Redis to verify `RedisRateLimiter` against; swapping it in is a stated drop-in change, not a redesign.

## Development

```bash
mvn test              # all 4 modules, real embedded Kafka, no Docker needed
mvn spring-boot:run -pl order-service    # needs OAUTH2_ISSUER_URI and a running Kafka broker
mvn spring-boot:run -pl payment-service  # needs a running Kafka broker
mvn spring-boot:run -pl api-gateway      # needs OAUTH2_ISSUER_URI and order-service running
```

## License

MIT © sangiya
