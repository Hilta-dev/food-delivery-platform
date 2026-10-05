# Distributed Food Delivery Platform (DSA612S Assignment 2)

Ballerina microservices + Kafka (KRaft) + MongoDB, orchestrated with Docker Compose.

## Services
| Service | Port | Owns |
|---|---|---|
| customer | 9091 | accounts, addresses, order history (from `orders.created`) |
| restaurant | 9092 | menus, inventory, opening hours; decrements stock on `orders.created` |
| order | 9093 | order state machine CREATED→CONFIRMED→PREPARING→READY→OUT_FOR_DELIVERY→DELIVERED / CANCELLED |
| payment | 9094 | simulated payments (fails above `maxAmount`) |
| delivery | 9095 | drivers, assignment, live location, completion |
| notification | 9096 | multi-channel alerts (email/sms/push, simulated) |
| admin | 9097 | restaurant + delivery reports from the event stream |

## Kafka topics (3 partitions, keyed by orderId)
`orders.created`, `payments.completed`, `payments.failed`, `order.status`, `delivery.assigned`, `delivery.completed`

## Event flow
1. `POST /orders` → order saved (CREATED) → `orders.created`
2. payment consumes → `payments.completed` / `payments.failed`
3. order consumes → CONFIRMED / CANCELLED → `order.status`
4. restaurant calls `PUT /orders/{id}/status` PREPARING → READY
5. delivery consumes `order.status`=READY → assigns driver → `delivery.assigned`
6. order → OUT_FOR_DELIVERY; driver `POST /delivery/deliveries/{orderId}/complete` → `delivery.completed` → DELIVERED
7. notification + admin consume everything

## Run
```
docker compose up --build
./scripts/smoke-test.sh
```
Local dev (one service): `docker compose up kafka kafka-init mongo`, then `cd services/order && bal run` (defaults point to localhost:29092 / localhost:27017).
