## HLD
```
Inventory - search, list, view
Delivery - Notification email, SMS, status update
Payment 
Cart- add, remove
Place, track order
Admin or seller updates inventory 

Out of scope
Recommendations, reviews, ads.


Metrics: requests on order service, how many times out, general error count, when raising exception, also send to metric system via Kafka.

Holt winters algorithm 

Seller upload image: cheap s3 upload to place and give link to access. Link stored in DB to send to user along with search result

Image table
Pid imgid link 

Choose: Modular monolith with clear boundaries: why?
Faster iteration, single deploy, consistent transactions, separate module and schemas, async events + contracts to avoid tight coupling, no cross module direct DB reads.
Microservices: independent scaling and ownership but operational complexity, distributed transactions, debugging hell.

API gateway: aggregate response: catalog, pricing, inventory, reviews, Auth, rate limit, request shaping. Like for product details: catalog pricing inventory reviews

1. Identity service: login sign up, sessions/jwt, MFA optional.
2. Catalog Service: product, SKU, category, attributes, media metadata, owns product truth, not pricing/inventory
3. Search service: indexing + query, open search/elastic search, catalogue updates publish events, search indexes async, don’t block writes on indexing. DB src of truth, as elastic search need not have all products info, which can be used by other service from DB. 
4. Pricing and promotion service: base price, discount rule, coupon, promotion, dynamic pricing rules, effective price for cart
5. Inventory service: stock, warehouse, reservation model, at checkout start, reserve stock with TTL, on payment, convert reservation to deducted stock, deduct on hand and reserved, on timeout/cancel release reservation by decreasing reserved.
6. Cart Service: user carts, guest carts, price is not locked until checkout
7. Order service: order creation, order state machine, immutable order records, idempotency key per checkout attempt, same order for same key.
8. payment service: create payment intent, recover provider webhook, update payment status and publish event for shipment trigger. For monitoring, cron job work on if status is processing for more than 10-20sec, then alert.
9. Fulfillment/ shipping service: shipment creation, carrier integration, tracking.
10. Notification service: Email, SMS, via queue.

Event bus: Kafka for sync workflow
Cache for hot reads
CEN for images/videos
Observability: logs metrics tracing
Feature flag + config service


Events and Async Flow:
Product created/Updated
Priceupdated
Inventory adjusted
Cart checked out
Order created
Payment authorized/Failed/Captured
Shipment Created/Delivered

Deep Dive
strong consistency for money and stock: 
- inventory reservation(oversell), 
- Order state transition(avoid twice order placement, paid buy order missing, non idempotent order)
- Payment finality: webhook truth and not client

Eventual consistency 
- search index
- recommendation, review browse page

Inventory model
- With deduction at order creation: if payment fails, restock logic needed, can drift, creates inventory leaks.
- with deduction at payment success: reduces reservation but oversell during spikes 
- best reserve on checkout. Explained above
Don’t use redis as system of record for inventory or reservation. Can lose data, misconfig unless careful ops.
Reservation live in durable DB, and not redis. Use redis for cart, read caches like product pages, rate limit. But checkout always hit inventory DB.
Use atomic conditional update, reserve 1 unit only if available >= 1.

On SKU level
on_hand = physical present
Reserved = temporarily held for checkouts
Available = on_hand - reserved

Race condition: 
TTL swipper expires reservation at same time as payment success comes in, can do double release, on hand count increment in DB.
Fix: state machine allowed transition. Active -> consumed. Active-> expired. 
Transition with atomic conditional update. Consume only if active and expires_at > now()

Double reservation on retries
Use idempotency key = checkout_attempt_id
Inventory service stores it with reservation.
If same key comes again, return same reservation.

Stock drift from manual adjustment / warehouse updates:
Fix- Inventory adjustments are events with audit trail Inventory ledger.
Periodic reconciliation: ledger totals = current on hand
Order shipped should match decrements

Inventory: sku_id, warehouse_id, on_hand, reserved, version.
Reservation: reservation_id, sku_id, warehouse id, qty, state, expires at, idem key

Checkout flow:
 sync vs async, who owns orchestration.
Sync from order service is tight coupling and cascading failure. Higher p99 latency. 
Fix: event driven saga, checkout with idempotency key, order emits order created via outbox pattern, Inventory reserves emit stock reserved or rejected, payment confirm from webhook emits payment captured. Order transition based on events. Resilient, decoupled scalable. Cons: eventual consistency, harder debugging.

Out of order events
Order service need to handle it.

Because OOO due to diff latency of diff services
Retries back off
Partitions, consumer lag, 
Kafka order within partition but cross keys or wrong partitioning break it. 

Paymentcaptured(123) event first if webhook is fast.
Stockreserved(123) later.

If logic assumes order: Can mark order confirmed even though stock isn’t reserved yet or reject. 

Duplicated events 
Kafka at least once delivered, consumer processes event, crashes before committing offset, so duplicates

Can decrement inventory twice
Send duplicate notifications 
Move order state twice if transition aren’t guarded

Sol: idempotent consumer, each event has unique event id, and or a deterministic key(payment intent id, reservation id), consumer stores processed event id or updates via conditional rules.
Common pattern: 
- inbox table like outbox but for consuming processed events(event_id pk) - if insert fails already processed skip.
- idempotent writes: see status to captured if current not captured
- transition order only if in expected state
- unique constraint ie one pagmentcaptured per payment intent id

Partial progress stuck: 
Pending payment status because webhook never reached you. 
Stock reserved but payment failed, reservation not released due to outage
Downstream consumer is down so transition never happen.

Sol: sagas need timeouts, compensation, reconciliation.

Solution: 
Outbox pattern for every service publishing events
Scenarios: DB updates but event not published. Or event published but DB not updated.

Without outbox:
Order writes order created into DB 
Tries to publish Kafka event
Crash between steps
Order created but no consumer worked on it

Or event first but crash before db commit then consumer act on order that does not exist yet.

Outbox pattern solution:
Inside same DB transaction
- write business change ie order row updated
- write an outbox row outbox events(id, type, payload, status)
Then background publisher
- Reads Pending outbox rows
- publishes to Kafka
- mark them as published

Event emission: effectively exactly once relative to DB state.

State machine with allowed transition

Solves: out of order, duplicates and invalid transition.

Created to pending stock to pending payment when stock reserved and pending payment to confirmed when payment captured, pending * to cancelled on timeout or failure, not allowed cancelled to confirmed


Out of order: if payment captured arrive while pending stock, record payment as captured but don’t confirm until stock reserved arrives.
If stock reserved arrives later state machine now move to confirmed.
Persists partial facts payment done stock done and derive state from them safely.

Table field
State
Stock status
Payment status
Updated at

Conditional update transition
Update order set state = confirmed where id=? and state in (pending payment, pending stock) and payment status = captured and stock_status = reserved.

 failed event due to bugs, schema mismatch or unrecoverable failures to dead letter queue, alert in DLQ growth. Once bug fixed, replay DLQ back into main topic or reprocess from event log offset or time range. 

Pricing Truth

Price calculated on checkout nd locked in order
Cart shows estimated, checkout produces price snapshot with items, order stores immutable unit price tax shipping.

Tax compute on address. Must compute at checkout


Oversell negative stock invariant 
Available = on hand - reserved

Search may lie due to indexing delay fix: finally query inventory live
Idempotent coupon redemption on order commit
Fraud rules / limits

Partitioning
Order by order id 
Search index sharded by category
Inventory by sku id

Edge: CDN, WAF + Bot protection.

```
## LLD Requirements  

### Functional  
- Create checkout attempt → create Order in PENDING state.  
- Reserve inventory for order items with TTL.  
- Create payment intent and capture via webhook.  
- Confirm order only when both stock reserved and payment captured.  
- Release reservations on cancel/timeout/payment failure.  
- Handle retries safely (idempotency).  

### Non-functional  
- Prevent oversell under concurrency.  
- At-least-once event delivery tolerated.  
- Strong audit trail for money/stock.  
- Operationally recoverable (DLQ + replay + reconciliation).  

## Service decomposition (LLD scope)
- Order Service (system of record for purchase)
Owns: order entity + state machine + order outbox events.  
Consumes: StockReserved/StockRejected, PaymentCaptured/PaymentFailed  

- Inventory Service (system of record for stock/reservations)   
Owns: stock, reservations + TTL expiry.  
Publishes: StockReserved/StockRejected/ReservationExpired  

- Payment Service  
Owns: payment intents + webhook handling.  
Publishes: PaymentCaptured/PaymentFailed   

You can code these as separate repos or as modules in one repo initially.  

## Entities & schema (PostgreSQL)  
### Order Service schema  
```SQL
- orders
id (UUID, PK)   
user_id (UUID)  
state (ENUM: PENDING, CONFIRMED, CANCELLED)  
stock_status (ENUM: UNKNOWN, RESERVED, REJECTED, RELEASED)  
payment_status (ENUM: NOT_STARTED, AUTHORIZED, CAPTURED, FAILED)  
idem_key (TEXT, UNIQUE) ← idempotency  
created_at, updated_at

- order_items  
id UUID   
order_id FK   
sku_id TEXT  
qty INT  
unit_price NUMERIC  
currency TEXT   

- outbox_events  
id UUID PK  
aggregate_type TEXT (e.g., order)   
aggregate_id UUID    
event_type TEXT   
payload JSONB   
status ENUM(PENDING,PUBLISHED)
created_at  

- inbox_events (dedup)  
event_id UUID PK  
received_at   

### Inventory Service schema  
- inventory   
(sku_id, warehouse_id) composite PK   
on_hand INT  
reserved INT   
version BIGINT (optional optimistic concurrency)   
Invariant: reserved >= 0, on_hand >= 0, and availability = on_hand - reserved   

- reservations   
id UUID PK   
idem_key TEXT UNIQUE ← idempotency for reserve   
order_id UUID   
sku_id TEXT   
warehouse_id TEXT   
qty INT  
state ENUM(ACTIVE,CONSUMED,RELEASED,EXPIRED)  
expires_at TIMESTAMP  
created_at  

- outbox_events, inbox_events same pattern  

### Payment Service schema   
- payment_intents   
id UUID PK   
order_id UUID UNIQUE  
provider TEXT   
provider_ref TEXT UNIQUE  
status ENUM(CREATED,CAPTURED,FAILED)   
amount NUMERIC   
currency TEXT   
created_at  
```

- Plus outbox/inbox.

## Flows (end-to-end)  
- Flow A: Happy path checkout   
1. Client → Order: POST /checkout (Idempotency-Key)  
1. Order created as PENDING  
1. Outbox: OrderCreated   
1. Inventory consumes OrderCreated   
1. Try reserve each item with atomic conditional update   
1. If all ok: create reservations (TTL) → outbox StockReserved(order_id, reservation_ids)  
1. Else: outbox StockRejected(order_id, reason)  
1. Payment intent created (either triggered by OrderCreated or by client calling Payment)    
1. Payment publishes PaymentCaptured on webhook  
1. Order consumes StockReserved + PaymentCaptured   
1. Moves state to CONFIRMED only if both facts true  
1. Outbox OrderConfirmed   
1. Fulfillment consumes OrderConfirmed   

- Flow B: Payment succeeded but stock not reserved (out-of-order / failure)  
1. Order receives PaymentCaptured first → store payment_status=CAPTURED, keep state PENDING  
1. Later if StockReserved arrives → confirm   
1. If stock rejected/timeout → cancel + initiate refund/void flow    

- Flow C: Timeout / zombie prevention   
1. Order has deadline: if still PENDING after X minutes → cancel and request Inventory release + void payment (if captured, refund)  
1. Inventory sweeper expires reservations by expires_at   

## Concurrency: the core correctness    
### Atomic conditional update (Inventory Reserve)  

This is the key line (SQL idea):  
“Increase reserved by qty only if (on_hand - reserved) >= qty”   

```SQL
In PostgreSQL:  
UPDATE inventory
SET reserved = reserved + :qty
WHERE sku_id=:sku AND warehouse_id=:wh
  AND (on_hand - reserved) >= :qty;

If updated row count = 1 → reservation can proceed

If 0 → insufficient availability (no oversell)

Why this is atomic: DB executes it as one statement, safe under concurrency.
```

### Idempotency (Reserve + Checkout)  

orders.idem_key unique ensures one order per checkout attempt.  
reservations.idem_key unique ensures one reserve action per attempt.  

If retry happens: you return existing order/reservation.  

### Outbox & Inbox  
Outbox prevents “DB updated but event lost”.  
Inbox prevents “event processed twice”.  

## State machine (Order Service)   
Facts  
payment_status  
stock_status  
Derived valid transitions  
PENDING → CONFIRMED only if payment_status=CAPTURED AND stock_status=RESERVED  
PENDING → CANCELLED on stock rejected / payment failed / timeout  
CONFIRMED never goes back (except separate returns/refunds domain)  

Implementation: apply events and then recompute allowed state.  

## Python implementation (patterns + code)  

Below is a clean, production-style skeleton using:  
Domain Model + State Machine  
Repository  
Unit of Work  
Outbox publisher   
Inbox dedup   
Command/Event handlers  

```python 

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Any
from uuid import UUID, uuid4
from datetime import datetime, timedelta


# ---------- Domain Enums ----------
class OrderState(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class StockStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    RESERVED = "RESERVED"
    REJECTED = "REJECTED"
    RELEASED = "RELEASED"


class PaymentStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    CAPTURED = "CAPTURED"
    FAILED = "FAILED"


# ---------- Domain Events ----------
@dataclass(frozen=True)
class DomainEvent:
    event_id: UUID
    occurred_at: datetime


@dataclass(frozen=True)
class OrderCreated(DomainEvent):
    order_id: UUID
    user_id: UUID
    items: list[dict[str, Any]]  # sku_id, qty, unit_price, currency


@dataclass(frozen=True)
class StockReserved(DomainEvent):
    order_id: UUID
    reservation_ids: list[UUID]


@dataclass(frozen=True)
class StockRejected(DomainEvent):
    order_id: UUID
    reason: str


@dataclass(frozen=True)
class PaymentCaptured(DomainEvent):
    order_id: UUID
    provider_ref: str


@dataclass(frozen=True)
class PaymentFailed(DomainEvent):
    order_id: UUID
    reason: str


@dataclass(frozen=True)
class OrderConfirmed(DomainEvent):
    order_id: UUID


@dataclass(frozen=True)
class OrderCancelled(DomainEvent):
    order_id: UUID
    reason: str


# ---------- Domain Aggregate ----------
class Order:
    def __init__(self, order_id: UUID, user_id: UUID, idem_key: str):
        self.id = order_id
        self.user_id = user_id
        self.idem_key = idem_key

        self.state: OrderState = OrderState.PENDING
        self.stock_status: StockStatus = StockStatus.UNKNOWN
        self.payment_status: PaymentStatus = PaymentStatus.NOT_STARTED

        self.items: list[dict[str, Any]] = []
        self._pending_events: list[DomainEvent] = []

    @staticmethod
    def create(user_id: UUID, idem_key: str, items: list[dict[str, Any]]) -> "Order":
        o = Order(order_id=uuid4(), user_id=user_id, idem_key=idem_key)
        o.items = items[:]
        o._raise(OrderCreated(uuid4(), datetime.utcnow(), o.id, user_id, o.items))
        return o

    def apply(self, evt: DomainEvent) -> None:
        # Event handler updates facts first
        if isinstance(evt, StockReserved):
            self.stock_status = StockStatus.RESERVED
        elif isinstance(evt, StockRejected):
            self.stock_status = StockStatus.REJECTED
        elif isinstance(evt, PaymentCaptured):
            self.payment_status = PaymentStatus.CAPTURED
        elif isinstance(evt, PaymentFailed):
            self.payment_status = PaymentStatus.FAILED

        # Then transition state deterministically
        self._recompute_state()

    def _recompute_state(self) -> None:
        if self.state == OrderState.CANCELLED:
            return  # terminal

        # cancel conditions
        if self.stock_status == StockStatus.REJECTED:
            self._cancel("STOCK_REJECTED")
            return
        if self.payment_status == PaymentStatus.FAILED:
            self._cancel("PAYMENT_FAILED")
            return

        # confirm condition
        if self.payment_status == PaymentStatus.CAPTURED and self.stock_status == StockStatus.RESERVED:
            if self.state != OrderState.CONFIRMED:
                self.state = OrderState.CONFIRMED
                self._raise(OrderConfirmed(uuid4(), datetime.utcnow(), self.id))

    def _cancel(self, reason: str) -> None:
        if self.state != OrderState.CANCELLED:
            self.state = OrderState.CANCELLED
            self._raise(OrderCancelled(uuid4(), datetime.utcnow(), self.id, reason))

    def _raise(self, evt: DomainEvent) -> None:
        self._pending_events.append(evt)

    def pull_events(self) -> list[DomainEvent]:
        evts = self._pending_events[:]
        self._pending_events.clear()
        return evts


# ---------- Ports (Repositories / UoW / Messaging) ----------
class OrderRepository(Protocol):
    def get_by_idem_key(self, idem_key: str) -> Order | None: ...
    def get(self, order_id: UUID) -> Order | None: ...
    def save(self, order: Order) -> None: ...


class InboxRepository(Protocol):
    def already_processed(self, event_id: UUID) -> bool: ...
    def mark_processed(self, event_id: UUID) -> None: ...


class OutboxRepository(Protocol):
    def add(self, aggregate_type: str, aggregate_id: UUID, event: DomainEvent) -> None: ...


class UnitOfWork(Protocol):
    orders: OrderRepository
    inbox: InboxRepository
    outbox: OutboxRepository
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


# ---------- Application Services ----------
class CheckoutService:
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    def checkout(self, user_id: UUID, idem_key: str, items: list[dict[str, Any]]) -> UUID:
        """
        Idempotent: same idem_key returns same order.
        """
        existing = self.uow.orders.get_by_idem_key(idem_key)
        if existing:
            return existing.id

        order = Order.create(user_id=user_id, idem_key=idem_key, items=items)
        self.uow.orders.save(order)

        for evt in order.pull_events():
            self.uow.outbox.add("order", order.id, evt)

        self.uow.commit()
        return order.id


class OrderEventConsumer:
    """
    Consumes StockReserved/Rejected and PaymentCaptured/Failed.
    Safe under duplicates via inbox dedup.
    """
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    def handle(self, evt: DomainEvent) -> None:
        if self.uow.inbox.already_processed(evt.event_id):
            return

        order_id = getattr(evt, "order_id", None)
        if not isinstance(order_id, UUID):
            # poison message - send to DLQ in real system
            return

        order = self.uow.orders.get(order_id)
        if not order:
            # out-of-order with creation; either retry later or DLQ
            return

        order.apply(evt)
        self.uow.orders.save(order)

        # mark processed + emit resulting events in same transaction scope
        self.uow.inbox.mark_processed(evt.event_id)
        for new_evt in order.pull_events():
            self.uow.outbox.add("order", order.id, new_evt)

        self.uow.commit()

```
