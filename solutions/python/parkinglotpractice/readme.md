# Parking Lot

## Must-Ask Clarifications (Interview Focus)
1. Parking Policy  
  Are vehicles restricted to spot type? (S → small only? L → large only)  
  Can motorcycles park in car spots?  
  Are there special rules for handicapped/EV?  
1. Billing Rules  
  Flat vs hourly billing?  
  Pricing per spot type?
  Grace period or penalty after max time?  
1. Spot Dimensions  
  Do spots have width and height constraints?  
  Are vehicles categorized by dimensions or fixed types?
1. Levels & Navigation  
  Are levels equal capacity?  
  Do we fill lowest level first?  
1. Concurrency / Real-world
  Simultaneous entry?
  Multiple gates?
  Multiple exit billing counters?
1. Reservation
  Are spots reservable?


## Capabilities:
Vehicle enters → Identify nearest appropriate spot
Release spot on exit, Calculate bill on exit, Grace period or penalty after max time?
The system should handle multiple entry and exit points and support concurrent access.
Extensible for new vehicle types and new levels
Fast spot lookup


## Error Handling
- what if all spots are full

## Scope Boundaries
- Whats in scope? whole flow or any specific piece like parking and exit?

## Entities
- ParkingLot  
  * Owns: levels, SpotAllocationStrategy, BillingService, tickets: Dict[ticket_id, ParkingTicket] active and closed.
  * Concurrency: tickets_lock for ticket map, Allocation/release locking happens inside Level  
  * Responsibilities: park(vehicle), exit(ticket_id), register_display_board(board)
- Level: Spot management on that level  
  * Owns: spots: Dict[spot_id, ParkingSpot], Free_heaps: Dict[SpotType, MinHeap[(distance, spot_id)]] (fast lookup),
  * lock, observers: List[LevelObserver]
  * Responsibilities: reserve_best_spot(vehicle) -> Optional[ParkingSpot], release_spot(spot_id), subscribe(observer) + _notify()
- ParkingSpot: Know type, availability, can_fit(vehicle)  
  * spot_id, spot_type, distance, occupied_by  
  * is_free(), can_fit(vehicle) -> bool  
  * Occupancy truth lives here (occupied_by), not in multiple places.   
- Vehicle: VehicleType + size  
   * plate, vtype  
   * VehicleFactory.create(plate, vtype)  
- BillingService: Calculates fees based on rules.  
   * strategy: BillingStrategy (HourlyBilling / FlatBilling), cfg: BillingConfig (rates, grace, penalty rules)  
   * calculate(ticket, spot_type, exit_time) -> Bill
- ParkingTicket: Vehicle, spot, entry_time  
  * ticket_id, vehicle_plate, level_id, spot_id, entry_time: datetime, exit_time: Optional[datetime]  
  * close(exit_time) (marks exit, prevents reuse): Keep ticket immutable-ish except exit_time; that’s interview-friendly.  
- DisplayBoard: Shows availability to drivers (Optional)  
  * LevelObserver.on_level_update(level_id, free_counts)  
  * react to level availability changes (screens, API, etc.)  
- SpotAllocator: Strategy pattern to pick nearest/best spot
   * allocate(levels, vehicle) -> (Level, ParkingSpot): NearestFirstAllocator
 
 Main

## Rules
Fit
Motorcycle fits [Small, Medium, Large]
Car fits [Medium, Large]
Truck fits [Large]
Bus fits [× or multiple large spots?]

billing rules:
  baseRate = X per hour
  small, medium, large rates
  gracePeriod = 10 mins
  penalty after 24 hrs
  supports flat and hourly

## Flow
EntryGate.scan(vehicle)
 → ParkingLot.assignSpot(vehicle)
   → SpotAllocator.findSpot(vehicle)
     → Level.checkAndReserveSpot
       → Spot.markOccupied
   → Ticket issued
   → DisplayBoard updated

ExitGate.validate(ticket)
 → BillingService.calculate(ticket)
 → Spot.release()
 → DisplayBoard.updated
 → Payment recorded

## Important points
Maintain separate queues per spot type or a priority structure if nearest spot first.
Thread safety when when concurrent parking/exit happens, else
- Two gates assigning same spot
- Billing updated while exit processing
- with level.lock:
    spot = free_spots.pop()
To fallback from small to large spot if vehicle small spot is filled but large is empty.

## Some class and function structure to start
from abc import ABC, abstractmethod
from datetime import datetime

class Vehicle(ABC):
    @property
    @abstractmethod
    def size(self): pass

class Spot(ABC):
    @property
    @abstractmethod
    def can_fit(self, vehicle): pass

class ParkingLot:
    def park(self, vehicle) -> "Ticket": ...
    def exit(self, ticket) -> "Bill": ...
    def get_free_spots(self): ...

## Design patterns
make sure to use design pattern like singleton, factory, strategy, observer wherever possible.

## Code
```python

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod
import heapq
import threading
import uuid
from typing import Dict, List, Optional, Tuple, Iterable


# ---------- Errors ----------
class ParkingLotError(Exception): ...
class LotFullError(ParkingLotError): ...
class InvalidTicketError(ParkingLotError): ...


# ---------- Enums ----------
class VehicleType(Enum):
    MOTORCYCLE = "MOTORCYCLE"
    CAR = "CAR"
    TRUCK = "TRUCK"


class SpotType(Enum):
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"


# ---------- Fit Rules (single source of truth) ----------
FIT_RULES: Dict[VehicleType, Tuple[SpotType, ...]] = {
    VehicleType.MOTORCYCLE: (SpotType.SMALL, SpotType.MEDIUM, SpotType.LARGE),
    VehicleType.CAR: (SpotType.MEDIUM, SpotType.LARGE),
    VehicleType.TRUCK: (SpotType.LARGE,),
}


# ---------- Domain ----------
@dataclass(frozen=True)
class Vehicle:
    plate: str
    vtype: VehicleType


class VehicleFactory:
    """Factory pattern: hides construction rules/validation."""
    @staticmethod
    def create(plate: str, vtype: VehicleType) -> Vehicle:
        if not plate:
            raise ValueError("plate required")
        return Vehicle(plate=plate, vtype=vtype)


@dataclass
class ParkingSpot:
    spot_id: str
    spot_type: SpotType
    distance: int  # "nearest" metric (lower is better)
    occupied_by: Optional[str] = None  # vehicle plate

    def is_free(self) -> bool:
        return self.occupied_by is None

    def can_fit(self, vehicle: Vehicle) -> bool:
        return self.spot_type in FIT_RULES[vehicle.vtype]


@dataclass
class ParkingTicket:
    ticket_id: str
    vehicle_plate: str
    level_id: str
    spot_id: str
    entry_time: datetime
    exit_time: Optional[datetime] = None

    def close(self, exit_time: datetime) -> None:
        self.exit_time = exit_time


@dataclass(frozen=True)
class Bill:
    ticket_id: str
    vehicle_plate: str
    amount: float
    duration: timedelta


# ---------- Observer ----------
class LevelObserver(ABC):
    @abstractmethod
    def on_level_update(self, level_id: str, free_counts: Dict[SpotType, int]) -> None: ...


class DisplayBoard(LevelObserver):
    """Observer: reacts to availability changes."""
    def __init__(self, name: str):
        self.name = name
        self.last: Dict[str, Dict[SpotType, int]] = {}

    def on_level_update(self, level_id: str, free_counts: Dict[SpotType, int]) -> None:
        # In interview: this could push to screens; here we just store state.
        self.last[level_id] = dict(free_counts)


# ---------- Billing (Strategy + Factory) ----------
@dataclass(frozen=True)
class BillingConfig:
    hourly_rate: Dict[SpotType, float]         # per hour rate
    grace_period_minutes: int = 10
    penalty_after_hours: int = 24
    penalty_multiplier: float = 2.0            # after 24h, multiply charges


class BillingStrategy(ABC):
    @abstractmethod
    def compute(self, spot_type: SpotType, duration: timedelta, cfg: BillingConfig) -> float: ...


class HourlyBilling(BillingStrategy):
    def compute(self, spot_type: SpotType, duration: timedelta, cfg: BillingConfig) -> float:
        if duration <= timedelta(minutes=cfg.grace_period_minutes):
            return 0.0

        hours = duration.total_seconds() / 3600.0
        # bill by started hour (ceil)
        billable_hours = int(hours) if hours.is_integer() else int(hours) + 1
        amount = billable_hours * cfg.hourly_rate[spot_type]

        if duration >= timedelta(hours=cfg.penalty_after_hours):
            amount *= cfg.penalty_multiplier
        return float(amount)


class FlatBilling(BillingStrategy):
    def __init__(self, flat_fee: float):
        self.flat_fee = flat_fee

    def compute(self, spot_type: SpotType, duration: timedelta, cfg: BillingConfig) -> float:
        # still respect grace, if asked
        if duration <= timedelta(minutes=cfg.grace_period_minutes):
            return 0.0
        return float(self.flat_fee)


class BillingStrategyFactory:
    @staticmethod
    def create(mode: str, *, flat_fee: float = 0.0) -> BillingStrategy:
        mode = mode.lower().strip()
        if mode == "hourly":
            return HourlyBilling()
        if mode == "flat":
            return FlatBilling(flat_fee=flat_fee)
        raise ValueError(f"Unknown billing mode: {mode}")


class BillingService:
    def __init__(self, strategy: BillingStrategy, cfg: BillingConfig):
        self._strategy = strategy
        self._cfg = cfg

    def calculate(self, ticket: ParkingTicket, spot_type: SpotType, exit_time: datetime) -> Bill:
        duration = exit_time - ticket.entry_time
        amount = self._strategy.compute(spot_type, duration, self._cfg)
        return Bill(ticket_id=ticket.ticket_id, vehicle_plate=ticket.vehicle_plate, amount=amount, duration=duration)


# ---------- Allocation (Strategy) ----------
class SpotAllocationStrategy(ABC):
    @abstractmethod
    def allocate(self, levels: Iterable["Level"], vehicle: Vehicle) -> Tuple["Level", ParkingSpot]: ...


class NearestFirstAllocator(SpotAllocationStrategy):
    """Try levels in order; within a level choose smallest feasible spot type first, by distance heap."""
    def allocate(self, levels: Iterable["Level"], vehicle: Vehicle) -> Tuple["Level", ParkingSpot]:
        for level in levels:
            spot = level.reserve_best_spot(vehicle)
            if spot:
                return level, spot
        raise LotFullError("No suitable spot available")


# ---------- Level ----------
class Level:
    def __init__(self, level_id: str, spots: List[ParkingSpot]):
        self.level_id = level_id
        self._lock = threading.Lock()
        self._spots: Dict[str, ParkingSpot] = {s.spot_id: s for s in spots}

        # fast lookup: heaps per spot type
        self._free_heaps: Dict[SpotType, List[Tuple[int, str]]] = {t: [] for t in SpotType}
        for s in spots:
            if s.is_free():
                heapq.heappush(self._free_heaps[s.spot_type], (s.distance, s.spot_id))

        self._observers: List[LevelObserver] = []

    def subscribe(self, obs: LevelObserver) -> None:
        self._observers.append(obs)
        self._notify()

    def _notify(self) -> None:
        free_counts = {t: self.free_count(t) for t in SpotType}
        for obs in self._observers:
            obs.on_level_update(self.level_id, free_counts)

    def free_count(self, spot_type: SpotType) -> int:
        # heap may contain stale entries; count accurately by scanning only when asked
        # (kept cheap by being called on changes only via _notify)
        return sum(1 for s in self._spots.values() if s.spot_type == spot_type and s.is_free())

    def get_spot(self, spot_id: str) -> ParkingSpot:
        return self._spots[spot_id]

    def reserve_best_spot(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        # Concurrency boundary: lock inside the level.
        with self._lock:
            for stype in FIT_RULES[vehicle.vtype]:
                heap = self._free_heaps[stype]
                while heap:
                    _, sid = heapq.heappop(heap)
                    spot = self._spots[sid]
                    if spot.is_free() and spot.can_fit(vehicle):
                        spot.occupied_by = vehicle.plate
                        self._notify()
                        return spot
                    # stale heap entry -> continue
            return None

    def release_spot(self, spot_id: str) -> None:
        with self._lock:
            spot = self._spots.get(spot_id)
            if not spot:
                raise ValueError("Unknown spot")
            spot.occupied_by = None
            heapq.heappush(self._free_heaps[spot.spot_type], (spot.distance, spot.spot_id))
            self._notify()


# ---------- Singleton ParkingLot ----------
class ParkingLot:
    _instance: Optional["ParkingLot"] = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        # Singleton pattern
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, levels: Optional[List[Level]] = None,
                 allocator: Optional[SpotAllocationStrategy] = None,
                 billing: Optional[BillingService] = None):
        # Avoid re-init in singleton
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True

        self._levels: List[Level] = levels or []
        self._allocator = allocator or NearestFirstAllocator()
        self._billing = billing or BillingService(
            strategy=BillingStrategyFactory.create("hourly"),
            cfg=BillingConfig(hourly_rate={SpotType.SMALL: 10, SpotType.MEDIUM: 15, SpotType.LARGE: 25}),
        )

        self._tickets_lock = threading.Lock()
        self._tickets: Dict[str, ParkingTicket] = {}  # ticket_id -> ticket

    def add_level(self, level: Level) -> None:
        self._levels.append(level)

    def register_display_board(self, board: DisplayBoard) -> None:
        for level in self._levels:
            level.subscribe(board)

    def park(self, vehicle: Vehicle) -> ParkingTicket:
        level, spot = self._allocator.allocate(self._levels, vehicle)
        ticket = ParkingTicket(
            ticket_id=str(uuid.uuid4()),
            vehicle_plate=vehicle.plate,
            level_id=level.level_id,
            spot_id=spot.spot_id,
            entry_time=datetime.utcnow(),
        )
        with self._tickets_lock:
            self._tickets[ticket.ticket_id] = ticket
        return ticket

    def exit(self, ticket_id: str) -> Bill:
        with self._tickets_lock:
            ticket = self._tickets.get(ticket_id)
        if not ticket or ticket.exit_time is not None:
            raise InvalidTicketError("Invalid or already closed ticket")

        exit_time = datetime.utcnow()
        ticket.close(exit_time)

        level = next((l for l in self._levels if l.level_id == ticket.level_id), None)
        if not level:
            raise InvalidTicketError("Level not found")

        spot = level.get_spot(ticket.spot_id)
        bill = self._billing.calculate(ticket, spot.spot_type, exit_time)
        level.release_spot(ticket.spot_id)
        return bill


# ---------- Gates (thin wrappers, good interview separation) ----------
class EntryGate:
    def __init__(self, gate_id: str, lot: ParkingLot):
        self.gate_id = gate_id
        self.lot = lot

    def scan_and_park(self, vehicle: Vehicle) -> ParkingTicket:
        return self.lot.park(vehicle)


class ExitGate:
    def __init__(self, gate_id: str, lot: ParkingLot):
        self.gate_id = gate_id
        self.lot = lot

    def scan_and_exit(self, ticket_id: str) -> Bill:
        return self.lot.exit(ticket_id)


```





