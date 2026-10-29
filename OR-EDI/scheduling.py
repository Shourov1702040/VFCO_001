from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple, Union, Optional


@dataclass(frozen=True)
class SlotEvent:
    """A verification event for one edge server in a specific slot."""
    es_index: int                 # 0-based index into the input F list
    slot_index: int               # 1..S (matches paper's k_{i,j})
    time_s: float                 # scheduled time (seconds from start of interval)
    frequency: int                # f_i


def frequency_wise_schedule(
    F: Sequence[Union[int, float]],
    T: float,
    *,
    start_at: float = 0.0,
    use_center_of_slot: bool = False,
    es_labels: Optional[Sequence[str]] = None,
) -> Tuple[Dict[int, List[SlotEvent]], List[SlotEvent], int, float]:
    
    if T <= 0:
        raise ValueError("T must be > 0 seconds.")

    # Convert frequencies to non-negative integers (counts within interval).
    f_int: List[int] = []
    for x in F:
        if x is None:
            f = 0
        else:
            f = int(round(float(x)))
        if f < 0:
            raise ValueError(f"Frequency must be >= 0, got {x}.")
        f_int.append(f)

    S = max(f_int) if f_int else 0
    if S == 0:
        return {}, [], 0, 0.0

    # Time step between adjacent slots (boundary-aligned)
    slot_step = T / (S - 1) if S > 1 else 0.0

    schedule_by_slot: Dict[int, List[SlotEvent]] = {k: [] for k in range(1, S + 1)}
    all_events: List[SlotEvent] = []

    for i, fi in enumerate(f_int):
        if fi <= 0:
            continue

        for j in range(fi):
            # General formula (works for all fi >= 1):
            # k_{i,j} = 1 + floor( (j + 1/2) * S / f_i )
            k = 1 + math.floor((j + 0.5) * S / fi)

            # Clamp safety
            k = max(1, min(S, k))

            # Map slot index to time
            if S == 1:
                base_t = start_at
            else:
                base_t = start_at + (k - 1.0) * slot_step  # slot 1 -> 0, slot S -> T

            if use_center_of_slot and S > 1:
                # If you ever want "center", shift by half a step, but clamp to [0, T]
                t = base_t + 0.5 * slot_step
                t = max(start_at, min(start_at + T, t))
            else:
                t = base_t

            ev = SlotEvent(es_index=i, slot_index=k, time_s=t, frequency=fi)
            schedule_by_slot[k].append(ev)
            all_events.append(ev)

    # Sort events inside each slot for determinism
    for k in schedule_by_slot:
        schedule_by_slot[k].sort(key=lambda e: (e.time_s, e.es_index))

    # Global sort
    all_events.sort(key=lambda e: (e.time_s, e.slot_index, e.es_index))

    return schedule_by_slot, all_events, S, slot_step


def pretty_print_schedule(
    schedule_by_slot: Dict[int, List[SlotEvent]],
    S: int,
    slot_step: float,
    *,
    es_labels: Optional[Sequence[str]] = None
) -> None:
    """Human-readable printing of slot schedule (boundary-aligned times)."""
    es_labels = es_labels or []
    for k in range(1, S + 1):
        events = schedule_by_slot.get(k, [])
        t_k = (k - 1) * slot_step if S > 1 else 0.0

        if not events:
            print(f"Slot {k:>3}/{S} @ t≈{t_k:.3f}s: (none)")
            continue

        names = []
        for ev in events:
            if ev.es_index < len(es_labels):
                names.append(es_labels[ev.es_index])
            else:
                names.append(f"ES{ev.es_index+1}")

        print(f"Slot {k:>3}/{S} @ t≈{t_k:.3f}s: {', '.join(names)}")

if __name__ == "__main__":
    ES_F = { "Edge-Server-1": 3, "Edge-Server-2": 5, "Edge-Server-3": 1, "Edge-Server-4": 3}
    es_labels = list(ES_F.keys())
    F = list(ES_F.values())   # ESs veriuficaiton fequency: verification per control interval, here it is showing only 4 are connected
    T = 60          # seconds

    schedule_by_slot, all_events, S, slot_step = frequency_wise_schedule(F, T, es_labels=es_labels)
    print(schedule_by_slot)
    print(all_events)
    print(S)
    print(slot_step)
    # pretty_print_schedule(schedule_by_slot, S, slot_step, es_labels=es_labels)
