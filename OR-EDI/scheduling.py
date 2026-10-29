from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple, Union, Optional
import secrets


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
    max_gap_s: float | None = None,
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

    # One randomly displaced challenge per cell of each ES's own grid.
    # Displacement is restricted by Eq. (22), including first and last gaps.
    slot_step = T / S
    schedule_by_slot: Dict[int, List[SlotEvent]] = {k: [] for k in range(1, S + 1)}
    all_events: List[SlotEvent] = []
    for i, fi in enumerate(f_int):
        if fi == 0:
            continue
        step_i = T / fi
        # A 2*step_i bound permits a positive displacement for every fi.
        # Caller may enforce a tighter maximum gap through frequency selection.
        max_gap = max_gap_s if max_gap_s is not None else 2.0 * step_i
        if max_gap <= step_i:
            raise ValueError(f"No admissible randomized schedule for ES {i}: gap {max_gap} <= {step_i}")
        delta = min(step_i / 2.0, (max_gap - step_i) / 2.0) * 0.95
        for j in range(fi):
            jitter = (2.0 * secrets.randbelow(10**9) / 10**9 - 1.0) * delta
            t = start_at + (j + 0.5) * step_i + jitter
            k = min(S, max(1, int((t - start_at) / slot_step) + 1))
            ev = SlotEvent(i, k, t, fi)
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
