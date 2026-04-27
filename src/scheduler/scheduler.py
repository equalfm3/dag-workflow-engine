"""Priority-queue based scheduler loop.

Maintains a priority queue of upcoming trigger fire times and
dispatches DAG executions when triggers fire. Supports multiple
concurrent DAGs with configurable concurrency limits.
"""

from __future__ import annotations

import argparse
import heapq
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.scheduler.cron_parser import CronSchedule, parse_cron
from src.scheduler.trigger import TriggerConfig, TriggerEvent, TriggerRegistry

logger = logging.getLogger(__name__)


@dataclass(order=True)
class ScheduleEntry:
    """Entry in the scheduler's priority queue.

    Ordered by fire_time for heap operations.

    Attributes:
        fire_time: When this entry should fire.
        trigger_id: ID of the trigger to fire.
    """

    fire_time: datetime
    trigger_id: str = field(compare=False)


class Scheduler:
    """Cron-based DAG scheduler with priority queue.

    Maintains a min-heap of upcoming fire times. On each tick,
    fires all triggers whose time has arrived and re-schedules
    them for their next occurrence.

    Args:
        registry: Registry of trigger configurations.
        on_trigger: Callback invoked when a trigger fires.
        tick_interval: Seconds between scheduler ticks.
    """

    def __init__(
        self,
        registry: TriggerRegistry,
        on_trigger: Optional[Callable[[TriggerEvent], None]] = None,
        tick_interval: float = 1.0,
    ) -> None:
        self._registry = registry
        self._on_trigger = on_trigger or self._default_handler
        self._tick_interval = tick_interval
        self._heap: List[ScheduleEntry] = []
        self._stop = threading.Event()
        self._history: List[TriggerEvent] = []
        self._active_runs: Dict[str, int] = {}
        self._lock = threading.Lock()

    @property
    def pending_count(self) -> int:
        """Number of entries in the schedule queue."""
        return len(self._heap)

    @property
    def history(self) -> List[TriggerEvent]:
        """Return the list of past trigger events."""
        return list(self._history)

    def initialize(self) -> None:
        """Populate the priority queue with next fire times.

        Computes the next fire time for each active trigger and
        adds it to the heap.
        """
        now = datetime.now(timezone.utc)
        for trigger in self._registry.active_triggers():
            next_fire = trigger.next_fire(now)
            heapq.heappush(
                self._heap,
                ScheduleEntry(fire_time=next_fire, trigger_id=trigger.trigger_id),
            )
        logger.info(
            "Scheduler initialized with %d triggers", len(self._heap)
        )

    def tick(self) -> List[TriggerEvent]:
        """Process one scheduler tick.

        Fires all triggers whose time has arrived and re-schedules
        them for their next occurrence.

        Returns:
            List of trigger events that fired during this tick.
        """
        now = datetime.now(timezone.utc)
        events: List[TriggerEvent] = []

        while self._heap and self._heap[0].fire_time <= now:
            entry = heapq.heappop(self._heap)
            trigger = self._registry.get(entry.trigger_id)

            if trigger is None or not trigger.enabled:
                continue

            # Check concurrency limit
            with self._lock:
                active = self._active_runs.get(entry.trigger_id, 0)
                if active >= trigger.max_concurrent:
                    logger.warning(
                        "Trigger %s skipped: %d/%d concurrent runs",
                        entry.trigger_id, active, trigger.max_concurrent,
                    )
                    # Re-schedule for next occurrence
                    next_fire = trigger.next_fire(now)
                    heapq.heappush(
                        self._heap,
                        ScheduleEntry(next_fire, entry.trigger_id),
                    )
                    continue
                self._active_runs[entry.trigger_id] = active + 1

            event = TriggerEvent(
                trigger_id=entry.trigger_id,
                dag_path=trigger.dag_path,
                scheduled_time=entry.fire_time,
                params=dict(trigger.params),
            )
            events.append(event)
            self._history.append(event)

            try:
                self._on_trigger(event)
            except Exception:
                logger.exception("Error handling trigger %s", entry.trigger_id)
            finally:
                with self._lock:
                    self._active_runs[entry.trigger_id] = max(
                        0, self._active_runs.get(entry.trigger_id, 1) - 1
                    )

            # Re-schedule for next occurrence
            next_fire = trigger.next_fire(now)
            heapq.heappush(
                self._heap,
                ScheduleEntry(next_fire, entry.trigger_id),
            )

        return events

    def run(self, max_ticks: Optional[int] = None) -> None:
        """Run the scheduler loop.

        Args:
            max_ticks: Maximum number of ticks before stopping.
                None means run until stop() is called.
        """
        self.initialize()
        tick_count = 0

        while not self._stop.is_set():
            self.tick()
            tick_count += 1
            if max_ticks is not None and tick_count >= max_ticks:
                break
            self._stop.wait(timeout=self._tick_interval)

        logger.info("Scheduler stopped after %d ticks", tick_count)

    def stop(self) -> None:
        """Signal the scheduler to stop."""
        self._stop.set()

    @staticmethod
    def _default_handler(event: TriggerEvent) -> None:
        """Default trigger handler that logs the event."""
        logger.info(
            "Trigger fired: %s (DAG: %s, scheduled: %s, delay: %.1fs)",
            event.trigger_id,
            event.dag_path,
            event.scheduled_time.isoformat(),
            event.delay_seconds,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DAG Scheduler")
    parser.add_argument("--ticks", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    registry = TriggerRegistry()

    # Register demo triggers
    registry.register(TriggerConfig(
        trigger_id="fast_job",
        dag_path="configs/sample_dag.yaml",
        schedule=parse_cron("* * * * *", "Every minute"),
        params={"mode": "fast"},
    ))
    registry.register(TriggerConfig(
        trigger_id="hourly_etl",
        dag_path="configs/sample_dag.yaml",
        schedule=parse_cron("0 * * * *", "Hourly"),
    ))

    def demo_handler(event: TriggerEvent) -> None:
        print(
            f"  -> Executing DAG {event.dag_path} "
            f"(run_id={event.run_id}, params={event.params})"
        )

    scheduler = Scheduler(
        registry=registry,
        on_trigger=demo_handler,
        tick_interval=args.interval,
    )

    print(f"Running scheduler for {args.ticks} ticks...")
    scheduler.run(max_ticks=args.ticks)
    print(f"\nHistory: {len(scheduler.history)} events fired")
    for event in scheduler.history:
        print(f"  {event.trigger_id}: {event.scheduled_time.isoformat()}")
