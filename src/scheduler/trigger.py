"""DAG trigger and parameterization.

Defines trigger configurations that bind cron schedules to DAGs
with optional runtime parameters. Triggers are the bridge between
the scheduler and the executor.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from src.scheduler.cron_parser import CronSchedule


@dataclass
class TriggerConfig:
    """Configuration for a scheduled DAG trigger.

    Attributes:
        trigger_id: Unique identifier for this trigger.
        dag_path: Path to the DAG YAML definition.
        schedule: Cron schedule for this trigger.
        params: Runtime parameters to pass to the DAG.
        enabled: Whether this trigger is active.
        max_concurrent: Maximum concurrent runs of this DAG.
        tags: Labels for filtering and grouping triggers.
    """

    trigger_id: str
    dag_path: str
    schedule: CronSchedule
    params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    max_concurrent: int = 1
    tags: List[str] = field(default_factory=list)

    def next_fire(self, after: Optional[datetime] = None) -> datetime:
        """Compute the next fire time for this trigger.

        Args:
            after: Reference time. Defaults to now (UTC).

        Returns:
            Next fire datetime.
        """
        return self.schedule.next_fire(after)


@dataclass
class TriggerEvent:
    """Record of a trigger firing.

    Attributes:
        trigger_id: Which trigger fired.
        dag_path: Path to the DAG that was triggered.
        scheduled_time: When the trigger was supposed to fire.
        actual_time: When the trigger actually fired.
        params: Parameters passed to the DAG run.
        run_id: Unique identifier for this DAG run.
    """

    trigger_id: str
    dag_path: str
    scheduled_time: datetime
    actual_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    params: Dict[str, Any] = field(default_factory=dict)
    run_id: str = ""

    def __post_init__(self) -> None:
        if not self.run_id:
            ts = int(self.actual_time.timestamp() * 1000)
            self.run_id = f"{self.trigger_id}_{ts}"

    @property
    def delay_seconds(self) -> float:
        """Compute how late this trigger fired."""
        return (self.actual_time - self.scheduled_time).total_seconds()


class TriggerRegistry:
    """Registry of all configured triggers.

    Manages trigger configurations and computes the global next-fire
    schedule across all active triggers.
    """

    def __init__(self) -> None:
        self._triggers: Dict[str, TriggerConfig] = {}

    def register(self, config: TriggerConfig) -> None:
        """Register a new trigger configuration.

        Args:
            config: The trigger configuration to register.

        Raises:
            ValueError: If a trigger with the same ID already exists.
        """
        if config.trigger_id in self._triggers:
            raise ValueError(
                f"Trigger {config.trigger_id!r} already registered"
            )
        self._triggers[config.trigger_id] = config

    def unregister(self, trigger_id: str) -> None:
        """Remove a trigger from the registry.

        Args:
            trigger_id: ID of the trigger to remove.
        """
        self._triggers.pop(trigger_id, None)

    def get(self, trigger_id: str) -> Optional[TriggerConfig]:
        """Look up a trigger by ID."""
        return self._triggers.get(trigger_id)

    def active_triggers(self) -> List[TriggerConfig]:
        """Return all enabled triggers."""
        return [t for t in self._triggers.values() if t.enabled]

    def next_trigger(
        self,
        after: Optional[datetime] = None,
    ) -> Optional[TriggerConfig]:
        """Find the trigger with the earliest next fire time.

        Args:
            after: Reference time. Defaults to now (UTC).

        Returns:
            The trigger that fires next, or None if no active triggers.
        """
        active = self.active_triggers()
        if not active:
            return None
        return min(active, key=lambda t: t.next_fire(after))

    @property
    def size(self) -> int:
        """Return the total number of registered triggers."""
        return len(self._triggers)


if __name__ == "__main__":
    from src.scheduler.cron_parser import parse_cron

    registry = TriggerRegistry()

    # Register some triggers
    triggers = [
        TriggerConfig(
            trigger_id="etl_hourly",
            dag_path="configs/sample_dag.yaml",
            schedule=parse_cron("0 * * * *", "Hourly ETL"),
            params={"source": "postgres", "target": "warehouse"},
            tags=["etl", "production"],
        ),
        TriggerConfig(
            trigger_id="report_daily",
            dag_path="configs/report_dag.yaml",
            schedule=parse_cron("0 6 * * *", "Daily report at 6am"),
            params={"format": "pdf"},
            tags=["reporting"],
        ),
        TriggerConfig(
            trigger_id="cleanup_weekly",
            dag_path="configs/cleanup_dag.yaml",
            schedule=parse_cron("0 0 * * 0", "Weekly cleanup"),
            tags=["maintenance"],
        ),
    ]

    for t in triggers:
        registry.register(t)

    now = datetime.now(timezone.utc)
    print(f"Registered triggers: {registry.size}")
    print(f"Active triggers: {len(registry.active_triggers())}")

    next_t = registry.next_trigger(now)
    if next_t:
        print(f"\nNext trigger: {next_t.trigger_id}")
        print(f"  Schedule: {next_t.schedule.description}")
        print(f"  Next fire: {next_t.next_fire(now).isoformat()}")
        print(f"  DAG: {next_t.dag_path}")
        print(f"  Params: {next_t.params}")
