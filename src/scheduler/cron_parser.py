"""Cron expression parser and next-fire computation.

Parses standard 5-field cron expressions and computes the next
fire time using the croniter library. Handles month-length
variations and day-of-week interactions.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, List, Optional

from croniter import croniter


@dataclass(frozen=True)
class CronSchedule:
    """Parsed cron schedule with next-fire computation.

    Attributes:
        expression: The raw cron expression string.
        description: Human-readable description of the schedule.
    """

    expression: str
    description: str = ""

    def __post_init__(self) -> None:
        if not croniter.is_valid(self.expression):
            raise ValueError(
                f"Invalid cron expression: {self.expression!r}"
            )

    def next_fire(
        self,
        after: Optional[datetime] = None,
    ) -> datetime:
        """Compute the next fire time after the given timestamp.

        Args:
            after: Reference time. Defaults to now (UTC).

        Returns:
            The next datetime when this schedule fires.
        """
        ref = after or datetime.now(timezone.utc)
        cron = croniter(self.expression, ref)
        return cron.get_next(datetime).replace(tzinfo=timezone.utc)

    def prev_fire(
        self,
        before: Optional[datetime] = None,
    ) -> datetime:
        """Compute the most recent fire time before the given timestamp.

        Args:
            before: Reference time. Defaults to now (UTC).

        Returns:
            The most recent datetime when this schedule fired.
        """
        ref = before or datetime.now(timezone.utc)
        cron = croniter(self.expression, ref)
        return cron.get_prev(datetime).replace(tzinfo=timezone.utc)

    def next_n(
        self,
        n: int,
        after: Optional[datetime] = None,
    ) -> List[datetime]:
        """Compute the next N fire times.

        Args:
            n: Number of fire times to compute.
            after: Reference time. Defaults to now (UTC).

        Returns:
            List of the next N fire datetimes.
        """
        ref = after or datetime.now(timezone.utc)
        cron = croniter(self.expression, ref)
        return [
            cron.get_next(datetime).replace(tzinfo=timezone.utc)
            for _ in range(n)
        ]

    def iter_fires(
        self,
        after: Optional[datetime] = None,
    ) -> Iterator[datetime]:
        """Iterate over fire times indefinitely.

        Args:
            after: Reference time. Defaults to now (UTC).

        Yields:
            Successive fire datetimes.
        """
        ref = after or datetime.now(timezone.utc)
        cron = croniter(self.expression, ref)
        while True:
            yield cron.get_next(datetime).replace(tzinfo=timezone.utc)

    def seconds_until_next(
        self,
        after: Optional[datetime] = None,
    ) -> float:
        """Compute seconds until the next fire time.

        Args:
            after: Reference time. Defaults to now (UTC).

        Returns:
            Number of seconds until the next fire.
        """
        ref = after or datetime.now(timezone.utc)
        next_dt = self.next_fire(ref)
        return (next_dt - ref).total_seconds()


# Common schedule presets
EVERY_MINUTE = CronSchedule("* * * * *", "Every minute")
EVERY_5_MINUTES = CronSchedule("*/5 * * * *", "Every 5 minutes")
HOURLY = CronSchedule("0 * * * *", "Every hour")
DAILY_MIDNIGHT = CronSchedule("0 0 * * *", "Daily at midnight")
WEEKLY_MONDAY = CronSchedule("0 0 * * 1", "Weekly on Monday")
MONTHLY_FIRST = CronSchedule("0 0 1 * *", "Monthly on the 1st")


def parse_cron(expression: str, description: str = "") -> CronSchedule:
    """Parse a cron expression string into a CronSchedule.

    Args:
        expression: Standard 5-field cron expression.
        description: Optional human-readable description.

    Returns:
        A validated CronSchedule object.

    Raises:
        ValueError: If the expression is invalid.
    """
    return CronSchedule(expression=expression, description=description)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cron expression parser")
    parser.add_argument(
        "--expr", default="*/15 * * * *",
        help="Cron expression to parse",
    )
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    schedule = parse_cron(args.expr)
    now = datetime.now(timezone.utc)

    print(f"Cron expression: {schedule.expression}")
    print(f"Reference time: {now.isoformat()}")
    print(f"Seconds until next: {schedule.seconds_until_next(now):.1f}")
    print(f"\nNext {args.count} fire times:")
    for dt in schedule.next_n(args.count, now):
        print(f"  {dt.isoformat()}")

    print("\nPreset schedules:")
    presets = [
        EVERY_MINUTE, EVERY_5_MINUTES, HOURLY,
        DAILY_MIDNIGHT, WEEKLY_MONDAY, MONTHLY_FIRST,
    ]
    for preset in presets:
        nxt = preset.next_fire(now)
        print(f"  {preset.description}: next at {nxt.isoformat()}")
