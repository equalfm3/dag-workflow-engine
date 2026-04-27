"""Dead letter queue for exhausted retries.

Captures failed tasks that have exceeded their retry limit, along
with full exception details and task metadata for post-mortem
analysis.
"""

from __future__ import annotations

import json
import threading
import traceback
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class DeadLetterEntry:
    """A single entry in the dead letter queue.

    Attributes:
        task_name: Name of the failed task.
        dag_name: Name of the DAG the task belongs to.
        error: The final exception that caused the failure.
        attempts: Total number of attempts made.
        metadata: Task metadata for debugging.
        timestamp: When the entry was created.
        traceback_str: Formatted traceback of the final error.
    """

    task_name: str
    dag_name: str
    error: Optional[Exception] = None
    attempts: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    traceback_str: str = ""

    def __post_init__(self) -> None:
        if self.error and not self.traceback_str:
            self.traceback_str = "".join(
                traceback.format_exception(
                    type(self.error), self.error, self.error.__traceback__
                )
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the entry for logging or storage."""
        return {
            "task_name": self.task_name,
            "dag_name": self.dag_name,
            "error_type": type(self.error).__name__ if self.error else None,
            "error_message": str(self.error) if self.error else None,
            "attempts": self.attempts,
            "metadata": self.metadata,
            "timestamp": datetime.fromtimestamp(
                self.timestamp, tz=timezone.utc
            ).isoformat(),
            "traceback": self.traceback_str,
        }


class DeadLetterQueue:
    """Thread-safe dead letter queue for failed tasks.

    Stores entries for tasks that exhausted all retry attempts.
    Supports querying, draining, and serialization for external
    storage or alerting systems.
    """

    def __init__(self, max_size: int = 1000) -> None:
        self._entries: List[DeadLetterEntry] = []
        self._lock = threading.Lock()
        self._max_size = max_size

    @property
    def size(self) -> int:
        """Return the number of entries in the queue."""
        with self._lock:
            return len(self._entries)

    @property
    def is_empty(self) -> bool:
        """Check if the queue has no entries."""
        return self.size == 0

    def add(self, entry: DeadLetterEntry) -> None:
        """Add a failed task entry to the queue.

        If the queue exceeds max_size, the oldest entry is dropped.

        Args:
            entry: The dead letter entry to add.
        """
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_size:
                self._entries.pop(0)

    def peek(self, count: int = 10) -> List[DeadLetterEntry]:
        """View the most recent entries without removing them.

        Args:
            count: Maximum number of entries to return.

        Returns:
            List of the most recent dead letter entries.
        """
        with self._lock:
            return list(self._entries[-count:])

    def drain(self) -> List[DeadLetterEntry]:
        """Remove and return all entries from the queue.

        Returns:
            All entries that were in the queue.
        """
        with self._lock:
            entries = list(self._entries)
            self._entries.clear()
            return entries

    def entries_for_dag(self, dag_name: str) -> List[DeadLetterEntry]:
        """Return entries for a specific DAG.

        Args:
            dag_name: Name of the DAG to filter by.

        Returns:
            List of entries belonging to the specified DAG.
        """
        with self._lock:
            return [e for e in self._entries if e.dag_name == dag_name]

    def to_json(self, indent: int = 2) -> str:
        """Serialize all entries to JSON.

        Args:
            indent: JSON indentation level.

        Returns:
            JSON string of all entries.
        """
        with self._lock:
            return json.dumps(
                [e.to_dict() for e in self._entries],
                indent=indent,
                default=str,
            )

    def summary(self) -> Dict[str, int]:
        """Return a count of entries grouped by error type.

        Returns:
            Dictionary mapping error type names to counts.
        """
        with self._lock:
            counts: Dict[str, int] = {}
            for entry in self._entries:
                key = type(entry.error).__name__ if entry.error else "Unknown"
                counts[key] = counts.get(key, 0) + 1
            return counts


if __name__ == "__main__":
    dlq = DeadLetterQueue()

    # Simulate some failures
    errors = [
        ConnectionError("Database connection refused"),
        TimeoutError("API call timed out after 30s"),
        ConnectionError("Redis cluster unreachable"),
        ValueError("Invalid data format in row 42"),
    ]

    for i, error in enumerate(errors):
        try:
            raise error
        except Exception as e:
            entry = DeadLetterEntry(
                task_name=f"task_{i}",
                dag_name="etl_pipeline",
                error=e,
                attempts=3,
                metadata={"priority": "high"},
            )
            dlq.add(entry)

    print(f"DLQ size: {dlq.size}")
    print(f"Summary: {dlq.summary()}")
    print(f"\nRecent entries:")
    for entry in dlq.peek(3):
        d = entry.to_dict()
        print(f"  {d['task_name']}: {d['error_type']} - {d['error_message']}")
    print(f"\nFull JSON:\n{dlq.to_json()}")
