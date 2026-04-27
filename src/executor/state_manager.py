"""Task state tracking and transitions.

Manages the lifecycle state of every task in a DAG execution,
tracks in-degree counters, and determines which tasks are ready
to execute after a dependency completes.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from src.dag.graph import DAG, TaskStatus


@dataclass
class TaskResult:
    """Result of a single task execution.

    Attributes:
        task_name: Name of the task.
        status: Final status after execution.
        result: Return value from the task callable.
        error: Exception if the task failed.
        duration: Wall-clock execution time in seconds.
        attempt: Which attempt this result is from (0-indexed).
    """

    task_name: str
    status: TaskStatus
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    attempt: int = 0


class StateManager:
    """Thread-safe task state tracker for DAG execution.

    Maintains in-degree counters and determines which tasks become
    ready as their dependencies complete. All mutations are protected
    by a lock for safe concurrent access.

    Args:
        dag: The DAG whose execution state to manage.
    """

    def __init__(self, dag: DAG) -> None:
        self._dag = dag
        self._lock = threading.Lock()
        self._in_degree: Dict[str, int] = dag.in_degree_map()
        self._status: Dict[str, TaskStatus] = {
            name: TaskStatus.PENDING for name in dag
        }
        self._results: Dict[str, TaskResult] = {}
        self._start_time = time.monotonic()

    @property
    def all_complete(self) -> bool:
        """Check if all tasks have reached a terminal state."""
        with self._lock:
            terminal = {TaskStatus.SUCCESS, TaskStatus.DEAD}
            return all(s in terminal for s in self._status.values())

    @property
    def has_failures(self) -> bool:
        """Check if any task ended in DEAD state."""
        with self._lock:
            return any(
                s == TaskStatus.DEAD for s in self._status.values()
            )

    def get_ready_tasks(self) -> List[str]:
        """Return task names that are pending with all deps satisfied.

        Returns:
            List of task names ready to be queued for execution.
        """
        with self._lock:
            ready = []
            for name, status in self._status.items():
                if status == TaskStatus.PENDING and self._in_degree[name] == 0:
                    ready.append(name)
            return sorted(ready)

    def mark_queued(self, task_name: str) -> None:
        """Transition a task from PENDING to QUEUED."""
        with self._lock:
            self._status[task_name] = TaskStatus.QUEUED

    def mark_running(self, task_name: str) -> None:
        """Transition a task to RUNNING."""
        with self._lock:
            self._status[task_name] = TaskStatus.RUNNING

    def mark_success(self, task_name: str, result: TaskResult) -> List[str]:
        """Mark a task as successful and return newly ready tasks.

        Decrements in-degree of all downstream tasks and returns
        those that become ready (in-degree reaches 0).

        Args:
            task_name: The completed task.
            result: The execution result.

        Returns:
            List of downstream task names that are now ready.
        """
        with self._lock:
            self._status[task_name] = TaskStatus.SUCCESS
            self._results[task_name] = result

            newly_ready: List[str] = []
            for downstream in self._dag.successors(task_name):
                self._in_degree[downstream] -= 1
                if (
                    self._in_degree[downstream] == 0
                    and self._status[downstream] == TaskStatus.PENDING
                ):
                    newly_ready.append(downstream)
            return sorted(newly_ready)

    def mark_failed(self, task_name: str, result: TaskResult) -> None:
        """Mark a task as failed (may be retried)."""
        with self._lock:
            self._status[task_name] = TaskStatus.FAILED
            self._results[task_name] = result

    def mark_dead(self, task_name: str, result: TaskResult) -> None:
        """Mark a task as dead (retries exhausted)."""
        with self._lock:
            self._status[task_name] = TaskStatus.DEAD
            self._results[task_name] = result

    def reset_for_retry(self, task_name: str) -> None:
        """Reset a failed task back to QUEUED for retry."""
        with self._lock:
            self._status[task_name] = TaskStatus.QUEUED

    def get_status(self, task_name: str) -> TaskStatus:
        """Get the current status of a task."""
        with self._lock:
            return self._status[task_name]

    def get_results(self) -> Dict[str, TaskResult]:
        """Return a copy of all task results."""
        with self._lock:
            return dict(self._results)

    def summary(self) -> Dict[str, int]:
        """Return a count of tasks in each status."""
        with self._lock:
            counts: Dict[str, int] = {}
            for status in self._status.values():
                counts[status.value] = counts.get(status.value, 0) + 1
            return counts


if __name__ == "__main__":
    from src.dag.graph import TaskNode

    dag = DAG("demo")
    for name in ["A", "B", "C", "D"]:
        dag.add_task(TaskNode(name=name))
    dag.add_edge("A", "C")
    dag.add_edge("B", "C")
    dag.add_edge("C", "D")

    sm = StateManager(dag)
    print(f"Ready tasks: {sm.get_ready_tasks()}")

    sm.mark_queued("A")
    sm.mark_running("A")
    newly_ready = sm.mark_success(
        "A", TaskResult("A", TaskStatus.SUCCESS, duration=1.0)
    )
    print(f"After A completes, newly ready: {newly_ready}")

    sm.mark_queued("B")
    sm.mark_running("B")
    newly_ready = sm.mark_success(
        "B", TaskResult("B", TaskStatus.SUCCESS, duration=0.5)
    )
    print(f"After B completes, newly ready: {newly_ready}")
    print(f"Summary: {sm.summary()}")
