"""Individual task execution with timeout.

Wraps task callables with timeout enforcement, exception capture,
and execution timing. Produces TaskResult objects for the state
manager.
"""

from __future__ import annotations

import concurrent.futures
import time
import traceback
from typing import Any, Callable, Dict, Optional

from src.dag.graph import TaskStatus
from src.executor.state_manager import TaskResult


class TaskRunner:
    """Executes a single task callable with timeout and error capture.

    Args:
        default_timeout: Default timeout in seconds if not specified per-task.
    """

    def __init__(self, default_timeout: float = 300.0) -> None:
        self._default_timeout = default_timeout

    def run(
        self,
        task_name: str,
        func: Optional[Callable[..., Any]],
        timeout: Optional[float] = None,
        attempt: int = 0,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> TaskResult:
        """Execute a task function and return the result.

        If the function is None, the task succeeds immediately (useful
        for placeholder/marker tasks).

        Args:
            task_name: Name of the task being executed.
            func: The callable to execute, or None for no-op tasks.
            timeout: Execution timeout in seconds.
            attempt: Current retry attempt number (0-indexed).
            kwargs: Keyword arguments to pass to the callable.

        Returns:
            A TaskResult with status, return value, and timing info.
        """
        effective_timeout = timeout or self._default_timeout
        run_kwargs = kwargs or {}

        if func is None:
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.SUCCESS,
                result=None,
                duration=0.0,
                attempt=attempt,
            )

        start = time.monotonic()
        try:
            result = self._execute_with_timeout(
                func, effective_timeout, run_kwargs
            )
            duration = time.monotonic() - start
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.SUCCESS,
                result=result,
                duration=duration,
                attempt=attempt,
            )
        except concurrent.futures.TimeoutError:
            duration = time.monotonic() - start
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED,
                error=TimeoutError(
                    f"Task {task_name!r} timed out after {effective_timeout}s"
                ),
                duration=duration,
                attempt=attempt,
            )
        except Exception as e:
            duration = time.monotonic() - start
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED,
                error=e,
                duration=duration,
                attempt=attempt,
            )

    def _execute_with_timeout(
        self,
        func: Callable[..., Any],
        timeout: float,
        kwargs: Dict[str, Any],
    ) -> Any:
        """Run a callable in a thread with timeout enforcement.

        Args:
            func: The callable to execute.
            timeout: Maximum execution time in seconds.
            kwargs: Arguments to pass to the callable.

        Returns:
            The return value of the callable.

        Raises:
            concurrent.futures.TimeoutError: If execution exceeds timeout.
            Exception: Any exception raised by the callable.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(func, **kwargs)
            return future.result(timeout=timeout)


if __name__ == "__main__":
    import random

    runner = TaskRunner(default_timeout=5.0)

    # Successful task
    def sample_task() -> str:
        time.sleep(0.1)
        return "data_extracted"

    result = runner.run("extract", sample_task)
    print(f"Success: {result.task_name} -> {result.result} ({result.duration:.3f}s)")

    # Failing task
    def failing_task() -> None:
        raise ConnectionError("Database unreachable")

    result = runner.run("load", failing_task, attempt=1)
    print(f"Failed: {result.task_name} -> {result.error} (attempt {result.attempt})")

    # Timeout task
    def slow_task() -> None:
        time.sleep(10)

    result = runner.run("slow", slow_task, timeout=0.5)
    print(f"Timeout: {result.task_name} -> {result.error}")

    # No-op task
    result = runner.run("marker", None)
    print(f"No-op: {result.task_name} -> status={result.status.value}")
