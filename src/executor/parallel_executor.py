"""Thread/process pool task executor.

Orchestrates parallel execution of DAG tasks using a dynamic ready
queue. As tasks complete, downstream tasks with satisfied dependencies
are dispatched to the worker pool.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import signal
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from src.dag.graph import DAG, TaskNode, TaskStatus
from src.dag.validator import validate_dag
from src.executor.state_manager import StateManager, TaskResult
from src.executor.task_runner import TaskRunner
from src.retry.backoff import compute_backoff
from src.retry.dead_letter import DeadLetterQueue, DeadLetterEntry
from src.retry.policy import RetryPolicy

logger = logging.getLogger(__name__)


class ParallelExecutor:
    """Executes DAG tasks in parallel with retry support.

    Uses a thread pool to run independent tasks concurrently.
    Maintains a dynamic ready queue that updates as tasks complete.

    Args:
        dag: The DAG to execute.
        max_workers: Maximum concurrent worker threads.
        retry_policy: Default retry policy for tasks.
        use_processes: If True, use ProcessPoolExecutor instead of threads.
    """

    def __init__(
        self,
        dag: DAG,
        max_workers: int = 4,
        retry_policy: Optional[RetryPolicy] = None,
        use_processes: bool = False,
    ) -> None:
        self._dag = dag
        self._max_workers = max_workers
        self._retry_policy = retry_policy or RetryPolicy()
        self._use_processes = use_processes
        self._state = StateManager(dag)
        self._runner = TaskRunner()
        self._dlq = DeadLetterQueue()
        self._shutdown = threading.Event()
        self._attempt_counts: Dict[str, int] = {n: 0 for n in dag}

    @property
    def state(self) -> StateManager:
        """Access the state manager for inspection."""
        return self._state

    @property
    def dead_letter_queue(self) -> DeadLetterQueue:
        """Access the dead letter queue."""
        return self._dlq

    def execute(self) -> Dict[str, TaskResult]:
        """Execute all tasks in the DAG respecting dependencies.

        Returns:
            Dictionary mapping task names to their execution results.
        """
        # Validate DAG first
        validate_dag(self._dag)

        pool_class = (
            concurrent.futures.ProcessPoolExecutor
            if self._use_processes
            else concurrent.futures.ThreadPoolExecutor
        )

        with pool_class(max_workers=self._max_workers) as pool:
            futures: Dict[concurrent.futures.Future[TaskResult], str] = {}

            # Dispatch initial ready tasks
            for task_name in self._state.get_ready_tasks():
                if self._shutdown.is_set():
                    break
                self._dispatch(pool, futures, task_name)

            # Process completions
            while futures and not self._shutdown.is_set():
                done, _ = concurrent.futures.wait(
                    futures.keys(),
                    timeout=1.0,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                for future in done:
                    task_name = futures.pop(future)
                    result = future.result()
                    self._handle_result(pool, futures, task_name, result)

        return self._state.get_results()

    def _dispatch(
        self,
        pool: concurrent.futures.Executor,
        futures: Dict[concurrent.futures.Future[TaskResult], str],
        task_name: str,
    ) -> None:
        """Submit a task to the worker pool."""
        self._state.mark_queued(task_name)
        self._state.mark_running(task_name)
        node = self._dag.nodes[task_name]
        attempt = self._attempt_counts[task_name]

        future = pool.submit(
            self._runner.run,
            task_name=task_name,
            func=node.func,
            timeout=node.timeout,
            attempt=attempt,
        )
        futures[future] = task_name

    def _handle_result(
        self,
        pool: concurrent.futures.Executor,
        futures: Dict[concurrent.futures.Future[TaskResult], str],
        task_name: str,
        result: TaskResult,
    ) -> None:
        """Process a completed task result."""
        if result.status == TaskStatus.SUCCESS:
            newly_ready = self._state.mark_success(task_name, result)
            logger.info(
                "Task %s succeeded (%.2fs)", task_name, result.duration
            )
            for ready_name in newly_ready:
                if not self._shutdown.is_set():
                    self._dispatch(pool, futures, ready_name)
        else:
            self._handle_failure(pool, futures, task_name, result)

    def _handle_failure(
        self,
        pool: concurrent.futures.Executor,
        futures: Dict[concurrent.futures.Future[TaskResult], str],
        task_name: str,
        result: TaskResult,
    ) -> None:
        """Handle a failed task: retry or send to DLQ."""
        node = self._dag.nodes[task_name]
        max_retries = node.retries or self._retry_policy.max_attempts
        attempt = self._attempt_counts[task_name]

        if self._retry_policy.should_retry(result.error, attempt, max_retries):
            self._attempt_counts[task_name] += 1
            delay = compute_backoff(attempt, self._retry_policy.base_delay)
            logger.warning(
                "Task %s failed (attempt %d), retrying in %.1fs: %s",
                task_name, attempt + 1, delay, result.error,
            )
            self._state.reset_for_retry(task_name)
            time.sleep(delay)
            if not self._shutdown.is_set():
                self._dispatch(pool, futures, task_name)
        else:
            logger.error(
                "Task %s exhausted retries (%d attempts): %s",
                task_name, attempt + 1, result.error,
            )
            self._state.mark_dead(task_name, result)
            self._dlq.add(DeadLetterEntry(
                task_name=task_name,
                dag_name=self._dag.name,
                error=result.error,
                attempts=attempt + 1,
                metadata=node.metadata,
            ))

    def shutdown(self) -> None:
        """Signal the executor to stop dispatching new tasks."""
        self._shutdown.set()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute a DAG")
    parser.add_argument(
        "--dag", default="configs/sample_dag.yaml",
        help="Path to YAML DAG definition",
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    from src.dag.parser import parse_dag_yaml

    # Build a demo DAG with actual callables
    dag = DAG("demo_pipeline")
    tasks = {
        "extract": lambda: (time.sleep(0.2), "raw_data")[1],
        "validate": lambda: (time.sleep(0.1), "valid")[1],
        "transform": lambda: (time.sleep(0.3), "transformed")[1],
        "load": lambda: (time.sleep(0.2), "loaded")[1],
        "notify": lambda: (time.sleep(0.05), "notified")[1],
    }
    for name, func in tasks.items():
        dag.add_task(TaskNode(name=name, func=func, retries=2))
    dag.add_edge("extract", "validate")
    dag.add_edge("extract", "transform")
    dag.add_edge("validate", "load")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")

    executor = ParallelExecutor(dag, max_workers=args.workers)
    print(f"Executing DAG '{dag.name}' with {args.workers} workers...")
    start = time.monotonic()
    results = executor.execute()
    elapsed = time.monotonic() - start

    print(f"\nCompleted in {elapsed:.2f}s")
    for name, result in sorted(results.items()):
        print(f"  {name}: {result.status.value} ({result.duration:.3f}s)")
    print(f"\nState summary: {executor.state.summary()}")
