"""DAG data structure with adjacency lists.

Provides the core directed acyclic graph representation used by the
workflow engine. Tasks are nodes, dependency edges point from upstream
to downstream tasks.
"""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Set


class TaskStatus(Enum):
    """Lifecycle states for a task node."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class TaskNode:
    """A single task in the workflow DAG.

    Attributes:
        name: Unique identifier for this task.
        func: Callable to execute, or None for placeholder tasks.
        retries: Maximum retry attempts on failure.
        timeout: Execution timeout in seconds.
        status: Current lifecycle state.
        metadata: Arbitrary key-value metadata.
    """

    name: str
    func: Optional[Callable[..., Any]] = None
    retries: int = 0
    timeout: float = 300.0
    status: TaskStatus = TaskStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskNode):
            return NotImplemented
        return self.name == other.name

    def __repr__(self) -> str:
        return f"TaskNode({self.name!r}, status={self.status.value})"


class DAG:
    """Directed acyclic graph of workflow tasks.

    Stores tasks as nodes and dependency edges as adjacency lists.
    Provides methods for adding tasks, edges, querying neighbors,
    and computing in-degree counts.

    Attributes:
        name: Human-readable name for this DAG.
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._nodes: Dict[str, TaskNode] = {}
        self._adj: Dict[str, Set[str]] = {}  # forward edges
        self._rev: Dict[str, Set[str]] = {}  # reverse edges

    @property
    def nodes(self) -> Dict[str, TaskNode]:
        """Return all task nodes keyed by name."""
        return dict(self._nodes)

    @property
    def num_tasks(self) -> int:
        """Return the number of tasks in the DAG."""
        return len(self._nodes)

    @property
    def num_edges(self) -> int:
        """Return the number of dependency edges."""
        return sum(len(s) for s in self._adj.values())

    def add_task(self, task: TaskNode) -> None:
        """Add a task node to the DAG.

        Args:
            task: The task node to add.

        Raises:
            ValueError: If a task with the same name already exists.
        """
        if task.name in self._nodes:
            raise ValueError(f"Task {task.name!r} already exists in DAG")
        self._nodes[task.name] = task
        self._adj.setdefault(task.name, set())
        self._rev.setdefault(task.name, set())

    def add_edge(self, upstream: str, downstream: str) -> None:
        """Add a dependency edge: upstream must complete before downstream.

        Args:
            upstream: Name of the task that must finish first.
            downstream: Name of the task that depends on upstream.

        Raises:
            KeyError: If either task name is not in the DAG.
        """
        if upstream not in self._nodes:
            raise KeyError(f"Upstream task {upstream!r} not found")
        if downstream not in self._nodes:
            raise KeyError(f"Downstream task {downstream!r} not found")
        self._adj[upstream].add(downstream)
        self._rev[downstream].add(upstream)

    def successors(self, task_name: str) -> Set[str]:
        """Return names of tasks that depend on the given task."""
        return set(self._adj.get(task_name, set()))

    def predecessors(self, task_name: str) -> Set[str]:
        """Return names of tasks that the given task depends on."""
        return set(self._rev.get(task_name, set()))

    def in_degree(self, task_name: str) -> int:
        """Return the number of dependencies for a task."""
        return len(self._rev.get(task_name, set()))

    def in_degree_map(self) -> Dict[str, int]:
        """Return a mapping of task name to in-degree for all tasks."""
        return {name: len(self._rev.get(name, set())) for name in self._nodes}

    def roots(self) -> List[str]:
        """Return task names with no dependencies (in-degree 0)."""
        return [n for n in self._nodes if self.in_degree(n) == 0]

    def leaves(self) -> List[str]:
        """Return task names with no downstream dependents."""
        return [n for n in self._nodes if len(self._adj.get(n, set())) == 0]

    def __iter__(self) -> Iterator[str]:
        return iter(self._nodes)

    def __contains__(self, task_name: str) -> bool:
        return task_name in self._nodes

    def __repr__(self) -> str:
        return f"DAG({self.name!r}, tasks={self.num_tasks}, edges={self.num_edges})"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the DAG to a dictionary."""
        return {
            "name": self.name,
            "tasks": [
                {
                    "name": t.name,
                    "retries": t.retries,
                    "timeout": t.timeout,
                    "metadata": t.metadata,
                }
                for t in self._nodes.values()
            ],
            "edges": [
                {"from": u, "to": d}
                for u, ds in self._adj.items()
                for d in ds
            ],
        }


if __name__ == "__main__":
    # Demo: build a simple ETL DAG
    dag = DAG("etl_pipeline")

    for name in ["extract", "validate", "transform", "load", "notify"]:
        dag.add_task(TaskNode(name=name))

    dag.add_edge("extract", "validate")
    dag.add_edge("extract", "transform")
    dag.add_edge("validate", "load")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")

    print(f"DAG: {dag}")
    print(f"Roots: {dag.roots()}")
    print(f"Leaves: {dag.leaves()}")
    print(f"In-degrees: {dag.in_degree_map()}")
    print(f"Successors of 'extract': {dag.successors('extract')}")
    print(f"\nSerialized:\n{json.dumps(dag.to_dict(), indent=2)}")
