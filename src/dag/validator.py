"""Cycle detection and dependency validation.

Uses Kahn's algorithm for cycle detection and DFS to report the
exact cycle path when one is found. Also validates that all edge
references point to existing tasks.
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Optional, Set

from src.dag.graph import DAG
from src.dag.topo_sort import topological_sort, TopologicalSortError


class CycleError(Exception):
    """Raised when a cycle is detected in the DAG."""

    def __init__(self, cycle_path: List[str]) -> None:
        self.cycle_path = cycle_path
        path_str = " -> ".join(cycle_path)
        super().__init__(f"Cycle detected: {path_str}")


def _find_cycle_dfs(
    nodes: Set[str],
    adj: Dict[str, Set[str]],
) -> Optional[List[str]]:
    """Find and return a cycle path using DFS back-edge detection.

    Args:
        nodes: Set of node names to search.
        adj: Adjacency list.

    Returns:
        A list representing the cycle path, or None if no cycle exists.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {n: WHITE for n in nodes}
    parent: Dict[str, Optional[str]] = {n: None for n in nodes}

    def dfs(u: str) -> Optional[List[str]]:
        color[u] = GRAY
        for v in sorted(adj.get(u, set())):
            if v not in color:
                continue
            if color[v] == GRAY:
                # Back edge found — reconstruct cycle
                cycle = [v, u]
                cur = u
                while cur != v:
                    cur = parent[cur]  # type: ignore[assignment]
                    if cur is None:
                        break
                    cycle.append(cur)
                cycle.reverse()
                cycle.append(cycle[0])  # close the cycle
                return cycle
            if color[v] == WHITE:
                parent[v] = u
                result = dfs(v)
                if result is not None:
                    return result
        color[u] = BLACK
        return None

    for node in sorted(nodes):
        if color[node] == WHITE:
            result = dfs(node)
            if result is not None:
                return result
    return None


def validate_dag(dag: DAG) -> List[str]:
    """Validate a DAG: check for cycles and return topological order.

    First attempts Kahn's algorithm. If it detects a cycle (incomplete
    sort), uses DFS to find and report the exact cycle path.

    Args:
        dag: The DAG to validate.

    Returns:
        A valid topological ordering of task names.

    Raises:
        CycleError: If the DAG contains a cycle, with the exact path.
        ValueError: If the DAG has no tasks.
    """
    if dag.num_tasks == 0:
        raise ValueError("DAG has no tasks")

    nodes = set(dag.nodes.keys())
    adj: Dict[str, Set[str]] = {
        name: dag.successors(name) for name in nodes
    }

    try:
        return topological_sort(nodes, adj)
    except TopologicalSortError:
        cycle = _find_cycle_dfs(nodes, adj)
        if cycle:
            raise CycleError(cycle)
        raise  # shouldn't happen, but propagate original error


def validate_edges(dag: DAG) -> List[str]:
    """Check for dangling edge references.

    Args:
        dag: The DAG to check.

    Returns:
        List of warning messages for any issues found.
    """
    warnings: List[str] = []
    for name in dag:
        for succ in dag.successors(name):
            if succ not in dag:
                warnings.append(
                    f"Edge {name} -> {succ}: target not in DAG"
                )
        for pred in dag.predecessors(name):
            if pred not in dag:
                warnings.append(
                    f"Edge {pred} -> {name}: source not in DAG"
                )
    return warnings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a DAG definition")
    parser.add_argument(
        "--dag", type=str, default="configs/sample_dag.yaml",
        help="Path to YAML DAG definition",
    )
    args = parser.parse_args()

    from src.dag.parser import parse_dag_yaml

    try:
        dag = parse_dag_yaml(args.dag)
        order = validate_dag(dag)
        print(f"DAG '{dag.name}' is valid")
        print(f"Tasks: {dag.num_tasks}, Edges: {dag.num_edges}")
        print(f"Topological order: {order}")
        print(f"Root tasks: {dag.roots()}")
        print(f"Leaf tasks: {dag.leaves()}")

        warnings = validate_edges(dag)
        if warnings:
            for w in warnings:
                print(f"WARNING: {w}", file=sys.stderr)
    except CycleError as e:
        print(f"INVALID: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"File not found: {args.dag}", file=sys.stderr)
        sys.exit(1)
