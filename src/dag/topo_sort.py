"""Kahn's algorithm topological sort.

Computes a valid execution order for DAG tasks using BFS-based
topological sorting. Naturally detects cycles — if the sorted output
has fewer nodes than the graph, a cycle exists.
"""

from __future__ import annotations

from collections import deque
from typing import Dict, List, Set


class TopologicalSortError(Exception):
    """Raised when topological sort fails due to a cycle."""

    def __init__(self, remaining_nodes: Set[str]) -> None:
        self.remaining_nodes = remaining_nodes
        super().__init__(
            f"Cycle detected: topological sort could not process "
            f"{len(remaining_nodes)} nodes: {remaining_nodes}"
        )


def topological_sort(
    nodes: Set[str],
    adj: Dict[str, Set[str]],
) -> List[str]:
    """Compute topological ordering using Kahn's algorithm.

    Repeatedly removes nodes with zero in-degree. If the algorithm
    terminates before processing all nodes, a cycle exists.

    Args:
        nodes: Set of all node names.
        adj: Adjacency list mapping each node to its successors.

    Returns:
        A list of node names in valid topological order.

    Raises:
        TopologicalSortError: If the graph contains a cycle.
    """
    in_degree: Dict[str, int] = {n: 0 for n in nodes}
    for src in nodes:
        for dst in adj.get(src, set()):
            if dst in in_degree:
                in_degree[dst] += 1

    queue: deque[str] = deque()
    for node in sorted(nodes):  # sorted for deterministic output
        if in_degree[node] == 0:
            queue.append(node)

    order: List[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in sorted(adj.get(node, set())):
            if neighbor in in_degree:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

    if len(order) != len(nodes):
        remaining = nodes - set(order)
        raise TopologicalSortError(remaining)

    return order


def topological_sort_levels(
    nodes: Set[str],
    adj: Dict[str, Set[str]],
) -> List[List[str]]:
    """Compute topological ordering grouped by parallel execution levels.

    Tasks within the same level have no mutual dependencies and can
    execute concurrently.

    Args:
        nodes: Set of all node names.
        adj: Adjacency list mapping each node to its successors.

    Returns:
        A list of levels, where each level is a list of task names
        that can run in parallel.

    Raises:
        TopologicalSortError: If the graph contains a cycle.
    """
    in_degree: Dict[str, int] = {n: 0 for n in nodes}
    for src in nodes:
        for dst in adj.get(src, set()):
            if dst in in_degree:
                in_degree[dst] += 1

    current_level: List[str] = sorted(
        [n for n in nodes if in_degree[n] == 0]
    )
    levels: List[List[str]] = []
    processed = 0

    while current_level:
        levels.append(current_level)
        processed += len(current_level)
        next_level_set: Set[str] = set()

        for node in current_level:
            for neighbor in adj.get(node, set()):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level_set.add(neighbor)

        current_level = sorted(next_level_set)

    if processed != len(nodes):
        remaining = nodes - {n for level in levels for n in level}
        raise TopologicalSortError(remaining)

    return levels


if __name__ == "__main__":
    # Demo: topological sort of a sample DAG
    demo_nodes = {"A", "B", "C", "D", "E", "F"}
    demo_adj: Dict[str, Set[str]] = {
        "A": {"C", "D"},
        "B": {"D"},
        "C": {"E"},
        "D": {"E", "F"},
        "E": {"F"},
        "F": set(),
    }

    print("=== Topological Sort ===")
    order = topological_sort(demo_nodes, demo_adj)
    print(f"Order: {order}")

    print("\n=== Parallel Levels ===")
    levels = topological_sort_levels(demo_nodes, demo_adj)
    for i, level in enumerate(levels):
        print(f"Level {i}: {level}")

    # Demo: cycle detection
    print("\n=== Cycle Detection ===")
    cyclic_nodes = {"X", "Y", "Z"}
    cyclic_adj: Dict[str, Set[str]] = {
        "X": {"Y"},
        "Y": {"Z"},
        "Z": {"X"},
    }
    try:
        topological_sort(cyclic_nodes, cyclic_adj)
    except TopologicalSortError as e:
        print(f"Caught: {e}")
