"""YAML DAG definition parser.

Parses workflow definitions from YAML files into DAG objects.
Supports task metadata, retry configuration, and dependency
declarations.
"""

from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml

from src.dag.graph import DAG, TaskNode


def _resolve_callable(dotted_path: str) -> Optional[Callable[..., Any]]:
    """Resolve a dotted module path to a callable.

    Args:
        dotted_path: e.g. 'src.tasks.etl.extract_data'

    Returns:
        The resolved callable, or None if resolution fails.
    """
    try:
        module_path, func_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)
    except (ImportError, AttributeError, ValueError):
        return None


def parse_dag_yaml(path: str) -> DAG:
    """Parse a YAML file into a DAG object.

    Expected YAML structure:
        name: my_pipeline
        tasks:
          - name: extract
            func: src.tasks.extract  # optional
            retries: 3
            timeout: 60
            metadata:
              priority: high
            depends_on:
              - validate
        edges:  # alternative to depends_on
          - from: extract
            to: transform

    Args:
        path: Path to the YAML file.

    Returns:
        A populated DAG object.

    Raises:
        FileNotFoundError: If the YAML file doesn't exist.
        ValueError: If the YAML structure is invalid.
    """
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"DAG definition not found: {path}")

    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML dict, got {type(data).__name__}")

    dag_name = data.get("name", yaml_path.stem)
    dag = DAG(name=dag_name)

    tasks_data: List[Dict[str, Any]] = data.get("tasks", [])
    if not tasks_data:
        raise ValueError("DAG definition must include at least one task")

    # First pass: create all task nodes
    for task_def in tasks_data:
        if not isinstance(task_def, dict) or "name" not in task_def:
            raise ValueError(f"Invalid task definition: {task_def}")

        func = None
        if "func" in task_def:
            func = _resolve_callable(task_def["func"])

        node = TaskNode(
            name=task_def["name"],
            func=func,
            retries=task_def.get("retries", 0),
            timeout=float(task_def.get("timeout", 300)),
            metadata=task_def.get("metadata", {}),
        )
        dag.add_task(node)

    # Second pass: add edges from depends_on
    for task_def in tasks_data:
        task_name = task_def["name"]
        depends_on: List[str] = task_def.get("depends_on", [])
        for dep in depends_on:
            if dep not in dag:
                raise ValueError(
                    f"Task {task_name!r} depends on unknown task {dep!r}"
                )
            dag.add_edge(dep, task_name)

    # Add explicit edges
    edges_data: List[Dict[str, str]] = data.get("edges", [])
    for edge_def in edges_data:
        src = edge_def.get("from", "")
        dst = edge_def.get("to", "")
        if src not in dag:
            raise ValueError(f"Edge source {src!r} not found in DAG")
        if dst not in dag:
            raise ValueError(f"Edge target {dst!r} not found in DAG")
        dag.add_edge(src, dst)

    return dag


def parse_dag_dict(data: Dict[str, Any]) -> DAG:
    """Parse a dictionary into a DAG object.

    Same structure as the YAML format but from an in-memory dict.

    Args:
        data: Dictionary with 'name', 'tasks', and optional 'edges'.

    Returns:
        A populated DAG object.
    """
    dag_name = data.get("name", "unnamed")
    dag = DAG(name=dag_name)

    for task_def in data.get("tasks", []):
        node = TaskNode(
            name=task_def["name"],
            func=task_def.get("func"),
            retries=task_def.get("retries", 0),
            timeout=float(task_def.get("timeout", 300)),
            metadata=task_def.get("metadata", {}),
        )
        dag.add_task(node)

    for task_def in data.get("tasks", []):
        for dep in task_def.get("depends_on", []):
            dag.add_edge(dep, task_def["name"])

    for edge in data.get("edges", []):
        dag.add_edge(edge["from"], edge["to"])

    return dag


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse a DAG YAML file")
    parser.add_argument(
        "--dag", type=str, default="configs/sample_dag.yaml",
        help="Path to YAML DAG definition",
    )
    args = parser.parse_args()

    dag = parse_dag_yaml(args.dag)
    print(f"Parsed DAG: {dag}")
    print(f"Tasks: {list(dag.nodes.keys())}")
    print(f"Roots: {dag.roots()}")
    print(f"Leaves: {dag.leaves()}")
    for name in dag:
        node = dag.nodes[name]
        deps = dag.predecessors(name)
        print(f"  {name}: retries={node.retries}, deps={deps}")
