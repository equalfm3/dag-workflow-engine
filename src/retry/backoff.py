"""Exponential backoff with jitter.

Computes retry delays using exponential backoff with full jitter
and a minimum floor. Prevents thundering herd problems when multiple
tasks retry simultaneously.
"""

from __future__ import annotations

import argparse
import random
import time
from typing import Optional


def compute_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """Compute the backoff delay for a retry attempt.

    Uses exponential backoff: delay = base * 2^attempt, capped at
    max_delay. With jitter enabled, the actual delay is drawn from
    Uniform(floor, computed_delay) where floor = base_delay / 2.

    Args:
        attempt: Zero-indexed retry attempt number.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay cap in seconds.
        jitter: Whether to add randomized jitter.

    Returns:
        The computed delay in seconds.
    """
    exponential = base_delay * (2 ** attempt)
    capped = min(exponential, max_delay)

    if jitter:
        floor = base_delay * 0.5
        return random.uniform(floor, capped)
    return capped


def compute_backoff_sequence(
    max_attempts: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> list[float]:
    """Compute a full sequence of backoff delays.

    Args:
        max_attempts: Number of retry attempts.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay cap in seconds.
        jitter: Whether to add randomized jitter.

    Returns:
        List of delay values, one per attempt.
    """
    return [
        compute_backoff(i, base_delay, max_delay, jitter)
        for i in range(max_attempts)
    ]


def success_probability(
    failure_rate: float,
    max_retries: int,
) -> float:
    """Compute probability of eventual success with retries.

    P(success) = 1 - q^(n+1) where q is per-attempt failure rate
    and n is the number of retries.

    Args:
        failure_rate: Per-attempt failure probability (0 to 1).
        max_retries: Maximum number of retry attempts.

    Returns:
        Probability of at least one successful attempt.
    """
    if not 0.0 <= failure_rate <= 1.0:
        raise ValueError(f"failure_rate must be in [0, 1], got {failure_rate}")
    return 1.0 - (failure_rate ** (max_retries + 1))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exponential backoff demo")
    parser.add_argument("--task", default="sample_task", help="Task name")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--base-delay", type=float, default=1.0)
    parser.add_argument("--max-delay", type=float, default=60.0)
    args = parser.parse_args()

    print(f"Backoff schedule for task '{args.task}':")
    print(f"  Base delay: {args.base_delay}s, Max delay: {args.max_delay}s")
    print()

    # Show with jitter
    print("With jitter:")
    delays = compute_backoff_sequence(
        args.max_retries, args.base_delay, args.max_delay, jitter=True
    )
    total = 0.0
    for i, delay in enumerate(delays):
        total += delay
        print(f"  Attempt {i + 1}: wait {delay:.3f}s (cumulative: {total:.3f}s)")

    # Show without jitter
    print("\nWithout jitter:")
    delays = compute_backoff_sequence(
        args.max_retries, args.base_delay, args.max_delay, jitter=False
    )
    total = 0.0
    for i, delay in enumerate(delays):
        total += delay
        print(f"  Attempt {i + 1}: wait {delay:.3f}s (cumulative: {total:.3f}s)")

    # Success probability
    print("\nSuccess probability (failure_rate=0.1):")
    for n in range(1, args.max_retries + 1):
        prob = success_probability(0.1, n)
        print(f"  {n} retries: {prob:.6f}")
