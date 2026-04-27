"""Retry policy configuration.

Defines which exceptions are retryable, maximum attempt counts,
and delay parameters. Policies can be configured per-task or
applied as a DAG-wide default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Set, Tuple, Type


@dataclass
class RetryPolicy:
    """Configuration for task retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts (0 = no retries).
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay cap in seconds.
        jitter: Whether to add randomized jitter to delays.
        retryable_exceptions: Set of exception types that trigger retries.
            If empty, all exceptions are retryable.
        non_retryable_exceptions: Set of exception types that should never
            be retried, even if retryable_exceptions is empty.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: bool = True
    retryable_exceptions: Set[Type[Exception]] = field(default_factory=set)
    non_retryable_exceptions: Set[Type[Exception]] = field(
        default_factory=lambda: {KeyboardInterrupt, SystemExit}  # type: ignore[arg-type]
    )

    def should_retry(
        self,
        error: Optional[Exception],
        attempt: int,
        max_override: Optional[int] = None,
    ) -> bool:
        """Determine whether a failed task should be retried.

        Args:
            error: The exception that caused the failure, or None.
            attempt: Current attempt number (0-indexed).
            max_override: Override max_attempts for this check.

        Returns:
            True if the task should be retried.
        """
        max_att = max_override if max_override is not None else self.max_attempts
        if attempt >= max_att:
            return False

        if error is None:
            return False

        # Check non-retryable first
        for exc_type in self.non_retryable_exceptions:
            if isinstance(error, exc_type):
                return False

        # If retryable set is specified, error must match
        if self.retryable_exceptions:
            return any(
                isinstance(error, exc_type)
                for exc_type in self.retryable_exceptions
            )

        # Default: all exceptions are retryable
        return True

    def with_overrides(self, **kwargs: object) -> "RetryPolicy":
        """Create a new policy with specific fields overridden.

        Args:
            **kwargs: Fields to override.

        Returns:
            A new RetryPolicy with the specified overrides.
        """
        current = {
            "max_attempts": self.max_attempts,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
            "jitter": self.jitter,
            "retryable_exceptions": set(self.retryable_exceptions),
            "non_retryable_exceptions": set(self.non_retryable_exceptions),
        }
        current.update(kwargs)
        return RetryPolicy(**current)  # type: ignore[arg-type]


# Common preset policies
AGGRESSIVE_RETRY = RetryPolicy(
    max_attempts=5,
    base_delay=0.5,
    max_delay=30.0,
)

CONSERVATIVE_RETRY = RetryPolicy(
    max_attempts=2,
    base_delay=5.0,
    max_delay=120.0,
)

NETWORK_RETRY = RetryPolicy(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    retryable_exceptions={
        ConnectionError,
        TimeoutError,
        OSError,
    },
)

NO_RETRY = RetryPolicy(max_attempts=0)


if __name__ == "__main__":
    print("=== Retry Policy Demo ===\n")

    policy = RetryPolicy(max_attempts=3)
    print(f"Default policy: max_attempts={policy.max_attempts}")

    # Test various scenarios
    scenarios = [
        (ConnectionError("timeout"), 0, "Connection error, attempt 0"),
        (ConnectionError("timeout"), 2, "Connection error, attempt 2"),
        (ConnectionError("timeout"), 3, "Connection error, attempt 3 (exhausted)"),
        (ValueError("bad input"), 0, "ValueError, attempt 0"),
        (None, 0, "No error"),
    ]

    for error, attempt, desc in scenarios:
        should = policy.should_retry(error, attempt)
        print(f"  {desc}: retry={should}")

    # Network-only policy
    print(f"\nNetwork policy (retryable={NETWORK_RETRY.retryable_exceptions}):")
    print(f"  ConnectionError: {NETWORK_RETRY.should_retry(ConnectionError(), 0)}")
    print(f"  ValueError: {NETWORK_RETRY.should_retry(ValueError(), 0)}")

    # Preset policies
    print(f"\nPresets:")
    print(f"  Aggressive: {AGGRESSIVE_RETRY.max_attempts} attempts, {AGGRESSIVE_RETRY.base_delay}s base")
    print(f"  Conservative: {CONSERVATIVE_RETRY.max_attempts} attempts, {CONSERVATIVE_RETRY.base_delay}s base")
    print(f"  No retry: {NO_RETRY.max_attempts} attempts")
