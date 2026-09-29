"""Select augmentation companions in source order, before parallel decoding."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Literal

from ._native import Record, SamplePool


@dataclass(frozen=True)
class SampleGroup:
    primary: Record
    extras: tuple[Record, ...]


class SamplePoolSource:
    """Recent-history sampling without changing primary coverage or doing I/O.

    A fresh pool/RNG is created for each iteration. Determinism assumes the same
    input order and seed; set seed explicitly when epoch-varying sampling is wanted.
    """

    def __init__(
        self,
        source: Iterable[Record],
        *,
        extra_samples: int = 3,
        max_records: int = 1024,
        max_bytes: int = 256 * 1024**2,
        seed: int = 0,
        replace: bool = False,
        warmup: Literal["skip", "replace"] = "skip",
    ) -> None:
        if extra_samples < 0 or extra_samples > max_records:
            raise ValueError("extra_samples must be between zero and max_records")
        if warmup not in ("skip", "replace"):
            raise ValueError("warmup must be 'skip' or 'replace'")
        SamplePool(max_records=max_records, max_bytes=max_bytes, seed=seed)
        self.source = source
        self.extra_samples = extra_samples
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.seed = seed
        self.replace = replace
        self.warmup = warmup

    def __iter__(self) -> Iterator[SampleGroup]:
        pool = SamplePool(max_records=self.max_records, max_bytes=self.max_bytes, seed=self.seed)
        for primary in self.source:
            exclusions = [primary.sample_id]
            available = pool.available(exclude_ids=exclusions)
            use_replacement = self.replace or (
                self.warmup == "replace" and available < self.extra_samples
            )
            if self.extra_samples == 0:
                extras = ()
            elif available and (use_replacement or available >= self.extra_samples):
                extras = tuple(
                    pool.sample(self.extra_samples, exclude_ids=exclusions, replace=use_replacement)
                )
            else:
                extras = ()  # first record / insufficient history: keep the primary
            pool.insert(primary)
            yield SampleGroup(primary, extras)

    def stats(self) -> dict:
        """Forward upstream native counters, if supported by the source."""
        stats = getattr(self.source, "stats", None)
        return stats() if stats is not None else {}
