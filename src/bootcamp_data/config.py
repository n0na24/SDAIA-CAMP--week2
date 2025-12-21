from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class Paths:
    root: Path
    data: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path


def make_paths(root: Path) -> Paths:
    data = root / "data"

    return Paths(
        root=root,
        data=data,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )
