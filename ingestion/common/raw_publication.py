"""Publish one observed raw JSON capture without replacing retained evidence."""
from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


def write_raw_json(data: dict[str, Any], destination: str | Path) -> Path:
    """Serialize privately, then expose one complete file using a no-replace link.

    The caller supplies a trusted local destination. A caught failure rolls back
    only this invocation's inode when filesystem cleanup is possible. This is
    not a power-loss durability guarantee or a hostile-directory sandbox.
    """
    path = Path(destination)
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise FileExistsError(f"Refusing to overwrite raw evidence: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    identity: tuple[int, int] | None = None
    try:
        with TemporaryDirectory(prefix=".raw-stage-", dir=path.parent) as temporary:
            staged = Path(temporary) / "payload.part"
            with staged.open("x", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, allow_nan=False)
            status = staged.stat()
            # Record before linking so an interrupt just after link creation is covered.
            identity = (status.st_dev, status.st_ino)
            try:
                path.hardlink_to(staged)
            except FileExistsError as exc:
                raise FileExistsError(f"Refusing to overwrite raw evidence: {path}") from exc
    except BaseException as error:
        if identity is not None:
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == identity:
                    path.unlink()
            except FileNotFoundError:
                pass
            except OSError as cleanup_error:
                error.add_note(f"Could not roll back {path}: {cleanup_error}")
        raise
    return path
