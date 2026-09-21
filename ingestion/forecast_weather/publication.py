"""No-replace publication for one local forecast file, not a batch transaction."""
from os import fstat
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Callable


def _remove_owned(path: Path, identity: tuple[int, int], error: BaseException) -> None:
    try:
        current = path.lstat()
        if (current.st_dev, current.st_ino) == identity:
            path.unlink()
    except FileNotFoundError:
        pass
    except OSError as cleanup_error:
        error.add_note(f"Could not clean up forecast file {path}: {cleanup_error}")


def publish_new_file(destination: Path, writer: Callable[[BinaryIO], object]) -> Path:
    """Expose only completed bytes; refuse existing names, including symlinks.

    Requires a trusted local directory hierarchy and hard-link support. Caught
    failures attempt identity-aware cleanup; process/power loss is not covered.
    """
    destination = Path(destination)
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    else:
        raise FileExistsError(f"Refusing to overwrite {destination}.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged: Path | None = None
    identity: tuple[int, int] | None = None
    publication_attempted = False
    try:
        # Random exclusive staging, not a predictable .tmp.parquet output.
        with NamedTemporaryFile(
            mode="w+b", prefix=".forecast-", suffix=".part",
            dir=destination.parent, delete=False,
        ) as handle:
            staged = Path(handle.name)
            info = fstat(handle.fileno())
            identity = (info.st_dev, info.st_ino)
            writer(handle)
        # Set before the syscall, covering an interruption after it creates a link.
        publication_attempted = True
        try:
            destination.hardlink_to(staged)
        except FileExistsError as exc:
            raise FileExistsError(f"Refusing to overwrite {destination}.") from exc
        staged.unlink()
    except BaseException as error:
        if identity is not None:
            if publication_attempted:
                _remove_owned(destination, identity, error)
            if staged is not None:
                _remove_owned(staged, identity, error)
        raise
    return destination
