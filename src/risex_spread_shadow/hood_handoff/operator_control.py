"""Local single-writer boundaries for operator-initiated cycles."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import stat


@contextmanager
def exclusive_lock(path: Path):
    parent = path.parent
    info = parent.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise RuntimeError('operator directory must be owner-only and not a symlink')
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_nlink != 1:
            raise RuntimeError('operator lock is unsafe')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another operator process is active') from None
        yield fd
    finally:
        # Do not explicitly unlock: a detached child may own an inherited copy.
        os.close(fd)


# Owner /stop marker next to the operator configuration.  Its presence alone is
# the request (readers never parse it), so a leftover can only make a cycle
# stop early; the Telegram controller removes it when a stop completes and at
# every fresh /run.
STOP_REQUEST_NAME = 'stop-request.json'


def stop_request_path(operator_dir) -> Path:
    return Path(operator_dir) / STOP_REQUEST_NAME


def stop_requested(operator_dir) -> bool:
    path = stop_request_path(operator_dir)
    return path.is_symlink() or path.exists()


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_stop_request(operator_dir, record) -> None:
    """Atomically create the owner-only marker (fixed, credential-free fields)."""
    import json
    import uuid

    directory = Path(operator_dir)
    temporary = directory / f'.stop-request-{uuid.uuid4().hex}'
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(record, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, stop_request_path(directory))
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    _fsync_directory(directory)


def clear_stop_request(operator_dir) -> bool:
    path = stop_request_path(operator_dir)
    if not stop_requested(operator_dir):
        return False
    path.unlink()
    _fsync_directory(Path(operator_dir))
    return True
