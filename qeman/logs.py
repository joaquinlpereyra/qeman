from qeman import dotfiles
from contextlib import contextmanager
from datetime import datetime
from typing import IO, Literal
import json
from pathlib import Path
from typing import Generator

StreamStr = Literal["stdout", "stderr"]

@contextmanager
def log_file(image: str) -> Generator[IO[str], None, None]:
    path: Path = dotfiles.get_log_path(image)
    f: IO[str] = open(path, "a", encoding="utf-8")
    try:
        yield f
    finally:
        f.close()

def write_event(image: str, event: str, fields):
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "event": event,
        **fields
    }
    with log_file(image) as f:
        f.write(json.dumps(payload) + "\n")
        f.flush()

def write_stream(f: IO[str], stream: StreamStr, data: bytes):
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "to": stream,
        "data": data.decode(errors="replace").rstrip()
    }
    f.write(json.dumps(payload) + "\n")
    f.flush()