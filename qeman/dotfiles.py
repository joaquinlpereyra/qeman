from pathlib import Path
import json
import tomllib
import psutil
import os
from contextlib import contextmanager
from typing import Generator, IO

DEFAULT_DATA_DIR = Path(os.getenv("QEMAN_HOME", Path.home() / ".qeman"))
IMAGES_DIR = DEFAULT_DATA_DIR / "imgs"
LOCKS_DIR = DEFAULT_DATA_DIR / "locks"
MONITOR_DIR = DEFAULT_DATA_DIR / "monitors"
LOG_DIR = DEFAULT_DATA_DIR / "logs"

CONFIG_PATH = DEFAULT_DATA_DIR / "config.toml"
RUNNING_FILE = DEFAULT_DATA_DIR / "running.json"
SSH_CONFIG = DEFAULT_DATA_DIR / "ssh"

DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(parents=True, exist_ok=True)
MONITOR_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

if not CONFIG_PATH.exists():
    default_config = '''
[binaries]
qemu_img = "qemu-img"
qemu_system = "qemu-system-x86_64"
'''
    CONFIG_PATH.write_text(default_config)

METADATA_SUFFIX = ".meta.json"
MONITOR_SUFFIX = ".monitor"
SSH_BASE_PORT = 4242

DEFAULT_BINARIES = {
    "qemu_img": "qemu-img",
    "qemu_system": "qemu-system-x86_64"
} 

def get_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    return {}

def get_ssh_config() -> Path:
    return SSH_CONFIG

def get_images() -> list[Path]:
    return [f for f in IMAGES_DIR.glob("*") if not f.name.endswith(METADATA_SUFFIX)]
    
def get_monitor(image: str) -> Path:
    monitor_path = MONITOR_DIR / f"{image}_monitor.sock"
    return monitor_path

def get_log_path(image: str) -> Path:
    return LOG_DIR / f"{image}.log"

def get_running_vms() -> dict:
    if not RUNNING_FILE.exists():
        return {}
    data = json.load(open(RUNNING_FILE))
    return data

def get_binary(name: str) -> str:
    config = get_config()
    return config.get("binaries", {}).get(name, DEFAULT_BINARIES[name])

def get_next_ssh_port() -> int:
    running = get_running_vms()
    used = {info["ssh_port"] for info in running.values()}
    port = SSH_BASE_PORT
    while port in used:
        port += 1
    return port

def get_metadata(image_path: Path):
    meta_path = image_path.with_suffix(image_path.suffix + METADATA_SUFFIX)
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f)
    return {}

def get_image(image_name: str) -> Path:
    return IMAGES_DIR / image_name

@contextmanager
def lock_image(image: str) -> Generator[None, None, None]:
    lock_path = LOCKS_DIR / f"{image}.lock"
    if lock_path.exists():
        raise FileExistsError
    lock_path.touch()
    try:
        yield
    finally:
        if lock_path.exists():
            lock_path.unlink()

def is_locked(image: str):
    return (LOCKS_DIR / f"{image}.lock").exists()

def set_metadata(image_path: Path, metadata: dict):
    meta_path = image_path.with_suffix(image_path.suffix + METADATA_SUFFIX)
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

def set_running_vm(image_name: str, pid: int, ssh_port: int):
    RUNNING_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = json.load(open(RUNNING_FILE)) if RUNNING_FILE.exists() else {}
    data[image_name] = {"pid": pid, "ssh_port": ssh_port}
    json.dump(data, open(RUNNING_FILE, "w"), indent=2)

def clean_stale_vms():
    if not RUNNING_FILE.exists():
        return
    data = json.load(open(RUNNING_FILE))
    changed = False

    for name in list(data.keys()):
        pid = data[name]["pid"]
        try:
            psutil.Process(pid)
        except psutil.NoSuchProcess:
            lock_path = LOCKS_DIR / name
            if lock_path.exists():
                lock_path.unlink()
            monitor_path = MONITOR_DIR / f"{name}_monitor.sock"
            if monitor_path.exists():
                monitor_path.unlink()
            del data[name]
            changed = True

    if changed:
        json.dump(data, open(RUNNING_FILE, "w"), indent=2)
