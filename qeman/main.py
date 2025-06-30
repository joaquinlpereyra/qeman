import typer
import subprocess
from pathlib import Path
from typing import Optional, List
import os
import json
import time
import itertools
import threading
import tomllib
import psutil

app = typer.Typer(help="Unified QEMU CLI tool")
snap_app = typer.Typer(help="Manage internal qcow2 snapshots")
list_app = typer.Typer(help="List state, like images and running VMs")
app.add_typer(snap_app, name="snap")
app.add_typer(list_app, name="list")

DEFAULT_DATA_DIR = Path(os.getenv("QEMAN_HOME", Path.home() / ".qeman"))
IMAGES_DIR = DEFAULT_DATA_DIR / "imgs"
LOCKS_DIR = DEFAULT_DATA_DIR / "locks"
CONFIG_PATH = DEFAULT_DATA_DIR / "config.toml"
RUNNING_FILE = DEFAULT_DATA_DIR / "running.json"

DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(parents=True, exist_ok=True)

if not CONFIG_PATH.exists():
    default_config = '''
[binaries]
qemu_img = "qemu-img"
qemu_system = "qemu-system-x86_64"
'''
    CONFIG_PATH.write_text(default_config)

METADATA_SUFFIX = ".meta.json"

DEFAULT_BINARIES = {
    "qemu_img": "qemu-img",
    "qemu_system": "qemu-system-x86_64"
}

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    return {}

def get_binary(name: str) -> str:
    config = load_config()
    return config.get("binaries", {}).get(name, DEFAULT_BINARIES[name])

def run_command(cmd: list[str], detach: bool = False) -> Optional[int]:
    typer.echo(f"Running: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(cmd)
        if not detach:
            def wait_and_warn(p: subprocess.Popen):
                p.wait()
                typer.echo(f"Process {p.pid} exited with code {p.returncode}")

            threading.Thread(target=wait_and_warn, args=(proc,), daemon=True).start()
        return proc.pid
    except Exception as e:
        typer.echo(f"Command failed: {e}", err=True)
        raise typer.Exit(code=1)

def resolve_image(name_or_path: str) -> Path:
    candidate = Path(name_or_path)
    if candidate.exists():
        return candidate
    img_path = IMAGES_DIR / name_or_path
    if img_path.exists():
        return img_path
    raise typer.BadParameter(f"Image '{name_or_path}' not found as file or in image registry")

def validate_qcow2_format(image_path: Path):
    result = subprocess.run([get_binary("qemu_img"), "info", "--output=json", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0 or 'qcow2' not in result.stdout:
        typer.echo(f"Invalid image format or failed to inspect: {image_path.name}", err=True)
        raise typer.Exit(code=1)

def lock_image(image_path: Path):
    lock_path = LOCKS_DIR / image_path.name
    if lock_path.exists():
        typer.echo(f"Image {image_path.name} appears to be in use.", err=True)
        raise typer.Exit(code=1)
    lock_path.touch()
    return lock_path

def unlock_image(lock_path: Path):
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass

def write_metadata(image_path: Path, metadata: dict):
    meta_path = image_path.with_suffix(image_path.suffix + METADATA_SUFFIX)
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

def read_metadata(image_path: Path):
    meta_path = image_path.with_suffix(image_path.suffix + METADATA_SUFFIX)
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f)
    return {}

@app.command()
def fork(base_image: str, new_image: str):
    base_path = resolve_image(base_image)
    new_path = IMAGES_DIR / new_image
    if new_path.exists():
        raise typer.BadParameter(f"Target image already exists: {new_path}")
    meta = read_metadata(base_path)
    meta["used_as_base"] = True
    write_metadata(base_path, meta)
    cmd = [get_binary("qemu_img"), "create", "-f", "qcow2", "-F", "qcow2", "-b", str(base_path), str(new_path)]
    run_command(cmd)

@snap_app.command("list")
def snap_list(image: str):
    image_path = resolve_image(image)
    validate_qcow2_format(image_path)
    lock_path = lock_image(image_path)
    try:
        run_command([get_binary("qemu_img"), "snapshot", "-l", str(image_path)])
    finally:
        unlock_image(lock_path)

@snap_app.command("create")
def snap_create(image: str, name: str):
    image_path = resolve_image(image)
    validate_qcow2_format(image_path)
    lock_path = lock_image(image_path)
    try:
        run_command([get_binary("qemu_img"), "snapshot", "-c", name, str(image_path)])
    finally:
        unlock_image(lock_path)

@snap_app.command("apply")
def snap_apply(image: str, name: str):
    image_path = resolve_image(image)
    validate_qcow2_format(image_path)
    lock_path = lock_image(image_path)
    try:
        run_command([get_binary("qemu_img"), "snapshot", "-a", name, str(image_path)])
    finally:
        unlock_image(lock_path)

@snap_app.command("delete")
def snap_delete(image: str, name: str):
    image_path = resolve_image(image)
    validate_qcow2_format(image_path)
    lock_path = lock_image(image_path)
    try:
        run_command([get_binary("qemu_img"), "snapshot", "-d", name, str(image_path)])
    finally:
        unlock_image(lock_path)

@app.command()
def new(image_name: str, iso: Path):
    if not iso.exists():
        raise typer.BadParameter(f"Installer ISO not found: {iso}")
    image_path = IMAGES_DIR / image_name
    if not image_path.exists():
        typer.echo(f"Creating image: {image_path}")
        subprocess.run([get_binary("qemu_img"), "create", "-f", "qcow2", str(image_path), "40G"], check=True)
    metadata = {"created_from_iso": str(iso), "notes": ""}
    write_metadata(image_path, metadata)
    cmd = [
        get_binary("qemu_system"),
        "-enable-kvm", "-m", "8G", "-cpu", "host", "-smp", "2",
        "-drive", f"file={image_path},format=qcow2,if=virtio",
        "-cdrom", str(iso), "-boot", "d",
        "-netdev", "user,id=net0", "-device", "virtio-net-pci,netdev=net0",
        "--display", "gtk"
    ]
    run_command(cmd)

def spinner(stop_flag: threading.Event):
    for c in itertools.cycle("|/-\\"):
        if stop_flag.is_set():
            break
        print(f"\rWaiting for VM to boot... {c}", end="", flush=True)
        time.sleep(0.1)
    print("\rBoot wait finished.          ")

def wait_with_spinner(stop_flag: threading.Event, seconds: int):
    thread = threading.Thread(target=spinner, args=(stop_flag,))
    thread.daemon = True
    thread.start()
    time.sleep(seconds)
    stop_flag.set()
    thread.join()

@app.command()
def run(image: str, mount: Optional[Path] = None, graphical: bool = False, detach: bool = True, post: Optional[Path] = None):
    image_path = resolve_image(image)
    meta = read_metadata(image_path)
    if meta.get("used_as_base"):
        typer.echo(f"Image '{image_path.name}' was used as a base. Running it directly may corrupt data.", err=True)
        raise typer.Exit(code=1)

    validate_qcow2_format(image_path)
    cmd = [
        get_binary("qemu_system"), "-enable-kvm", "-m", "8G", "-cpu", "host", "-smp", "4",
        "-drive", f"file={image_path},format=qcow2,if=virtio", "-boot", "c",
        "-netdev", "user,id=net0,hostfwd=tcp::2222-:22",
        "-device", "virtio-net-pci,netdev=net0",
        "-device", "virtio-serial", "-device", "virtio-balloon",
        "-boot", "order=c",
    ]
    if mount:
        cmd += [
            "-fsdev", f"local,id=fsdev0,path={mount},security_model=none",
            "-device", "virtio-9p-pci,fsdev=fsdev0,mount_tag=quarantine"
        ]

    if graphical:
        cmd += ["--display", "gtk", 
                "-chardev", "spicevmc,id=vdagent,name=vdagent",
                "-device", "virtserialport,chardev=vdagent,name=com.redhat.spice.0"]
    else:
        cmd += ["--display", "none"]

    pid = run_command(cmd, detach=detach)
    if pid:
        register_running_vm(image_name=image, pid=pid)
    if post:
        typer.echo(f"Waiting for VM to boot to run post script: {post}")
        stop_flag = threading.Event()
        wait_with_spinner(stop_flag, 3)
        post_path = Path(post).expanduser()
        if not post_path.exists() or not os.access(post_path, os.X_OK):
            typer.echo(f"Invalid post-run script: {post_path}", err=True)
            raise typer.Exit(code=1)
        try:
            subprocess.run([str(post_path)], check=True)
        except subprocess.CalledProcessError as e:
            typer.echo(f"Post-run script failed: {e}", err=True)

def register_running_vm(image_name: str, pid: int):
    RUNNING_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = json.load(open(RUNNING_FILE)) if RUNNING_FILE.exists() else {}
        data[image_name] = pid
        json.dump(data, open(RUNNING_FILE, "w"), indent=2)
    except Exception as e:
        typer.echo(f"Failed to write running registry: {e}", err=True)

def get_running_vms() -> dict:
    if RUNNING_FILE.exists():
        data = json.load(open(RUNNING_FILE))
        updated = {k: v for k, v in data.items() if psutil.pid_exists(v)}
        if updated != data:
            json.dump(updated, open(RUNNING_FILE, "w"), indent=2)
        return updated
    return {}

@app.command()
def kill(vm: str):
    running = get_running_vms()
    if vm not in running:
        typer.echo(f"No running VM registered under name '{vm}'", err=True)
        raise typer.Exit(code=1)
    pid = running[vm]
    try:
        psutil.Process(pid).kill()
        typer.echo(f"Killed VM '{vm}' with PID {pid}.")
        del running[vm]
        json.dump(running, open(RUNNING_FILE, "w"), indent=2)
    except Exception as e:
        typer.echo(f"Failed to kill process {pid}: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def info(image: str):
    image_path = resolve_image(image)
    result = subprocess.run([get_binary("qemu_img"), "info", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0:
        typer.echo("Failed to retrieve image info.", err=True)
        raise typer.Exit(code=1)
    typer.echo(result.stdout)

@app.command()
def version():
    typer.echo("qeman v0.1.0")

@list_app.command("images")
def list_cmd_images():
    for img in sorted(IMAGES_DIR.glob("*")):
        if img.name.endswith(METADATA_SUFFIX):
            continue
        meta = read_metadata(img)
        note = meta.get("notes", "")
        typer.echo(f"- {img.name}" + (f" — {note}" if note else ""))

@list_app.command("vms")
def list_cmd_vms():
    running = get_running_vms()
    for name, pid in running.items():
        typer.echo(f"- {name}: PID {pid}")


if __name__ == "__main__":
    app()
