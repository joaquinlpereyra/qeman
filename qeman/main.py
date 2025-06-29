import typer
import subprocess
from pathlib import Path
from typing import Optional, List
import os
import json

app = typer.Typer(help="Unified QEMU CLI tool")

DEFAULT_DATA_DIR = Path(os.getenv("QEMAN_HOME", Path.home() / ".qeman"))
IMAGES_DIR = DEFAULT_DATA_DIR / "imgs"
LOCKS_DIR = DEFAULT_DATA_DIR / "locks"

# Ensure directories are created
DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(parents=True, exist_ok=True)

METADATA_SUFFIX = ".meta.json"

def run_command(cmd: List[str]):
    typer.echo(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        typer.echo(f"Command failed with exit code {e.returncode}: {' '.join(cmd)}", err=True)
        raise typer.Exit(code=e.returncode)

def resolve_image(name_or_path: str) -> Path:
    candidate = Path(name_or_path)
    if candidate.exists():
        return candidate
    else:
        img_path = IMAGES_DIR / name_or_path
        if img_path.exists():
            return img_path
        img_base_path = IMAGES_DIR / f"{name_or_path}.base"
        if img_base_path.exists():
            return img_base_path
        raise typer.BadParameter(f"Image '{name_or_path}' not found as file or in image registry")

def validate_qcow2_format(image_path: Path):
    result = subprocess.run(["qemu-img", "info", "--output=json", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0:
        typer.echo(f"Failed to inspect image: {image_path}", err=True)
        raise typer.Exit(code=1)
    if 'qcow2' not in result.stdout:
        typer.echo(f"Warning: image {image_path.name} does not appear to be in qcow2 format.", err=True)
        raise typer.Exit(code=1)

def lock_image(image_path: Path):
    lock_path = LOCKS_DIR / image_path.name
    if lock_path.exists():
        typer.echo(f"Image {image_path.name} appears to be in use (lockfile exists).", err=True)
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
def fork(
    base_image: str,
    new_image: str,
):
    """Create a new QCOW2 overlay image."""
    base_path = resolve_image(base_image)
    new_path = IMAGES_DIR / new_image

    if new_path.exists():
        raise typer.BadParameter(f"Target image already exists: {new_path}")

    if base_path.suffix != ".base":
        locked_path = base_path.with_suffix(base_path.suffix + ".base")
        base_path.rename(locked_path)
        base_path = locked_path

    cmd = [
        "qemu-img", "create", "-f", "qcow2", "-F", "qcow2",
        "-b", str(base_path), str(new_path)
    ]
    run_command(cmd)

@app.command()
def snap(
    action: str = typer.Option(..., help="Action: list, create, apply, delete"),
    name: Optional[str] = typer.Argument(None, help="Snapshot name (if required)"),
    image: str = typer.Argument(..., help="Image name or path"),
    extra: List[str] = typer.Argument(None, help="Extra args to qemu-img snapshot")
):
    """Manage internal qcow2 snapshots."""
    image_path = resolve_image(image)
    validate_qcow2_format(image_path)
    lock_path = lock_image(image_path)

    try:
        cmd = ["qemu-img", "snapshot"]
        if action == "list":
            cmd += ["-l", str(image_path)]
        elif action == "create":
            if not name:
                raise typer.BadParameter("Snapshot name is required for create")
            cmd += ["-c", name, str(image_path)]
        elif action == "apply":
            if not name:
                raise typer.BadParameter("Snapshot name is required for apply")
            cmd += ["-a", name, str(image_path)]
        elif action == "delete":
            if not name:
                raise typer.BadParameter("Snapshot name is required for delete")
            cmd += ["-d", name, str(image_path)]
        else:
            raise typer.BadParameter(f"Unknown action: {action}")

        if extra:
            cmd += extra

        run_command(cmd)
    finally:
        unlock_image(lock_path)

@app.command()
def new(
    image_name: str = typer.Argument(..., help="Name of the new image"),
    iso: Path = typer.Argument(..., help="Installer ISO"),
    extra: List[str] = typer.Argument(None, help="Extra args to qemu-system")
):
    """Create a new VM by booting from an installer ISO."""
    if not iso.exists():
        raise typer.BadParameter(f"Installer ISO not found: {iso}")

    image_path = IMAGES_DIR / image_name

    if not image_path.exists():
        typer.echo(f"Creating image: {image_path}")
        subprocess.run(["qemu-img", "create", "-f", "qcow2", str(image_path), "40G"], check=True)

    metadata = {
        "created_from_iso": str(iso),
        "notes": "",
    }
    write_metadata(image_path, metadata)

    cmd = [
        "qemu-system-x86_64",
        "-enable-kvm",
        "-m", "8G",
        "-cpu", "host",
        "-smp", "2",
        "-drive", f"file={image_path},format=qcow2,if=virtio",
        "-cdrom", str(iso),
        "-boot", "d",
        "-netdev", "user,id=net0",
        "-device", "virtio-net-pci,netdev=net0",
        "--display", "gtk"
    ]

    if extra:
        cmd += extra

    run_command(cmd)

@app.command()
def run(
    image: str = typer.Argument(..., help="Image name or path"),
    mount: Optional[Path] = typer.Option(None, help="Path to shared folder"),
    graphical: bool = typer.Option(False, "--graphical", help="Enable GTK display"),
    extra: List[str] = typer.Argument(None, help="Extra args to qemu-system")
):
    """Run a VM from an existing image."""
    image_path = resolve_image(image)

    if image_path.name.endswith(".base"):
        typer.echo(f"Cannot run image '{image_path.name}' — it is locked as a base image.", err=True)
        raise typer.Exit(code=1)

    validate_qcow2_format(image_path)

    cmd = [
        "qemu-system-x86_64",
        "-enable-kvm",
        "-m", "8G",
        "-cpu", "host",
        "-smp", "4",
        "-drive", f"file={image_path},format=qcow2,if=virtio",
        "-boot", "c",
        "-spice", "port=5930,disable-ticketing=on",
        "-netdev", "user,id=net0,hostfwd=tcp::2222-:22",
        "-device", "virtio-net-pci,netdev=net0",
        "-device", "virtio-serial",
        "-device", "virtio-balloon",
        "-chardev", "spicevmc,id=vdagent,name=vdagent",
        "-device", "virtserialport,chardev=vdagent,name=com.redhat.spice.0",
    ]

    if mount:
        cmd += [
            "-fsdev", f"local,id=fsdev0,path={mount},security_model=none",
            "-device", "virtio-9p-pci,fsdev=fsdev0,mount_tag=quarantine"
        ]

    if graphical:
        cmd += ["--display", "gtk"]
    else:
        cmd += ["-nographic"]

    if extra:
        cmd += extra

    run_command(cmd)

@app.command()
def list_images():
    """List all images managed in the image registry."""
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    typer.echo("Available images:")
    for img in sorted(IMAGES_DIR.glob("*")):
        if img.suffix == METADATA_SUFFIX:
            continue
        meta = read_metadata(img)
        note = meta.get("notes", "")
        typer.echo(f"- {img.name}" + (f" — {note}" if note else ""))

@app.command()
def info(image: str):
    """Show detailed info about an image."""
    image_path = resolve_image(image)
    result = subprocess.run(["qemu-img", "info", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0:
        typer.echo("Failed to retrieve image info.", err=True)
        raise typer.Exit(code=1)
    typer.echo(result.stdout)

@app.command()
def version():
    """Print version"""
    typer.echo("qeman v0.3.0")

if __name__ == "__main__":
    app()
