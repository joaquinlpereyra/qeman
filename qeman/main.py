import urllib.parse
import typer
from typing import List
from typing_extensions import Annotated
from typer import Context, Argument
import subprocess
from pathlib import Path
from typing import Optional, List
import os
import json
import time
import itertools
import threading
import platform
import uuid
from typing import IO, Literal
from qeman import dotfiles
from qeman import logs
from qeman import ps
from qeman import qmp

app = typer.Typer(help="Unified QEMU CLI tool")
snap_app = typer.Typer(help="Manage internal qcow2 snapshots")
list_app = typer.Typer(help="List state, like images and running VMs")
usb_app = typer.Typer(help="USB passthrough controls")
app.add_typer(snap_app, name="snap")
app.add_typer(list_app, name="list")
app.add_typer(usb_app, name="usb")


DEVNULL = open(os.devnull, "wb")
IS_GOOD_OS = not platform.system() == "Darwin"

def open_browser(url: str):
    datadir = Path("~/.qeman/chrome").expanduser()
    datadir.mkdir(parents=True, exist_ok=True)

    print(f"Opening browser at: {url}")
    subprocess.Popen(["google-chrome", f"--app={url}",
                    f"--user-data-dir={datadir}"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def complete_image_names(ctx: typer.Context, args: List[str], incomplete: str):
    for img in sorted(dotfiles.get_images()):
        if incomplete in img.name:
            yield img.name

def running_vm_names(ctx: Context, args: List[str], incomplete: str):
    already_entered = set(args)
    for vm in dotfiles.get_running_vms().keys():
        if vm.startswith(incomplete) and vm not in already_entered:
            yield vm

def complete_usb_device_ids(ctx: typer.Context, incomplete: str):
    vm = ctx.params.get("vm")
    if not vm:
        return []

    for dev in dotfiles.list_usb_devices(vm):
        if dev.startswith(incomplete):
            yield dev

def run_command(cmd: List[str], log: Optional[IO[str]] = None) -> int:
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE if log else None,
        stderr=subprocess.PIPE if log else None,
        start_new_session=True
    )

    if log is None:
        return proc.pid

    def stream(pipe: IO[bytes], stream_name: Literal["stdout", "stderr"]):
        for line in pipe:
            logs.write_stream(log_path, stream_name, line)

    threading.Thread(target=stream, args=(proc.stdout, "stdout"), daemon=True).start()
    threading.Thread(target=stream, args=(proc.stderr, "stderr"), daemon=True).start()

    return proc.pid

def validate_qcow2_format(image_path: Path):
    result = subprocess.run([dotfiles.get_binary("qemu_img"), "info", "--output=json", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0 or 'qcow2' not in result.stdout:
        typer.echo(f"Invalid image format or failed to inspect: {image_path.name}", err=True)
        raise typer.Exit(code=1)

@app.command()
def fork(
    base_image: Annotated[str, typer.Argument(help="Base image to fork", autocompletion=complete_image_names)],
    new_image: Annotated[str, Argument(help="Name for the new forked image")],
):
    """Create a copy-on-write overlay image from a base image."""
    base_path = dotfiles.get_image(base_image)
    new_path = dotfiles.get_image(new_image)
    if new_path.exists():
        raise typer.BadParameter(f"Target image already exists: {new_path}")
    meta = dotfiles.get_metadata(base_path)
    deps = meta.get("dependents", [])
    if new_image not in deps:
        deps.append(new_image)
    meta["dependents"] = deps
    dotfiles.set_metadata(base_path, meta)
    cmd = [dotfiles.get_binary("qemu_img"), "create", "-f", "qcow2", "-F", "qcow2", "-b", str(base_path), str(new_path)]

    logs.write_event(new_image, "fork", {"dependent_of": "base_image"})
    run_command(cmd, logs.log_file(new_image))

@snap_app.command("list")
def snap_list(image: Annotated[str, Argument(help="Image to inspect", autocompletion=complete_image_names)]):
    """List all snapshots for a QCOW2 image."""
    image_path = dotfiles.get_image(image)
    validate_qcow2_format(image_path)
    try:
        lock_path = dotfiles.lock_image(image_path)
        run_command([dotfiles.get_binary("qemu_img"), "snapshot", "-l", str(image_path)])
    except FileExistsError:
        typer.echo(f"Image {image_path.name} appears to be in use.", err=True)
        raise typer.Exit(code=1)

    dotfiles.unlock_image(lock_path)

@snap_app.command("create")
def snap_create(
    image: Annotated[str, Argument(help="Image to snapshot", autocompletion=complete_image_names)],
    name: Annotated[str, Argument(help="Name for the snapshot")],
):
    """Create a named snapshot of a QCOW2 image."""
    image_path = dotfiles.get_image(image)
    validate_qcow2_format(image_path)
    try:
        lock_path = dotfiles.lock_image(image_path)
        run_command([dotfiles.get_binary("qemu_img"), "snapshot", "-c", name, str(image_path)])
    except FileExistsError:
        typer.echo(f"Image {image_path.name} appears to be in use.", err=True)
        raise typer.Exit(code=1)
    dotfiles.unlock_image(lock_path)

@snap_app.command("apply")
def snap_apply(
    image: Annotated[str, Argument(help="Image to restore", autocompletion=complete_image_names)],
    name: Annotated[str, Argument(help="Snapshot name to apply")],
):
    """Restore a QCOW2 image to a named snapshot."""
    image_path = dotfiles.get_image(image)
    validate_qcow2_format(image_path)
    try:
        lock_path = dotfiles.lock_image(image_path)
        run_command([dotfiles.get_binary("qemu_img"), "snapshot", "-a", name, str(image_path)])
    except FileExistsError:
        typer.echo(f"Image {image_path.name} appears to be in use.", err=True)
        raise typer.Exit(code=1)
    dotfiles.unlock_image(lock_path)

@snap_app.command("delete")
def snap_delete(
    image: Annotated[str, Argument(help="Image containing snapshot", autocompletion=complete_image_names)],
    name: Annotated[str, Argument(help="Snapshot name to delete")],
):
    """Delete a snapshot from a QCOW2 image."""
    image_path = dotfiles.get_image(image)
    validate_qcow2_format(image_path)
    try:
        lock_path = dotfiles.lock_image(image_path)
        run_command([dotfiles.get_binary("qemu_img"), "snapshot", "-d", name, str(image_path)])
    except FileExistsError:
        typer.echo(f"Image {image_path.name} appears to be in use.", err=True)
        raise typer.Exit(code=1)

    dotfiles.unlock_image(lock_path)

@app.command()
def new(
    image_name: Annotated[str, Argument(help="Name for the new VM image")],
    iso: Annotated[Path, Argument(help="Path to installer ISO")],
):
    """Create a new VM image and boot from an installer ISO."""
    if not iso.exists():
        raise typer.BadParameter(f"Installer ISO not found: {iso}")
    image_path = dotfiles.get_image(image_name)
    if not image_path.exists():
        typer.echo(f"Creating image: {image_path}")
        subprocess.run([dotfiles.get_binary("qemu_img"), "create", "-f", "qcow2", str(image_path), "100G"], check=True)
    metadata = {"created_from_iso": str(iso), "notes": ""}
    dotfiles.set_metadata(image_path, metadata)
    monitor_path = dotfiles.get_monitor(image_path)

    cmd = [
        dotfiles.get_binary("qemu_system"),
        "-m", "8G", "-smp", "2",
        "-drive", f"file={image_path},format=qcow2,if=virtio",
        "-cdrom", str(iso), "-boot", "d",
        "-netdev", "user,id=net0", "-device", "virtio-net-pci,netdev=net0",
        "-qmp-pretty", f"unix:{monitor_path},server,nowait",
    ]

    if IS_GOOD_OS:
        cmd += ["--enable-kvm", "-cpu", "host", "--display", "gtk"]
    else:
        cmd += ["--display", "cocoa"]

    logs.write_event(image_name, "create", {})
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

def ssh_command(port):
    key_path = Path(dotfiles.get_ssh_config().get("key_path"))
    key_path = str(key_path.expanduser())
    ssh_cmd = ["ssh", "-p", str(port),
                # IdentityOnly: prevent the agent from offering
                # other identities than specified by the `key_path` here
                "-o", "IdentitiesOnly=yes",
                # we are generating hosts like a madman and only locally
                # sometimes reusing ports
                "-oStrictHostKeyChecking=no",
                # ignore known hosts for same reason as above
                "-oUserKnownHostsFile=/dev/null",
                # Ignore our `.ssh` configuration file
                "-F", "/dev/null",
                "-i", str(key_path), "j@localhost"
                ]
    return ssh_cmd

def run_in_vm(port, cmd: str):
    ssh_cmd = ssh_command(port)
    ssh_cmd.append(cmd)
    return subprocess.run(ssh_cmd,
                    text=True,
                    capture_output=True,
                    check=True)

@app.command()
def connect(vm: Annotated[str, Argument(help="VM to connect to", autocompletion=running_vm_names)]):
    """SSH into a running VM."""
    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"No VM named '{vm}' running.", err=True)
        raise typer.Exit(1)
    port = info["ssh_port"]
    ssh_cmd = ssh_command(port)
    # sysargvs[0] must be the exec name again,
    # so we repeat here
    os.execvp("ssh", ssh_cmd)

@app.command()
def run(
    image: Annotated[str, Argument(autocompletion=complete_image_names, help="Image to run")],
    mount: Annotated[Optional[Path], typer.Option(help="Host directory to mount via 9p")] = None,
    graphical: Annotated[bool, typer.Option(help="Enable graphical display")] = False,
    post: Annotated[Optional[Path], typer.Option(help="Script to run after VM boots")] = None,
):
    """Launch a VM from an existing image."""
    image_path = dotfiles.get_image(image)
    meta = dotfiles.get_metadata(image_path)
    if meta.get("dependents"):
        typer.echo(f"Image '{image_path.name}' has dependent forks. Running it directly may corrupt data.", err=True)
        raise typer.Exit(code=1)

    monitor_path = dotfiles.get_monitor(image)
    validate_qcow2_format(image_path)
    ssh_port = dotfiles.get_next_ssh_port()

    cmd = [
        dotfiles.get_binary("qemu_system"), "-m", "24G", "-smp", "4",
        "-drive", f"file={image_path},format=qcow2,if=virtio", "-boot", "c",
        "-netdev", f"user,id=net0,hostfwd=tcp::{ssh_port}-:22",
        "-device", "virtio-net-pci,netdev=net0",
        "-device", "virtio-serial", "-device", "virtio-balloon",
        "-device", "qemu-xhci,id=xhci", # add usb support
        "-qmp-pretty", f"unix:{monitor_path},server,nowait",
        "-boot", "order=c",
    ]
    if IS_GOOD_OS:
        cmd += [
            "--enable-kvm", "-cpu", "host"
        ]

    if mount:
        cmd += [
            # security_model=mapped makes it so that
            # the 9p mount is owned by the user inside the VM
            # writes permissions in the extended attributes
            # thus, modifying permissions inside the VM does not affect host
            "-fsdev", f"local,id=fsdev0,path={mount},security_model=mapped",
            "-device", "virtio-9p-pci,fsdev=fsdev0,mount_tag=quarantine"
        ]

    if graphical:
        if IS_GOOD_OS:
            cmd += ["-display", "gtk",
                    "-device", "virtio-gpu-pci"]
        else:
            cmd += ["--display", "cocoa"]
    else:
        cmd += ["--display", "none"]

    pid = run_command(cmd)
    dotfiles.set_running_vm(image, pid, ssh_port)

    logs.write_event(image, "run", {"pid": pid})

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

@app.command()
def kill(vms: Annotated[List[str], typer.Argument(help="VMs to stop", autocompletion=running_vm_names)]):
    """Gracefully shut down one or more running VMs."""
    running = dotfiles.get_running_vms()
    for vm in vms:
        if vm not in running:
            typer.echo(f"No running VM registered under name '{vm}'", err=True)
            raise typer.Exit(code=1)

        monitor_path = dotfiles.get_monitor(vm)
        if not monitor_path.exists():
            typer.echo(f"Monitor socket not found for VM '{vm}'", err=True)
            raise typer.Exit(code=1)

        qmp.send_shutdown(monitor_path)

    # There was some logic to remove the monitor manually here
    # BUt qemu appears to remove it automatically on shutdown.

@app.command()
def info(image: Annotated[str, Argument(help="Image to inspect", autocompletion=complete_image_names)]):
    """Display QEMU image information and metadata."""
    image_path = dotfiles.get_image(image)
    result = subprocess.run([dotfiles.get_binary("qemu_img"), "info", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0:
        typer.echo("Failed to retrieve image info.", err=True)
        raise typer.Exit(code=1)
    typer.echo(result.stdout)

@app.command()
def version():
    """Show qeman version."""
    typer.echo("qeman v0.6.0")

@list_app.command("images")
def list_cmd_images():
    """List all managed VM images with metadata."""
    out = []
    for img in sorted(dotfiles.get_images()):
        meta = dotfiles.get_metadata(img)
        # include name + all metadata keys
        entry = {"name": img.name, **meta}
        out.append(entry)
    typer.echo(json.dumps(out, indent=2))


@list_app.command("vms")
def list_cmd_vms():
    """List running VMs with resource usage."""
    running = dotfiles.get_running_vms()
    out = []
    for name, info in running.items():
        pid = info["pid"]
        mem = ps.rss_mb(pid)
        cpu = ps.cpu_percent(pid)

        out.append({
            "name":       name,
            "pid":        pid,
            "ssh_port":   info.get("ssh_port"),
            "memory_rss": f"{mem:.1f}mb" if mem is not None else "-",
            "cpu_percent": f"{cpu:.1f}" if cpu is not None else "-",
        })
    typer.echo(json.dumps(out, indent=2))

@app.command()
def rm(image: Annotated[str, Argument(help="Image to remove", autocompletion=complete_image_names)]):
    """Remove a VM image and its associated files."""
    image_path = dotfiles.get_image(image)

    if not image_path.exists():
        typer.echo(f"Image '{image}' not found.", err=True)
        raise typer.Exit(code=1)

    running = dotfiles.get_running_vms()
    if image in running:
        typer.echo(f"Cannot remove '{image}': VM is running.", err=True)
        raise typer.Exit(code=1)

    meta = dotfiles.get_metadata(image_path)
    if meta.get("dependents"):
        typer.echo(f"Cannot remove '{image}': it has dependent forks.", err=True)
        raise typer.Exit(code=1)

    if dotfiles.is_locked(image):
        typer.echo(f"Image '{image}' appears to be locked. Inspect the lockfile at {dotfiles.LOCKS_DIR}", err=True)
        raise typer.Exit(code=1)

    # Remove image and associated files
    image_path.unlink()
    meta_path = dotfiles.get_metadata(image_path)
    if meta_path.exists():
        meta_path.unlink()

    monitor_path = dotfiles.get_monitor(image)
    if monitor_path.exists():
        monitor_path.unlink()

    log_path = dotfiles.get_log_path(image)
    if log_path.exists():
        log_path.unlink()



@app.command()
def code(vm: Annotated[str, Argument(help="VM to connect to", autocompletion=running_vm_names)]):
    """Open VS Code tunnel to a running VM."""


    def _first_url_in(line: str) -> str | None:
        for tok in line.split():
            if tok.startswith(("http://", "https://")):
                return tok
        return None

    def _is_allowed(url: str) -> bool:
        ALLOWED_HOSTS = {"github.com", "vscode.dev"}
        try:
            u = urllib.parse.urlparse(url)
            return u.scheme == "https" and u.hostname in ALLOWED_HOSTS
        except Exception:
            return False

    def _maybe_device_code(line: str) -> str | None:
        for tok in line.split():
            if len(tok) == 9 and tok[4] == "-" and tok.replace("-", "").isalnum() and tok.isupper():
                return tok
        return None

    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"VM '{vm}' is not running.", err=True)
        raise typer.Exit(1)

    port = info["ssh_port"]
    # run with nohup so tunnel process keeps running
    # in vm, write to log so we can read
    # touch first so tail finds the file for sure
    run_in_vm(port, "touch /tmp/code-tunnel.log && nohup code tunnel > /tmp/code-tunnel.log 2>&1 &")

    read_cmd = ssh_command(port) + ["tail -F /tmp/code-tunnel.log"]
    proc = subprocess.Popen(
        read_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line buffered
    )

    done = threading.Event()

    def monitor():
        link: str | None = None
        code: str | None = None
        tunnel_exists = False

        if not proc.stdout:
            done.set()
            return

        opened = False
        for raw in iter(proc.stdout.readline, ""):
            line = raw.strip()
            print(line)
            if not line or line.startswith("*"):
                continue

            if line.startswith("Connected to an existing tunnel"):
                tunnel_exists = True

            # pick up code if we see one
            if code is None:
                maybe = _maybe_device_code(line)
                if maybe:
                    code = maybe

            url = _first_url_in(line)
            if url:
                url = url.strip()
                if not _is_allowed(url):
                    typer.echo(f"Refusing to open untrusted URL: {url}", err=True)
                    continue  # keep scanning; maybe a valid URL comes next

                link = url
                if not opened:
                    if not tunnel_exists and code:
                        typer.echo(f"Device code: {code}")
                    time.sleep(1)  # tiny debounce, sometimes it hangs without it
                    open_browser(link)
                    opened = True
                    done.set()  # let the CLI return quickly
                    return

        done.set()  # EOF
        return

    t = threading.Thread(target=monitor, daemon=True)
    t.start()

    if not done.wait(timeout=5):
        typer.echo(f"VM '{vm}' tunnel timeout.", err=True)


@usb_app.command("list")
def usb_list(vm: Annotated[str, Argument(autocompletion=running_vm_names)]):
    """List host USB devices visible to QEMU."""
    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"VM '{vm}' is not running.", err=True)
        raise typer.Exit(1)
    port = info["ssh_port"]
    proc = run_in_vm(port, "lsusb")
    print(proc.stdout)

@usb_app.command("attach")
def usb_attach(
    vm: Annotated[str, Argument(autocompletion=running_vm_names)],
    vendor: Optional[str] = typer.Option(None, "--vendor", help="hex, e.g. 0x2c97"),
    product: Optional[str] = typer.Option(None, "--product", help="hex, e.g. 0x0001"),
    hostbus: Optional[int] = typer.Option(None, "--hostbus", help="lsusb Bus number"),
    hostaddr: Optional[int] = typer.Option(None, "--hostaddr", help="lsusb Device address"),
    controller_id: str = typer.Option("xhci", "--controller-id", help="xHCI id in guest"),
):
    """
    Attach a host USB device to the running VM.
    Provide (--vendor,--product) OR (--hostbus,--hostaddr).
    """

    running = dotfiles.get_running_vms()
    if vm not in running:
        typer.echo(f"VM '{vm}' is not running.", err=True); raise typer.Exit(1)
    mon = dotfiles.get_monitor(vm)

    # Ensure an xHCI controller exists (add if missing)
    try:
        qtree = qmp.hmp(mon, "info qtree")
        if f'id "{controller_id}"' not in qtree:
            qmp.exec(mon, "device_add", {"driver": "qemu-xhci", "id": controller_id, "bus": "pcie.0"})
    except Exception as e:
        typer.echo(f"Warning: could not verify/add xHCI: {e}", err=True)

    dev_id = f"usb_{uuid.uuid4().hex[:8]}"
    args: dict = {"driver": "usb-host", "id": dev_id}
    if vendor and product:
        args["vendorid"] = int(vendor, 16) if isinstance(vendor, str) else vendor
        args["productid"] = int(product, 16) if isinstance(product, str) else product
    elif hostbus is not None and hostaddr is not None:
        args["hostbus"] = hostbus
        args["hostaddr"] = hostaddr
    else:
        typer.echo("Provide --vendor/--product OR --hostbus/--hostaddr", err=True)
        raise typer.Exit(2)

    resp = qmp.exec(mon, "device_add", args)
    if "error" in resp:
        typer.echo(f"Attach failed: {resp['error']}", err=True); raise typer.Exit(1)
    dotfiles.add_usb_device(vm, dev_id)
    typer.echo(f"Attached as {dev_id}")

@usb_app.command("detach")
def usb_detach(
    vm: Annotated[str, Argument(autocompletion=running_vm_names)],
    dev_id: str = typer.Argument(..., help="Device id to detach, e.g. usb_abcd1234", autocompletion=complete_usb_device_ids),
):
    """
    Detach a previously attached USB device from the running VM.
    """
    running = dotfiles.get_running_vms()
    if vm not in running:
        typer.echo(f"VM '{vm}' is not running.", err=True); raise typer.Exit(1)
    mon = dotfiles.get_monitor(vm)

    resp = qmp.exec(mon, "device_del", {"id": dev_id})
    if "error" in resp:
        typer.echo(f"Detach failed: {resp['error']}", err=True); raise typer.Exit(1)
    typer.echo(f"Detached {dev_id}")

def get_latest_mtime_local(path: Path) -> float:
    """Get the latest modification time of any file in the local directory."""
    if not path.exists():
        return 0.0
    latest = 0.0
    for root, dirs, files in os.walk(path):
        for f in files:
            try:
                mtime = os.path.getmtime(os.path.join(root, f))
                latest = max(latest, mtime)
            except OSError:
                pass
    return latest

def get_latest_mtime_remote(port: int, remote_path: str) -> float:
    """Get the latest modification time of any file in the remote directory via SSH."""
    ssh_cmd = ssh_command(port)
    # Use find to get all file mtimes and return the max
    cmd = f'find "{remote_path}" -type f -printf "%T@\\n" 2>/dev/null | sort -rn | head -1'
    ssh_cmd.append(cmd)
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError):
        pass
    return 0.0

@app.command()
def sync(
    vm: Annotated[str, Argument(help="VM to sync with", autocompletion=running_vm_names)],
    host_dir: Annotated[str, Argument(help="Local directory path")],
    remote_dir: Annotated[Optional[str], Argument(help="Remote directory under /home/j/ (defaults to host_dir)")] = None,
    host_to_remote: Annotated[bool, typer.Option("--host-to-remote", "-H", help="Force sync from host to remote")] = False,
    remote_to_host: Annotated[bool, typer.Option("--remote-to-host", "-R", help="Force sync from remote to host")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-n", help="Show what would be transferred")] = False,
    delete: Annotated[bool, typer.Option("--delete", help="Delete files not present in source")] = False,
    git_ignore: Annotated[bool, typer.Option("--git-ignore", "-g", help="Respect .gitignore files")] = False,
):
    """Sync a directory between host and a running VM using rsync.

    By default, auto-detects which side has newer changes and syncs accordingly.
    Use --host-to-remote or --remote-to-host to override.

    Examples:
        qeman sync myvm project/foo        # syncs to /home/j/project/foo
        qeman sync myvm ~/code otherdir    # syncs ~/code to /home/j/otherdir
    """
    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"VM '{vm}' is not running.", err=True)
        raise typer.Exit(1)

    port = info["ssh_port"]
    host_path = Path(host_dir).expanduser().resolve()

    # Default remote_dir to host_dir if not specified
    if remote_dir is None:
        remote_dir = host_dir
    remote_path = f"/home/j/{remote_dir}"

    typer.echo(f"Host: {host_path}")
    typer.echo(f"Remote: {remote_path}")

    if host_to_remote and remote_to_host:
        typer.echo("Cannot specify both --host-to-remote and --remote-to-host", err=True)
        raise typer.Exit(1)

    # Determine sync direction
    if host_to_remote:
        direction = "host-to-remote"
    elif remote_to_host:
        direction = "remote-to-host"
    else:
        # Auto-detect based on modification times
        typer.echo("Auto-detecting sync direction...")
        host_mtime = get_latest_mtime_local(host_path)
        remote_mtime = get_latest_mtime_remote(port, remote_path)

        if host_mtime == 0 and remote_mtime == 0:
            typer.echo("Neither directory contains files. Use --host-to-remote or --remote-to-host.", err=True)
            raise typer.Exit(1)
        elif host_mtime >= remote_mtime:
            direction = "host-to-remote"
            typer.echo(f"Host is newer (or equal), syncing host -> remote")
        else:
            direction = "remote-to-host"
            typer.echo(f"Remote is newer, syncing remote -> host")

    # Build SSH options for rsync
    ssh_config = dotfiles.get_ssh_config()
    key_path_str = ssh_config.get("key_path")
    if not key_path_str:
        typer.echo("SSH key path not configured in ~/.qeman/config.toml", err=True)
        raise typer.Exit(1)
    key_path = Path(key_path_str).expanduser()
    ssh_opts = f"ssh -p {port} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -F /dev/null -i {key_path}"

    # Build rsync command
    rsync_cmd = ["rsync", "-avz", "--progress", "-e", ssh_opts]

    if dry_run:
        rsync_cmd.append("--dry-run")
    if delete:
        rsync_cmd.append("--delete")
    if git_ignore:
        rsync_cmd.append("--filter=:- .gitignore")

    if direction == "host-to-remote":
        # Ensure host path exists
        if not host_path.exists():
            typer.echo(f"Host directory does not exist: {host_path}", err=True)
            raise typer.Exit(1)
        # Create remote directory if it doesn't exist
        run_in_vm(port, f'mkdir -p "{remote_path}"')
        # Add trailing slash to copy contents, not the directory itself
        src = f"{host_path}/"
        dst = f"j@localhost:{remote_path}/"
        typer.echo(f"Syncing: {host_path} -> {vm}:{remote_path}")
    else:
        # remote-to-host
        host_path.mkdir(parents=True, exist_ok=True)
        src = f"j@localhost:{remote_path}/"
        dst = f"{host_path}/"
        typer.echo(f"Syncing: {vm}:{remote_path} -> {host_path}")

    rsync_cmd.extend([src, dst])

    # Run rsync
    result = subprocess.run(rsync_cmd)
    if result.returncode != 0:
        typer.echo("Rsync failed", err=True)
        raise typer.Exit(result.returncode)

    typer.echo("Sync complete.")

# main.py should never by a library anyway,
# so no worries about putting this in the top level
# we generally does not trigger the if __name__ == "__main__"
# block as the entrypoint is `app()`
dotfiles.clean_stale_vms()
if __name__ == "__main__":
    app()
