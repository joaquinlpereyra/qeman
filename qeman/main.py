import urllib
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
import socket
import platform
from typing import IO, Literal
from qeman import dotfiles
from qeman import logs
from qeman import ps

app = typer.Typer(help="Unified QEMU CLI tool")
snap_app = typer.Typer(help="Manage internal qcow2 snapshots")
list_app = typer.Typer(help="List state, like images and running VMs")
app.add_typer(snap_app, name="snap")
app.add_typer(list_app, name="list")

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
    base_image: Annotated[str, typer.Argument(help="Base image to fork", autocompletion=complete_image_names,)],
    new_image: str):
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
def snap_list(image: str):
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
def snap_create(image: str, name: str):
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
def snap_apply(image: str, name: str):
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
def snap_delete(image: str, name: str):
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
    image_name: str,
    iso: Path):
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
        "--display", "gtk"
    ]

    if IS_GOOD_OS:
        cmd += ["--enable-kvm", "-cpu", "host"]

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
                # Ignore our `.ssh` configuration file
                "-F", "/dev/null",
                "-i", str(key_path), "j@localhost"
                ]
    return ssh_cmd

@app.command()
def connect(vm: str = Argument(..., autocompletion=running_vm_names)):
    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"No VM named '{vm}' running.", err=True)
        raise typer.Exit(1)
    port = info["ssh_port"]
    ssh_cmd = ssh_command(port)
    ssh_cmd += [
        "-oStrictHostKeyChecking=no",
        "-oUserKnownHostsFile=/dev/null",
    ]
    # sysargvs[0] must be the exec name again,
    # so we repeat here
    os.execvp("ssh", ssh_cmd)

@app.command()
def run(
    image: Annotated[str, Argument(autocompletion=complete_image_names, help="Image to run")],
    mount: Optional[Path] = None,
    graphical: bool = False,
    post: Optional[Path] = None):
    image_path = dotfiles.get_image(image)
    meta = dotfiles.get_metadata(image_path)
    if meta.get("dependents"):
        typer.echo(f"Image '{image_path.name}' has dependent forks. Running it directly may corrupt data.", err=True)
        raise typer.Exit(code=1)

    monitor_path = dotfiles.get_monitor(image)
    validate_qcow2_format(image_path)
    ssh_port = dotfiles.get_next_ssh_port()

    cmd = [
        dotfiles.get_binary("qemu_system"), "-m", "12G", "-smp", "4",
        "-drive", f"file={image_path},format=qcow2,if=virtio", "-boot", "c",
        "-netdev", f"user,id=net0,hostfwd=tcp::{ssh_port}-:22",
        "-device", "virtio-net-pci,netdev=net0",
        "-device", "virtio-serial", "-device", "virtio-balloon",
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
            cmd += ["--display", "gtk",
                    "-chardev", "spicevmc,id=vdagent,name=vdagent",
                    "-device", "virtserialport,chardev=vdagent,name=com.redhat.spice.0"]
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
def kill(vms: Annotated[List[str], typer.Argument(autocompletion=running_vm_names)]):
    def send_qmp_shutdown(monitor_path: Path):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.connect(str(monitor_path))
                s.settimeout(2)

                s.sendall(b'{"execute":"qmp_capabilities"}\n')
                time.sleep(0.1)
                _ = s.recv(4096)

                s.sendall(b'{"execute":"system_powerdown"}\n')
                time.sleep(0.1)
        except Exception as e:
            typer.echo(f"QMP command failed: {e}", err=True)
            raise typer.Exit(code=1)

    running = dotfiles.get_running_vms()
    for vm in vms:
        if vm not in running:
            typer.echo(f"No running VM registered under name '{vm}'", err=True)
            raise typer.Exit(code=1)

        monitor_path = dotfiles.get_monitor(vm)
        if not monitor_path.exists():
            typer.echo(f"Monitor socket not found for VM '{vm}'", err=True)
            raise typer.Exit(code=1)

        send_qmp_shutdown(monitor_path)

    # There was some logic to remove the monitor manually here
    # BUt qemu appears to remove it automatically on shutdown.

@app.command()
def info(image: str):
    image_path = dotfiles.get_image(image)
    result = subprocess.run([dotfiles.get_binary("qemu_img"), "info", str(image_path)], capture_output=True, text=True)
    if result.returncode != 0:
        typer.echo("Failed to retrieve image info.", err=True)
        raise typer.Exit(code=1)
    typer.echo(result.stdout)

@app.command()
def version():
    typer.echo("qeman v0.6.0")

@list_app.command("images")
def list_cmd_images():
    out = []
    for img in sorted(dotfiles.get_images()):
        meta = dotfiles.get_metadata(img)
        # include name + all metadata keys
        entry = {"name": img.name, **meta}
        out.append(entry)
    typer.echo(json.dumps(out, indent=2))


@list_app.command("vms")
def list_cmd_vms():
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
def code(vm: str):
    """
    SSH into the VM, run `code tunnel`, grab the device link (and code), open browser.
    Leaves the remote tunnel running.
    """


    def _first_url_in(line: str) -> str | None:
        for tok in line.split():
            if tok.startswith(("http://", "https://")):
                return tok
        return None

    def _is_allowed(url: str) -> bool:
        ALLOWED_HOSTS = {"github.com", "vscode.dev"}
        try:
            u = urllib.parse.urlparse(url)
            return u.scheme == "https" and u.netloc in ALLOWED_HOSTS
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
    ssh_cmd = ssh_command(port) + ["code", "tunnel"]

    proc = subprocess.Popen(
        ssh_cmd,
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
                if not _is_allowed(url):
                    typer.echo(f"Refusing to open untrusted URL: {url}", err=True)
                    continue  # keep scanning; maybe a valid URL comes next

                link = url
                if not opened:
                    if not tunnel_exists and code:
                        typer.echo(f"Device code: {code}")
                    time.sleep(0.5)  # tiny debounce, sometimes it hangs without it
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

# main.py should never by a library anyway,
# so no worries about putting this in the top level
# we generally does not trigger the if __name__ == "__main__"
# block as the entrypoint is `app()`
dotfiles.clean_stale_vms()
if __name__ == "__main__":
    app()
