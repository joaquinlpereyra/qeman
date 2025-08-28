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
import psutil
import socket
import platform
from typing import IO, Literal
from qeman import dotfiles
from qeman import logs

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
            # security_model=mapped maps uid,gid,model are mapped
            # i.e.: qemu will try to translate permissions so that
            # host/vm agree (i.e.: you will be able to write to files
            # you own in the host from the vm)
            "-fsdev", f"local,id=fsdev0,path={mount},security_model=mapped",
            "-device", "virtio-9p-pci,fsdev=fsdev0,mount_tag=quarantine"
        ]

    if graphical:
        cmd += ["--display", "gtk",
                "-chardev", "spicevmc,id=vdagent,name=vdagent",
                "-device", "virtserialport,chardev=vdagent,name=com.redhat.spice.0"]
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
        try:
            p = psutil.Process(pid)
            mem_rss = p.memory_info().rss
            cpu_pct = p.cpu_percent(interval=0.1)
        except psutil.NoSuchProcess:
            mem_rss = None
            cpu_pct = None

        out.append({
            "name":       name,
            "pid":        pid,
            "ssh_port":   info.get("ssh_port"),
            "memory_rss": f"{(mem_rss or 0) / (1024**2):.1f}mb" if mem_rss is not None else "-",
            "cpu_percent": cpu_pct if cpu_pct is not None else "-" ,
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
    SSH into the VM, run `code tunnel`, extract the login code, validate it,
    and open the browser.
    """

    running = dotfiles.get_running_vms()
    info = running.get(vm)
    if not info:
        typer.echo(f"VM '{vm}' is not running.", err=True)
        raise typer.Exit(1)

    port = info["ssh_port"]
    key_path = Path(dotfiles.get_ssh_config().get("key_path"))
    key_path = str(key_path.expanduser())

    ssh_cmd = ssh_command(port) + ["code tunnel"]

    proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    done = threading.Event()

    def monitor():
        tunnel_exists = False
        for line in iter(proc.stdout.readline, ''):
            line = line.strip()
            print(line)
            words = line.split()

            if not tunnel_exists:
                tunnel_exists = line.startswith("Connected to an existing tunnel")

            if not tunnel_exists and len(words) >= 5 and words[-5] == "https://github.com/login/device":
                url, code = words[-5], words[-1]
                if len(code) == 9 and code[4] == '-' and code.replace('-', '').isalnum() and code.isupper():
                    open_browser(url)
                    print(f"Code for login in {code}. Browser opened.")
                    done.set()
                    return

            if len(words) == 7 and line.startswith('Open this link in your browser'):
                open_browser(words[-1])
                done.set()
                return


    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    if done.wait(timeout=5):
        return
    else:
        typer.echo(f"VM '{vm}' tunnel timeout.")


# main.py should never by a library anyway,
# so no worries about putting this in the top level
# we generally does not trigger the if __name__ == "__main__"
# block as the entrypoint is `app()`
dotfiles.clean_stale_vms()
if __name__ == "__main__":
    app()
