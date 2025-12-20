import socket
import time
import json
import typer
from pathlib import Path

def _qmp_send(sock: socket.socket, obj: dict):
    sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))

def _qmp_recv(sock: socket.socket, timeout=2.0) -> dict:
    sock.settimeout(timeout)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
        if b"\n" in data:
            line, _, _ = data.rpartition(b"\n")
            try:
                return json.loads(line.decode("utf-8"))
            except Exception:
                try:
                    return json.loads(data.decode("utf-8"))
                except Exception:
                    pass
    return {}

def exec(monitor_path: Path, execute: str, arguments: dict | None = None) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(monitor_path))
        _ = _qmp_recv(s)  # banner
        _qmp_send(s, {"execute": "qmp_capabilities"})
        _ = _qmp_recv(s)
        payload = {"execute": execute}
        if arguments:
            payload["arguments"] = arguments
        _qmp_send(s, payload)
        return _qmp_recv(s)

def hmp(monitor_path: Path, command_line: str) -> str:
    resp = exec(monitor_path, "human-monitor-command", {"command-line": command_line})
    if "error" in resp:
        raise RuntimeError(resp["error"])
    return resp.get("return", "")

def send_shutdown(monitor_path: Path):
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