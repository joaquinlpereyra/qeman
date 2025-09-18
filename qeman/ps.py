import os, time, subprocess, sys
from pathlib import Path
from typing import Optional

IS_LINUX = sys.platform.startswith("linux")
IS_DARWIN = sys.platform == "darwin"

# --- Memory (MB) ---
def rss_mb(pid: int) -> Optional[float]:
    try:
        if IS_LINUX:
            with open(f"/proc/{pid}/statm") as f:
                _, rss_pages, *_ = f.read().split()
            rss_bytes = int(rss_pages) * os.sysconf("SC_PAGE_SIZE")
            return rss_bytes / (1024**2)
        elif IS_DARWIN:
            # ps rss is in KB on macOS
            out = subprocess.check_output(["/bin/ps", "-o", "rss=", "-p", str(pid)], text=True)
            kb = int(out.strip() or "0")
            return kb / 1024.0
        else:
            return None
    except Exception:
        return None

# --- CPU percent ---
def _linux_proc_times(pid: int):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().split()
        utime = int(fields[13]); stime = int(fields[14])
    with open("/proc/stat") as f:
        total = sum(int(x) for x in f.readline().split()[1:])
    return utime + stime, total

def cpu_percent(pid: int) -> Optional[float]:
    try:
        if IS_LINUX:
            internal = 0.1
            t1, tot1 = _linux_proc_times(pid); time.sleep(interval); t2, tot2 = _linux_proc_times(pid)
            if tot2 == tot1: return 0.0
            ncpu = os.cpu_count() or 1
            return 100.0 * (t2 - t1) / (tot2 - tot1) * ncpu
        elif IS_DARWIN:
            # ps %cpu is already a moving average; keep it simple
            out = subprocess.check_output(["/bin/ps", "-o", "%cpu=", "-p", str(pid)], text=True)
            return float(out.strip() or "0.0")
        else:
            return None
    except Exception:
        return None