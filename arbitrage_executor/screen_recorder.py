"""
Screen recorder for arb executions.

Spawns ffmpeg via gdigrab while an execution runs. Captures the screen rect
occupied by the bot's CDP-attached Chrome window (located by HWND lookup) so
secondary monitors don't leak in. Falls back to full virtual-desktop capture
if the bot's Chrome can't be located.

Fail-silent: if ffmpeg is missing or the recording can't start, the bot continues.

Why HWND-targeted region capture (vs full `-i desktop`):
    - `-i desktop` grabs the entire virtual desktop bounding rect (all
      monitors). Secondary-monitor windows leak in.
    - Targeting the bot Chrome's on-screen rect (via HWND -> GetWindowRect ->
      `-offset_x/-offset_y/-video_size`) eliminates the multi-monitor leak.
    - We do NOT use gdigrab's `-i title=` window mode: it BitBlts from the
      window's device context, and Chrome's GPU/DirectComposition rendering
      bypasses the window DC, producing black frames.

Known limitation:
    Region capture reads screen pixels, so windows occluding the bot Chrome
    (e.g. an OS popup on top) will appear in the recording instead. Proper
    occlusion-safe capture for GPU-composited Chrome requires the
    Windows.Graphics.Capture WinRT API — a larger refactor. For now: keep
    other windows off the bot Chrome's rect during runs.
"""

import ctypes
import os
import shutil
import subprocess
import sys
from ctypes import wintypes
from typing import Optional, Tuple

try:
    import psutil
except ImportError:
    psutil = None


CDP_PORT = "9223"
_HAS_WIN32 = sys.platform == "win32"


def _ffmpeg_path() -> Optional[str]:
    return shutil.which("ffmpeg")


def _find_bot_chrome_hwnd() -> Optional[int]:
    """Locate the HWND of the bot's Chrome browser window (the process launched
    with --remote-debugging-port=9223 and no --type= renderer/utility flag).
    Returns None if not on Windows, psutil missing, or no match."""
    if not _HAS_WIN32 or psutil is None:
        return None

    target_pids: set[int] = set()
    port_flag = f"--remote-debugging-port={CDP_PORT}"
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            name = (proc.info["name"] or "").lower()
            if "chrome" not in name:
                continue
            cmdline = proc.info["cmdline"] or []
            if not any(port_flag in arg for arg in cmdline):
                continue
            # Skip renderer / utility / GPU subprocesses — only the browser
            # process owns the top-level HWND. Browser process has no --type= arg.
            if any(arg.startswith("--type=") for arg in cmdline):
                continue
            target_pids.add(proc.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not target_pids:
        return None

    user32 = ctypes.windll.user32
    found: list[int] = []

    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def _enum(hwnd, _):
        if not user32.IsWindowVisible(hwnd):
            return True
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if pid.value not in target_pids:
            return True
        # Only consider top-level windows with a non-empty title — skips
        # Chrome's hidden helper windows.
        if user32.GetWindowTextLengthW(hwnd) == 0:
            return True
        found.append(int(hwnd))
        return False  # stop on first hit

    user32.EnumWindows(_enum, 0)
    return found[0] if found else None


_DWMWA_EXTENDED_FRAME_BOUNDS = 9


def _get_window_rect(hwnd: int) -> Optional[Tuple[int, int, int, int]]:
    """Return (x, y, width, height) of the window's visible screen rect.

    Uses DwmGetWindowAttribute(DWMWA_EXTENDED_FRAME_BOUNDS) — GetWindowRect
    includes ~8px of invisible drop-shadow padding on modern Win10/11 windows,
    which causes gdigrab to error 'capture area extends outside window area'.
    """
    if not _HAS_WIN32 or not hwnd:
        return None
    try:
        rect = wintypes.RECT()
        hr = ctypes.windll.dwmapi.DwmGetWindowAttribute(
            wintypes.HWND(hwnd),
            wintypes.DWORD(_DWMWA_EXTENDED_FRAME_BOUNDS),
            ctypes.byref(rect),
            ctypes.sizeof(rect),
        )
        if hr != 0:
            # Fall back to GetWindowRect (older Windows, or DWM unavailable).
            if not ctypes.windll.user32.GetWindowRect(hwnd, ctypes.byref(rect)):
                return None
        w = rect.right - rect.left
        h = rect.bottom - rect.top
        if w <= 0 or h <= 0:
            return None
        # gdigrab requires even dimensions for libx264 yuv420p.
        w -= w % 2
        h -= h % 2
        return rect.left, rect.top, w, h
    except Exception:
        return None


def start_recording(out_path: str, framerate: int = 5, scale_width: int = 1280) -> Optional[subprocess.Popen]:
    """
    Start a recording to out_path. Targets the bot Chrome's on-screen rect
    (HWND -> GetWindowRect -> gdigrab offset/size). Falls back to full
    virtual-desktop capture if the HWND can't be located. Returns the Popen
    handle, or None if ffmpeg is unavailable. Never raises.
    """
    ffmpeg = _ffmpeg_path()
    if not ffmpeg:
        print("[rec] ffmpeg not found on PATH - skipping recording", file=sys.stderr)
        return None

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    region_args: list[str] = []
    hwnd = _find_bot_chrome_hwnd()
    rect = _get_window_rect(hwnd) if hwnd else None
    if rect:
        x, y, w, h = rect
        region_args = [
            "-offset_x", str(x),
            "-offset_y", str(y),
            "-video_size", f"{w}x{h}",
        ]
        print(f"  [rec] Targeting bot Chrome HWND={hwnd} rect=({x},{y}) {w}x{h}")
    else:
        print("  [rec] Bot Chrome HWND not found; capturing full virtual desktop", file=sys.stderr)

    cmd = [
        ffmpeg,
        "-y",
        "-f", "gdigrab",
        "-framerate", str(framerate),
        *region_args,
        "-i", "desktop",
        "-vf", f"scale={scale_width}:-2",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "28",
        "-pix_fmt", "yuv420p",
        "-loglevel", "error",
        out_path,
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"  [rec] Recording -> {out_path}")
        return proc
    except Exception as e:
        print(f"[rec] Failed to start ffmpeg recording: {e}", file=sys.stderr)
        return None


def stop_recording(proc: Optional[subprocess.Popen], timeout: float = 5.0) -> None:
    """
    Stop a recording started by start_recording. Sends 'q' on stdin for a
    clean MP4 trailer, falls back to terminate/kill. No-op if proc is None.
    """
    if proc is None:
        return

    if proc.poll() is not None:
        return

    try:
        if proc.stdin and not proc.stdin.closed:
            try:
                proc.stdin.write(b"q\n")
                proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
            try:
                proc.stdin.close()
            except OSError:
                pass

        try:
            proc.wait(timeout=timeout)
            print(f"  [rec] Recording saved")
            return
        except subprocess.TimeoutExpired:
            pass

        proc.terminate()
        try:
            proc.wait(timeout=2.0)
            print(f"  [rec] Recording stopped (terminate)")
            return
        except subprocess.TimeoutExpired:
            pass

        proc.kill()
        proc.wait(timeout=2.0)
        print(f"  [rec] Recording killed", file=sys.stderr)
    except Exception as e:
        print(f"[rec] Error stopping recording: {e}", file=sys.stderr)
