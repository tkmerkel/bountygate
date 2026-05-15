"""
Screen recorder for arb executions.

Spawns ffmpeg via gdigrab to record the full desktop while an execution runs.
Designed to fail-silent: if ffmpeg is missing or crashes, the bot continues.
"""

import os
import shutil
import subprocess
import sys
from typing import Optional


def _ffmpeg_path() -> Optional[str]:
    return shutil.which("ffmpeg")


def start_recording(out_path: str, framerate: int = 5, scale_width: int = 1280) -> Optional[subprocess.Popen]:
    """
    Start a full-screen recording to out_path. Returns the Popen handle, or
    None if ffmpeg is unavailable. Never raises.
    """
    ffmpeg = _ffmpeg_path()
    if not ffmpeg:
        print("[rec] ffmpeg not found on PATH - skipping recording", file=sys.stderr)
        return None

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    cmd = [
        ffmpeg,
        "-y",
        "-f", "gdigrab",
        "-framerate", str(framerate),
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
