"""
Chrome / CDP launch helpers.

The launch configuration here was tuned to bypass sportsbook bot detection
and the bot runs on a local Windows machine whose Chrome profile holds the
required plugins. Do not modify the flag list, the flag order, the profile
directory handling, or the subprocess invocation without explicit user
authorization — small changes can re-trigger detection.
"""

import os
import subprocess
import time
import urllib.request


CDP_PORT = 9223
profile_dir = os.path.join(os.getcwd(), "chrome_profile")


def _find_chrome_exe() -> str:
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("Could not find chrome.exe")


def _is_chrome_ready(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def ensure_chrome_cdp(profile_path: str, port: int) -> str:
    """Ensure Chrome is running with remote debugging enabled."""
    endpoint = f"http://127.0.0.1:{port}"
    if _is_chrome_ready(port):
        print(f"✓ Chrome already running on port {port}")
        return endpoint

    print(f"Starting Chrome with CDP on port {port}...")
    chrome_exe = _find_chrome_exe()
    cmd = [
        chrome_exe,
        f"--user-data-dir={profile_path}",
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    subprocess.Popen(cmd)

    for i in range(40):
        if _is_chrome_ready(port):
            print(f"✓ Chrome ready")
            return endpoint
        time.sleep(0.25)

    raise RuntimeError(f"Chrome did not become ready on {endpoint}")
