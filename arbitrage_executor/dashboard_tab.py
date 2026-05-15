"""
Dashboard tab manager.

Ensures the local review dashboard is open as a tab in the bot's Chrome
window. The bot opens this tab once via CDP; existing Chrome launch flags
in chrome_helpers.py are not modified (frozen for bot-detection bypass).

User pins the tab manually once -> stays leftmost across Chrome restarts.
"""

import os


def _dashboard_url() -> str:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = os.path.join(repo_root, "dashboard", "index.html")
    return "file:///" + path.replace("\\", "/")


def ensure_dashboard_tab(browser) -> None:
    """
    Open the dashboard tab in the existing CDP-controlled Chrome context if
    not already present. Best-effort: never raises.
    """
    target_url = _dashboard_url()
    target_path = target_url.rsplit("/", 1)[-1]

    try:
        context = browser.contexts[0] if browser.contexts else None
        if context is None:
            return

        for page in context.pages:
            url = (page.url or "").lower()
            if url.startswith("file:///") and url.endswith(target_path):
                return

        dash = context.new_page()
        dash.goto(target_url, wait_until="domcontentloaded", timeout=5000)
        print(f"  [dash] Dashboard tab opened")
    except Exception as e:
        print(f"  [dash] Could not ensure dashboard tab: {e}")
