import threading
import time

import pytest

from human.modals import ModalWatcher


class FakeButton:
    def __init__(self):
        self.clicked = False
        self.visible = True

    def is_visible(self):
        return self.visible

    def click(self):
        self.clicked = True
        # Click also closes the modal.
        self.visible = False


class FakeModal:
    def __init__(self, button):
        self._button = button

    def count(self):
        return 1 if self._button.visible else 0

    def locator(self, sel):
        # Always return our single button locator.
        class Btns:
            def __init__(self, btn):
                self.btn = btn
            def count(self):
                return 1 if self.btn.visible else 0
            @property
            def first(self):
                return self.btn
        return Btns(self._button)

    @property
    def first(self):
        return self

    def is_visible(self):
        return self._button.visible


class FakePage:
    def __init__(self, button):
        self.modal = FakeModal(button)

    def locator(self, sel):
        # All selector variants point at the same modal in this fake.
        return self.modal


def test_watcher_dismisses_a_modal_within_a_few_polls():
    button = FakeButton()
    page = FakePage(button)
    watcher = ModalWatcher(page, poll_range_ms=(50, 80))
    watcher.start()
    try:
        # Give it up to 1s to see and dismiss.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not button.clicked:
            time.sleep(0.05)
        assert button.clicked, "watcher never clicked the modal button"
    finally:
        watcher.stop()


def test_watcher_stops_cleanly_on_context_manager_exit():
    button = FakeButton()
    button.visible = False
    page = FakePage(button)
    with ModalWatcher(page, poll_range_ms=(50, 80)) as watcher:
        time.sleep(0.15)
    # After exit, the thread should have stopped.
    assert not watcher.is_running()
