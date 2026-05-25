from human.modals import ModalWatcher, check_all_active, _active_watcher_count


class FakeButton:
    def __init__(self, name: str = ""):
        self.clicked = False
        self.visible = True
        self.name = name

    def is_visible(self):
        return self.visible

    def click(self):
        self.clicked = True
        # Click also closes the modal.
        self.visible = False


class _Locator:
    """Simple count/first/get_by_role locator stub."""
    def __init__(self, items):
        self._items = list(items)

    def count(self):
        return len([i for i in self._items if getattr(i, "visible", True)])

    @property
    def first(self):
        for i in self._items:
            if getattr(i, "visible", True):
                return i
        return self._items[0] if self._items else None

    def get_by_role(self, role, name=None):
        if role != "button":
            return _Locator([])
        matched = [b for b in self._items
                   if getattr(b, "name", "") == name and getattr(b, "visible", True)]
        return _Locator(matched)


class FakeModal:
    """Modal with a list of buttons and optional inputs / iframes.

    .locator("input") returns the modal's inputs (empty list = no
    credential form). .locator("iframe") returns the modal's iframes
    (non-empty marks a wrapped login form like FD's). .locator("button")
    returns the modal's buttons. .get_by_role("button", name=...) on
    .first returns name-matched.
    """
    def __init__(self, buttons, inputs=None, iframes=None):
        self._buttons = buttons
        self._inputs = inputs or []
        self._iframes = iframes or []

    def count(self):
        return 1 if any(b.visible for b in self._buttons) else 0

    def locator(self, sel):
        if sel == "input":
            return _Locator(self._inputs)
        if sel == "iframe":
            return _Locator(self._iframes)
        # Default — return buttons (test fake collapses other sels).
        return _Locator(self._buttons)

    def get_by_role(self, role, name=None):
        if role != "button":
            return _Locator([])
        matched = [b for b in self._buttons
                   if b.name == name and b.visible]
        return _Locator(matched)

    @property
    def first(self):
        return self

    def is_visible(self):
        return any(b.visible for b in self._buttons)


class FakePage:
    def __init__(self, buttons=None, inputs=None, iframes=None, button=None):
        # ``button`` arg kept for back-compat with older callers.
        if button is not None:
            buttons = [button]
        self.modal = FakeModal(buttons or [], inputs or [], iframes or [])

    def locator(self, sel):
        # All selector variants point at the same modal in this fake.
        return self.modal


def test_check_once_dismisses_a_visible_modal_with_sole_button():
    button = FakeButton()  # no name — falls to sole-button path
    page = FakePage(buttons=[button])
    watcher = ModalWatcher(page)
    assert watcher.check_once() is True
    assert button.clicked


def test_check_once_prefers_named_dismiss_button():
    # Modal with 2 buttons: one with a known dismiss name, one without.
    # Should pick the named one (sole-button fallback won't fire when
    # count > 1).
    safe = FakeButton(name="I Understand")
    other = FakeButton(name="Maybe Later")
    page = FakePage(buttons=[other, safe])  # safe is NOT first
    watcher = ModalWatcher(page)
    assert watcher.check_once() is True
    assert safe.clicked
    assert not other.clicked


def test_check_once_skips_modal_with_input_field():
    # Credential / form modal — has an input. Must not dismiss.
    button = FakeButton(name="OK")
    page = FakePage(buttons=[button], inputs=[FakeButton()])
    watcher = ModalWatcher(page)
    assert watcher.check_once() is False
    assert not button.clicked


def test_check_once_skips_modal_wrapping_an_iframe():
    # FD login modal — outer wrapper has 0 inputs (they're inside the
    # iframe, invisible to outer locators) and 1 sole button (the
    # wrapper's × close). Must not dismiss; clicking × detaches the
    # login frame. Regression: sweep #9 (2026-05-25).
    close = FakeButton(name="")
    page = FakePage(buttons=[close], iframes=[FakeButton()])
    watcher = ModalWatcher(page)
    assert watcher.check_once() is False
    assert not close.clicked


def test_check_once_returns_false_when_multiple_buttons_and_no_safe_name():
    # Modal with multiple buttons but none in the safe-name list and
    # count > 1 so sole-button fallback won't fire either.
    a = FakeButton(name="Cancel")
    b = FakeButton(name="Submit")
    page = FakePage(buttons=[a, b])
    watcher = ModalWatcher(page)
    assert watcher.check_once() is False
    assert not a.clicked
    assert not b.clicked


def test_check_once_returns_false_when_no_modal():
    button = FakeButton()
    button.visible = False
    page = FakePage(buttons=[button])
    watcher = ModalWatcher(page)
    assert watcher.check_once() is False
    assert not button.clicked


def test_start_registers_in_active_set_and_stop_unregisters():
    button = FakeButton()
    page = FakePage(buttons=[button])
    watcher = ModalWatcher(page)
    before = _active_watcher_count()
    watcher.start()
    assert _active_watcher_count() == before + 1
    assert watcher.is_running()
    watcher.stop()
    assert _active_watcher_count() == before
    assert not watcher.is_running()


def test_context_manager_register_and_unregister():
    button = FakeButton()
    page = FakePage(buttons=[button])
    before = _active_watcher_count()
    with ModalWatcher(page) as watcher:
        assert _active_watcher_count() == before + 1
        assert watcher.is_running()
    assert _active_watcher_count() == before
    assert not watcher.is_running()


def test_check_all_active_dismisses_visible_modals_on_registered_watchers():
    button = FakeButton()
    page = FakePage(buttons=[button])
    with ModalWatcher(page):
        dismissed = check_all_active()
        assert dismissed == 1
        assert button.clicked


def test_check_all_active_skips_unregistered_watchers():
    button = FakeButton()
    page = FakePage(buttons=[button])
    # Construct but do not start — should not be checked.
    watcher = ModalWatcher(page)
    assert not watcher.is_running()
    dismissed = check_all_active()
    assert dismissed == 0
    assert not button.clicked


def test_check_once_never_raises_when_page_explodes():
    class BoomPage:
        def locator(self, sel):
            raise RuntimeError("CDP disconnected")
    watcher = ModalWatcher(BoomPage())
    # Must not raise.
    assert watcher.check_once() is False


def test_start_is_idempotent():
    button = FakeButton()
    page = FakePage(buttons=[button])
    before = _active_watcher_count()
    watcher = ModalWatcher(page)
    watcher.start()
    watcher.start()
    assert _active_watcher_count() == before + 1
    watcher.stop()
