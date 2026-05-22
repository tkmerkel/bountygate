from human.modals import ModalWatcher, check_all_active, _active_watcher_count


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


def test_check_once_dismisses_a_visible_modal():
    button = FakeButton()
    page = FakePage(button)
    watcher = ModalWatcher(page)
    assert watcher.check_once() is True
    assert button.clicked


def test_check_once_returns_false_when_no_modal():
    button = FakeButton()
    button.visible = False
    page = FakePage(button)
    watcher = ModalWatcher(page)
    assert watcher.check_once() is False
    assert not button.clicked


def test_start_registers_in_active_set_and_stop_unregisters():
    button = FakeButton()
    page = FakePage(button)
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
    page = FakePage(button)
    before = _active_watcher_count()
    with ModalWatcher(page) as watcher:
        assert _active_watcher_count() == before + 1
        assert watcher.is_running()
    assert _active_watcher_count() == before
    assert not watcher.is_running()


def test_check_all_active_dismisses_visible_modals_on_registered_watchers():
    button = FakeButton()
    page = FakePage(button)
    with ModalWatcher(page):
        dismissed = check_all_active()
        assert dismissed == 1
        assert button.clicked


def test_check_all_active_skips_unregistered_watchers():
    button = FakeButton()
    page = FakePage(button)
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
    page = FakePage(button)
    before = _active_watcher_count()
    watcher = ModalWatcher(page)
    watcher.start()
    watcher.start()
    assert _active_watcher_count() == before + 1
    watcher.stop()
