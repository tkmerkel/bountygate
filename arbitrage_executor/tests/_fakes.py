"""Playwright-surface test doubles used by bet_placer tests.

The fakes implement only the slice of the Playwright API that BetPlacer
calls. Grow this surface incrementally as new tests need new methods —
do NOT mock methods speculatively.
"""


class FakeElement:
    def __init__(self, *, visible=True, text="", on_click=None, attributes=None,
                 input_value="", on_fill=None, on_type=None, on_press=None):
        self.visible = visible
        self.text = text
        self.on_click = on_click
        self.clicked = False
        self.attributes = attributes or {}
        self._input_value = input_value
        self.fills = []
        self.types = []
        self.presses = []
        self.on_fill = on_fill
        self.on_type = on_type
        self.on_press = on_press

    def is_visible(self):
        return self.visible

    def click(self, *args, **kwargs):
        self.clicked = True
        if self.on_click:
            self.on_click()

    def text_content(self):
        return self.text

    def get_attribute(self, name):
        return self.attributes.get(name)

    def input_value(self):
        return self._input_value

    def fill(self, value):
        self.fills.append(value)
        self._input_value = value
        if self.on_fill:
            self.on_fill(value)

    def type(self, value, **kwargs):
        self.types.append(value)
        self._input_value += value
        if self.on_type:
            self.on_type(value)

    def press(self, key):
        self.presses.append(key)
        if self.on_press:
            self.on_press(key)

    def evaluate(self, *args, **kwargs):
        return ""

    def wait_for(self, **kwargs):
        return None


class FakeLocator:
    def __init__(self, elements=None):
        self.elements = list(elements or [])

    @property
    def first(self):
        return self.elements[0] if self.elements else FakeElement(visible=False)

    def count(self):
        return len(self.elements)

    def nth(self, index):
        if index < len(self.elements):
            return self.elements[index]
        return FakeElement(visible=False)

    def is_visible(self):
        return self.first.is_visible()

    def click(self, *args, **kwargs):
        return self.first.click(*args, **kwargs)

    def text_content(self):
        return self.first.text_content()

    def all(self):
        return list(self.elements)

    def get_attribute(self, name):
        return self.first.get_attribute(name)

    def evaluate(self, *args, **kwargs):
        return []

    def wait_for(self, **kwargs):
        return None


class FakeKeyboard:
    def __init__(self):
        self.presses = []
        self.types = []

    def press(self, key):
        self.presses.append(key)

    def type(self, value, **kwargs):
        self.types.append(value)


class FakePage:
    def __init__(self, *, locators=None, text_locators=None, role_locators=None,
                 label_locators=None, url=""):
        self.locators = locators or {}
        self.text_locators = text_locators or {}
        self.role_locators = role_locators or {}
        self.label_locators = label_locators or {}
        self.waits = []
        self.navigations = []
        self.viewport_sizes = []
        self.evaluations = []
        self.keyboard = FakeKeyboard()
        self.url = url

    def locator(self, selector):
        return self.locators.get(selector, FakeLocator())

    def get_by_text(self, text, exact=False):
        if hasattr(text, "pattern"):
            return self.text_locators.get(text.pattern, FakeLocator())
        return self.text_locators.get(text, FakeLocator())

    def get_by_role(self, role, name=None):
        if name is not None and hasattr(name, "pattern"):
            key = (role, name.pattern)
        else:
            key = (role, name)
        return self.role_locators.get(key, FakeLocator())

    def get_by_label(self, label):
        if label is not None and hasattr(label, "pattern"):
            key = label.pattern
        else:
            key = label
        return self.label_locators.get(key, FakeLocator())

    def wait_for_timeout(self, ms):
        self.waits.append(ms)

    def wait_for_selector(self, selector, **kwargs):
        return None

    def screenshot(self, *args, **kwargs):
        return None

    def goto(self, url, **kwargs):
        self.navigations.append(url)
        self.url = url

    def set_viewport_size(self, size):
        self.viewport_sizes.append(size)

    def evaluate(self, script):
        self.evaluations.append(script)
        return None
