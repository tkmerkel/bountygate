import os
import sys

# app/shared/python on sys.path so `import bountygate.transforms...` resolves on host.
_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)
