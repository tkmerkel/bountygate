import os
import sys

# repo root on path so `import app.web...` resolves (app/ is a PEP 420 namespace pkg)
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
