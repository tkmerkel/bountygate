"""Render wiki .md files into HTML.

Two custom block types:
- Standard ```mermaid fence → <pre class="mermaid">…</pre> (Mermaid CDN renders).
- :::reactflow id="..." endpoint="..." → <div class="reactflow-mount">…JSON…</div>

Returns (html, has_reactflow). Caller uses has_reactflow to decide whether to
include the React Flow bootstrap script tag.
"""
from __future__ import annotations

import re
from typing import Tuple

import markdown
from markdown.extensions import Extension
from markdown.preprocessors import Preprocessor

_REACTFLOW_RE = re.compile(
    r"^:::reactflow\s+([^\n]+)\n(.*?)\n:::\s*$",
    re.MULTILINE | re.DOTALL,
)
_ATTR_RE = re.compile(r'(\w[\w-]*)="([^"]*)"')


class _ReactFlowPreprocessor(Preprocessor):
    """Replace :::reactflow blocks with mount-div HTML before markdown processing."""

    def __init__(self, md):
        super().__init__(md)
        self.found_reactflow = False

    def run(self, lines):
        text = "\n".join(lines)

        def replace(m: "re.Match[str]") -> str:
            self.found_reactflow = True
            attr_str, body = m.group(1), m.group(2).strip()
            attrs = dict(_ATTR_RE.findall(attr_str))
            attr_html = " ".join(f'data-{k}="{v}"' for k, v in attrs.items())
            safe_body = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            return f'<div class="reactflow-mount" {attr_html}>{safe_body}</div>'

        text = _REACTFLOW_RE.sub(replace, text)
        return text.split("\n")


class _ReactFlowExtension(Extension):
    def __init__(self):
        super().__init__()
        self.preprocessor = None

    def extendMarkdown(self, md):
        self.preprocessor = _ReactFlowPreprocessor(md)
        md.preprocessors.register(self.preprocessor, "reactflow_mount", 175)


def _mermaid_fence(src, lang, css_cls, opts, md_inst, **kw):
    return f'<pre class="{css_cls}">{src}</pre>'


def render_markdown(md_text: str) -> Tuple[str, bool]:
    """Render markdown → HTML. Returns (html, has_reactflow_blocks)."""
    rf_ext = _ReactFlowExtension()
    md = markdown.Markdown(
        extensions=[
            "fenced_code",
            "tables",
            "pymdownx.superfences",
            rf_ext,
        ],
        extension_configs={
            "pymdownx.superfences": {
                "custom_fences": [
                    {"name": "mermaid", "class": "mermaid", "format": _mermaid_fence}
                ]
            }
        },
    )
    html = md.convert(md_text)
    return html, (rf_ext.preprocessor.found_reactflow if rf_ext.preprocessor else False)
