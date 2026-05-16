from app.web.wiki import render_markdown


def test_renders_basic_heading_and_paragraph():
    md = "# Hello\n\nSome text."
    html, has_reactflow = render_markdown(md)
    assert "<h1>Hello</h1>" in html
    assert "<p>Some text.</p>" in html
    assert has_reactflow is False


def test_mermaid_fence_becomes_pre_class_mermaid():
    md = "```mermaid\nflowchart LR\nA-->B\n```\n"
    html, _ = render_markdown(md)
    assert '<pre class="mermaid">' in html
    assert "flowchart LR" in html


def test_reactflow_directive_becomes_mount_div():
    md = ':::reactflow id="g1" endpoint="/api/wiki/x.json"\n{"nodes":[]}\n:::\n'
    html, has_reactflow = render_markdown(md)
    assert 'class="reactflow-mount"' in html
    assert 'data-id="g1"' in html
    assert 'data-endpoint="/api/wiki/x.json"' in html
    assert has_reactflow is True


def test_no_reactflow_flag_when_no_directive():
    _, has_reactflow = render_markdown("# nope\n\njust prose")
    assert has_reactflow is False
