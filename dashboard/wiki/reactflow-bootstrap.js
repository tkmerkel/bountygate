// dashboard/wiki/reactflow-bootstrap.js
// Loaded only on wiki pages that contain a :::reactflow block.
// Loads React + React Flow from CDN UMD, finds every .reactflow-mount,
// reads its JSON payload + data-endpoint, fetches live metrics, renders.

(async function () {
  const mounts = document.querySelectorAll('.reactflow-mount');
  if (!mounts.length) return;

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }
  function loadStyle(href) {
    const l = document.createElement('link');
    l.rel = 'stylesheet';
    l.href = href;
    document.head.appendChild(l);
  }

  try {
    await loadScript('https://unpkg.com/react@18/umd/react.production.min.js');
    await loadScript('https://unpkg.com/react-dom@18/umd/react-dom.production.min.js');
    await loadScript('https://unpkg.com/reactflow@11/dist/umd/index.js');
    loadStyle('https://unpkg.com/reactflow@11/dist/style.css');
  } catch (e) {
    console.error('Failed loading React/ReactFlow CDN bundles', e);
    mounts.forEach(m => {
      m.innerHTML = '<p style="color:#ff6e6e;">Could not load React Flow bundles from CDN.</p>';
    });
    return;
  }

  // ReactFlow UMD attaches as window.ReactFlow with named exports.
  const React = window.React;
  const ReactDOM = window.ReactDOM;
  const RF = window.ReactFlow;
  const ReactFlowComponent = RF.ReactFlow || RF.default;
  const Background = RF.Background;
  const Controls = RF.Controls;

  for (const mount of mounts) {
    let spec;
    try {
      spec = JSON.parse(mount.textContent.trim());
    } catch (e) {
      console.error('reactflow JSON parse failed', e, mount.textContent);
      mount.innerHTML = '<p style="color:#ff6e6e;">Invalid React Flow spec JSON.</p>';
      continue;
    }
    mount.textContent = '';

    const endpoint = mount.dataset.endpoint;
    let metrics = { node_metrics: {}, edge_metrics: {} };
    if (endpoint) {
      try {
        const r = await fetch(endpoint);
        if (r.ok) metrics = await r.json();
      } catch (e) {
        console.warn('metrics fetch failed', endpoint, e);
      }
    }

    // Decorate node labels with live metrics.
    const baseNodes = (spec.nodes || []).map(n => {
      const m = metrics.node_metrics[n.id] || {};
      let label = (n.data && n.data.label) || n.id;
      if (m.runs_24h !== undefined) {
        const dur = (m.avg_duration_s != null) ? ` · ${m.avg_duration_s.toFixed(1)}s` : '';
        label = `${label}\n${m.runs_24h}/24h${dur}`;
      }
      return {
        ...n,
        data: { ...(n.data || {}), label },
        style: {
          background: '#1f2330',
          color: '#e7ebf3',
          border: '1px solid #2a2f3c',
          padding: 8,
          fontSize: 12,
          ...(n.style || {}),
        },
      };
    });

    const baseEdges = (spec.edges || []).map(e => ({
      ...e,
      animated: e.layer === 'decisions' ? false : !!e.animated,
      style: { stroke: e.layer === 'decisions' ? '#a78bfa' : '#4a5568', ...(e.style || {}) },
      labelStyle: { fill: '#a78bfa', fontSize: 10 },
    }));

    const layers = spec.layers || [];
    const defaultActive = new Set(layers.filter(l => l.default).map(l => l.id));

    function App() {
      const [active, setActive] = React.useState(defaultActive);
      const toggle = (lid) => {
        const next = new Set(active);
        if (next.has(lid)) next.delete(lid); else next.add(lid);
        setActive(next);
      };
      const visibleNodes = baseNodes.filter(n => !n.layer || active.has(n.layer));
      const visibleEdges = baseEdges.filter(e =>
        (!e.layer || active.has(e.layer)) &&
        visibleNodes.some(n => n.id === e.source) &&
        visibleNodes.some(n => n.id === e.target)
      );

      return React.createElement('div', {
        style: { display: 'grid', gridTemplateColumns: '1fr 200px', gap: 12, minHeight: 460 }
      },
        React.createElement('div', { style: { height: 460, background: '#0f1115', borderRadius: 4 } },
          React.createElement(ReactFlowComponent, {
            nodes: visibleNodes,
            edges: visibleEdges,
            fitView: true,
            nodesDraggable: false,
            nodesConnectable: false,
            proOptions: { hideAttribution: true },
          },
            Background && React.createElement(Background, { color: '#2a2f3c', gap: 16 }),
            Controls && React.createElement(Controls, { showInteractive: false })
          )
        ),
        React.createElement('aside', { style: { fontSize: 12, color: '#e7ebf3' } },
          React.createElement('h4', {
            style: { color: '#a78bfa', margin: '0 0 8px', textTransform: 'uppercase', fontSize: 10, letterSpacing: '0.06em' }
          }, 'Layers'),
          ...layers.map(l => React.createElement('label', {
            key: l.id,
            style: { display: 'block', padding: '5px 0', cursor: 'pointer' }
          },
            React.createElement('input', {
              type: 'checkbox',
              checked: active.has(l.id),
              onChange: () => toggle(l.id),
              style: { marginRight: 6 }
            }),
            React.createElement('span', { style: { color: l.color || '#e7ebf3' } }, l.label)
          )),
          React.createElement('div', {
            style: { marginTop: 14, paddingTop: 10, borderTop: '1px solid #2a2f3c', fontSize: 11, color: '#8b95a8' }
          }, 'Hover a node for live metrics.')
        )
      );
    }

    const root = ReactDOM.createRoot(mount);
    root.render(React.createElement(App));
  }
})();
