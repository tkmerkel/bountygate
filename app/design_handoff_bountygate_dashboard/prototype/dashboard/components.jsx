/* Pixel Augusta primitives — extended for the BountyGate dashboard.
   Globally exported via window. */

const { useState, useEffect, useMemo, useRef } = React;

/* ── WindowCard (SC3K advisor window) ─────────────────────── */
function WindowCard({ title, glyph = "[X]", subtitle, right, children, onGlyph }) {
  return (
    <div className="pa-card">
      <div className="pa-card__title">
        <span>{title}{subtitle && <span style={{
          marginLeft: 10, color: "rgba(255,255,255,.6)", fontSize: 14, letterSpacing: ".08em",
        }}>{subtitle}</span>}</span>
        <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {right}
          <button className="pa-card__glyph" onClick={onGlyph}>{glyph}</button>
        </span>
      </div>
      <div className="pa-card__body">{children}</div>
    </div>
  );
}
function Well({ children, style }) { return <div className="pa-well" style={style}>{children}</div>; }

/* ── Button ───────────────────────────────────────────────── */
function Button({ variant = "default", pressed, disabled, onClick, children, style }) {
  const cls = ["pa-btn", `pa-btn--${variant}`, pressed && "is-pressed", disabled && "is-disabled"]
    .filter(Boolean).join(" ");
  return (
    <button className={cls} onClick={disabled ? undefined : onClick} style={style}>{children}</button>
  );
}

/* ── Ticker (rotates a list of items, marquee animation) ──── */
function Ticker({ items, badge = "LIVE" }) {
  const text = items.join("  \u00b7\u00b7\u00b7  ") + "  \u00b7\u00b7\u00b7  ";
  return (
    <div className="pa-ticker" role="status">
      <span className="pa-ticker__badge">{badge}</span>
      <span className="pa-ticker__fade"></span>
      <div className="pa-ticker__content">{text}{text}</div>
    </div>
  );
}

/* ── Isometric tile field (shared by hero) ────────────────── */
function IsometricField({ height = 280, children }) {
  return (
    <div className="pa-iso" style={{ height, position: "relative" }}>
      <div className="pa-iso__overlay"></div>
      {children}
    </div>
  );
}

/* ── Hedcut avatar (small) ────────────────────────────────── */
function Hedcut({ size = 40 }) {
  return (
    <div className="pa-hedcut__avatar" style={{ width: size, height: size, borderRadius: "50%" }}></div>
  );
}

Object.assign(window, { WindowCard, Well, Button, Ticker, IsometricField, Hedcut });
