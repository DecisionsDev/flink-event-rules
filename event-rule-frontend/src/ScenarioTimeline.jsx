import React, { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';

/**
 * ScenarioTimeline
 *
 * Renders a horizontal timeline for a scenario's events.
 * Each event is shown as a coloured circle, evenly spaced in declaration order.
 *
 * A tooltip panel floats next to the hovered circle and shows every
 * key/value pair from the event payload.
 *
 * Props:
 *   events  {Array}  — the flat ordered event list extracted from the scenario
 *                      Each item: { schemaType, topic, payload }
 *                      schemaType — class/schema name (e.g. "AccountEvent")
 */

// ── Colour palette, cycled by distinct event type ────────────────────────────
const TYPE_PALETTE = [
  '#2563eb', // blue
  '#16a34a', // green
  '#b45309', // amber
  '#7c3aed', // purple
  '#db2777', // pink
  '#0891b2', // cyan
  '#ea580c', // orange
  '#65a30d', // lime
];

/**
 * Format a duration in milliseconds into a concise yet friendly string.
 * Examples:
 *   "1d 5h 3min 10s"
 *   "5h 3min 10s"
 *   "3min 10s"
 *   "10s"
 *   "500ms"
 *   "0s"
 */
export function formatDuration(ms) {
  if (typeof ms !== 'number' || isNaN(ms) || ms < 0) return null;
  if (ms === 0) return '0s';

  const totalSeconds = Math.floor(ms / 1000);
  const remainderMs = ms % 1000;

  if (totalSeconds === 0) {
    return `${remainderMs}ms`;
  }

  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}min`);
  if (seconds > 0) parts.push(`${seconds}s`);

  return parts.length > 0 ? parts.join(' ') : '0s';
}

/**
 * Extract a timestamp as epoch milliseconds from an event payload or root event.
 * Supports string ISO dates, numeric timestamps in milliseconds or seconds.
 */
export function extractTimestampMs(event) {
  if (!event) return null;
  const raw = event.payload?.timestamp ?? event.timestamp ?? event.payload?.time ?? event.time ?? event.payload?.dateTime ?? event.dateTime;
  if (raw === undefined || raw === null || raw === '') return null;

  if (typeof raw === 'number') {
    // If raw is in seconds (e.g. Unix timestamp < 1e11), convert to ms
    return raw > 0 && raw < 1e11 ? raw * 1000 : raw;
  }

  if (typeof raw === 'string') {
    const parsed = Date.parse(raw);
    if (!isNaN(parsed)) return parsed;
    const num = Number(raw);
    if (!isNaN(num)) {
      return num > 0 && num < 1e11 ? num * 1000 : num;
    }
  }

  return null;
}

function typeColor(type, colorMap) {
  if (!colorMap.has(type)) {
    colorMap.set(type, TYPE_PALETTE[colorMap.size % TYPE_PALETTE.length]);
  }
  return colorMap.get(type);
}

// ── Tooltip component — rendered into document.body via a portal so it is
//    never clipped by any overflow:hidden ancestor.
function EventTooltip({ event, anchorRect }) {
  const ref = useRef(null);
  const [style, setStyle] = useState({ visibility: 'hidden', position: 'fixed', top: 0, left: 0 });

  useEffect(() => {
    if (!ref.current || !anchorRect) return;
    const tip = ref.current.getBoundingClientRect();
    const vw  = window.innerWidth;
    const vh  = window.innerHeight;
    const GAP = 12;

    // Prefer right of circle; flip left if not enough room
    let left = anchorRect.right + GAP;
    if (left + tip.width > vw - 8) left = anchorRect.left - tip.width - GAP;
    left = Math.max(8, left);

    // Centre vertically on circle; clamp within viewport
    let top = anchorRect.top + anchorRect.height / 2 - tip.height / 2;
    top = Math.min(Math.max(top, 8), vh - tip.height - 8);

    setStyle({ visibility: 'visible', position: 'fixed', top, left });
  }, [anchorRect]);

  const payload    = event.payload || {};
  const payloadOp  = payload.type ? String(payload.type) : null;
  const titleText  = payloadOp && event.schemaType && event.schemaType !== payloadOp
    ? `${payloadOp} (${event.schemaType})`
    : payloadOp || event.schemaType || 'Event';

  return createPortal(
    <div ref={ref} className="timeline-tooltip" style={style}>
      <div className="timeline-tooltip-type">{titleText}</div>
      {event.topic && (
        <div className="timeline-tooltip-topic">{event.topic}</div>
      )}
      <table className="timeline-tooltip-table">
        <tbody>
          {Object.entries(payload).map(([k, v]) => (
            <tr key={k}>
              <td className="timeline-tooltip-key">{k}</td>
              <td className="timeline-tooltip-val">
                {typeof v === 'object' ? JSON.stringify(v) : String(v)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>,
    document.body
  );
}

// ── Main component ────────────────────────────────────────────────────────
/**
 * Props:
 *   events     {Array}   — ordered event list from the scenario
 *   activeStep {number|null} — index of the event currently being processed;
 *                              null when no run is in progress
 */
export default function ScenarioTimeline({ events, activeStep = null }) {
  const containerRef = useRef(null);
  const [hovered, setHovered]   = useState(null); // { index, anchorRect }
  const [width, setWidth]       = useState(600);

  // Observe container width for responsive positioning
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      setWidth(entries[0].contentRect.width || 600);
    });
    ro.observe(el);
    setWidth(el.getBoundingClientRect().width || 600);
    return () => ro.disconnect();
  }, []);

  if (!events || events.length === 0) {
    return (
      <div className="timeline-empty">
        No events in this scenario.
      </div>
    );
  }

  // Assign a stable colour keyed by payload.type, falling back to schemaType then topic
  const colorMap  = new Map();
  // legendMap: colorKey → schemaType (captured on first encounter, for legend labels)
  const legendMap = new Map();
  events.forEach(e => {
    const colorKey = e.payload?.type || e.schemaType || e.topic || 'Event';
    typeColor(colorKey, colorMap);
    if (!legendMap.has(colorKey)) legendMap.set(colorKey, e.schemaType || null);
  });

  // Compute X positions — evenly spread by index order
  const PAD        = 32;  // px from SVG edge to the track end-ticks
  const INNER_PAD  = 40;  // additional px inward before first / after last circle
  const CY    = 40;  // circle centre Y in the SVG
  const R     = 9;   // circle radius
  const SVG_H = 80;  // SVG height

  const n      = events.length;
  const xStart = PAD + INNER_PAD;
  const xEnd   = width - PAD - INNER_PAD;
  const xForIndex = (i) => n === 1 ? (xStart + xEnd) / 2 : xStart + (i / (n - 1)) * (xEnd - xStart);

  // Legend — one entry per distinct payload.type, labelled "TYPE (SchemaType)"
  // Omit the parenthetical when it is identical to the main label.
  const legend = [...colorMap.entries()].map(([colorKey, color]) => {
    const schemaType = legendMap.get(colorKey);
    const label = schemaType && schemaType !== colorKey
      ? `${colorKey} (${schemaType})`
      : colorKey;
    return { colorKey, color, label };
  });

  return (
    <div ref={containerRef} className="timeline-container">
      {/* Legend */}
      <div className="timeline-legend">
        {legend.map(({ colorKey, color, label }) => (
          <span key={colorKey} className="timeline-legend-item">
            <span className="timeline-legend-dot" style={{ background: color }} />
            <span className="timeline-legend-label">{label}</span>
          </span>
        ))}
      </div>

      {/* SVG track */}
      <div className="timeline-svg-wrap" style={{ position: 'relative' }}>
        <svg
          width="100%"
          height={SVG_H}
          className="timeline-svg"
          style={{ overflow: 'visible', display: 'block' }}
        >
          {/* Track line */}
          <line
            x1={PAD} y1={CY} x2={width - PAD} y2={CY}
            stroke="var(--border)" strokeWidth={2}
          />
          {/* End ticks */}
          <line x1={PAD} y1={CY - 8} x2={PAD} y2={CY + 8} stroke="var(--border)" strokeWidth={1.5} />
          <line x1={width - PAD} y1={CY - 8} x2={width - PAD} y2={CY + 8} stroke="var(--border)" strokeWidth={1.5} />

          {/* Duration labels between consecutive events */}
          {events.slice(0, -1).map((evt, i) => {
            const nextEvt = events[i + 1];
            const ts1 = extractTimestampMs(evt);
            const ts2 = extractTimestampMs(nextEvt);
            if (ts1 === null || ts2 === null) return null;

            const diffMs = Math.abs(ts2 - ts1);
            const durationLabel = formatDuration(diffMs);
            if (!durationLabel) return null;

            const x1 = xForIndex(i);
            const x2 = xForIndex(i + 1);
            const midX = (x1 + x2) / 2;

            return (
              <g key={`interval-${i}`} className="timeline-duration-group">
                <text
                  x={midX}
                  y={CY + 18}
                  textAnchor="middle"
                  className="timeline-duration-label"
                  fontSize={10}
                  fill="var(--text-muted)"
                  fontFamily="inherit"
                >
                  {durationLabel}
                </text>
              </g>
            );
          })}

          {/* Event circles */}
          {events.map((evt, i) => {
            const cx      = xForIndex(i);
            const color   = typeColor(evt.payload?.type || evt.schemaType || evt.topic || 'Event', colorMap);
            const isHov   = hovered?.index === i;
            const isActive = activeStep === i;
            return (
              <g key={i}
                className="timeline-event-group"
                onMouseEnter={(e) => {
                  // Find the <circle> element inside this <g> to get its viewport rect
                  const circle = e.currentTarget.querySelector('circle');
                  const anchorRect = circle
                    ? circle.getBoundingClientRect()
                    : e.currentTarget.getBoundingClientRect();
                  setHovered({ index: i, anchorRect });
                }}
                onMouseLeave={() => setHovered(null)}
                style={{ cursor: 'pointer' }}
              >
                {/* Step label above */}
                <text
                  x={cx} y={CY - R - 5}
                  textAnchor="middle"
                  fontSize={9}
                  fill="var(--text-faint)"
                  fontFamily="inherit"
                >
                  {i + 1}
                </text>

                {/* Pulse ring — only rendered while this step is active */}
                {isActive && (
                  <circle
                    cx={cx} cy={CY} r={R}
                    fill="none"
                    stroke={color}
                    strokeWidth={2}
                    opacity={0.6}
                    className="timeline-pulse-ring"
                  />
                )}

                <circle
                  cx={cx} cy={CY} r={isHov ? R + 3 : R}
                  fill={color}
                  stroke={isHov ? 'var(--text)' : color}
                  strokeWidth={isHov ? 2 : 0}
                  style={{ transition: 'r 0.12s, stroke 0.12s' }}
                  className={isActive ? 'timeline-circle--active' : undefined}
                />
              </g>
            );
          })}
        </svg>

        {/* Floating tooltip — portalled to body, never clipped */}
        {hovered !== null && (
          <EventTooltip
            event={events[hovered.index]}
            anchorRect={hovered.anchorRect}
          />
        )}
      </div>
    </div>
  );
}
