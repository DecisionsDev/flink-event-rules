import React, { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { formatTooltipValue, extractTimestampMs, formatDuration } from './ScenarioTimeline';

/**
 * LiveExecutionTimeline
 *
 * Displays a real-time horizontal timeline of Kafka events as they arrive.
 *
 * • Input events  → filled circle   (●)  colour keyed by event type
 * • Output events → upward triangle (▲)  colour keyed by event type
 *
 * Changelog / agent-error topics are always excluded.
 * Events are ordered left-to-right by timestamp.
 * Duration between consecutive events is shown below the track.
 * The legend labels use the event type (payload.type or schemaType), not the topic name.
 * The hover tooltip matches the ScenarioTimeline tooltip exactly.
 *
 * Props:
 *   eventsByType  {Object}  — map of topicName → event array (from useEventStream)
 *   descriptor    {Object}  — use-case descriptor with .topics and .outboundTopics
 *   maxEvents     {number}  — max recent events to display (default 60)
 */

// ── Colour palette (same as ScenarioTimeline) ─────────────────────────────
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

// Topic types that are always excluded from the live timeline
const EXCLUDED_TYPES = new Set(['entity-changelog', 'agent-error']);

function typeColor(type, colorMap) {
  if (!colorMap.has(type)) {
    colorMap.set(type, TYPE_PALETTE[colorMap.size % TYPE_PALETTE.length]);
  }
  return colorMap.get(type);
}

// ── Tooltip — identical structure to ScenarioTimeline's EventTooltip ──────
function EventTooltip({ event, anchorRect }) {
  const ref = useRef(null);
  const [style, setStyle] = useState({ visibility: 'hidden', position: 'fixed', top: 0, left: 0 });

  useEffect(() => {
    if (!ref.current || !anchorRect) return;
    const tip = ref.current.getBoundingClientRect();
    const vw  = window.innerWidth;
    const vh  = window.innerHeight;
    const GAP = 12;

    let left = anchorRect.right + GAP;
    if (left + tip.width > vw - 8) left = anchorRect.left - tip.width - GAP;
    left = Math.max(8, left);

    let top = anchorRect.top + anchorRect.height / 2 - tip.height / 2;
    top = Math.max(8, Math.min(top, vh - tip.height - 8));

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
              <td className="timeline-tooltip-val">{formatTooltipValue(v)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>,
    document.body
  );
}

// ── Triangle path helper ──────────────────────────────────────────────────
// Returns SVG polygon points for an upward-pointing triangle centred at (cx, cy).
function trianglePoints(cx, cy, size) {
  const h     = size * 1.6;
  const top   = cy - h * 0.6;
  const bLeft = cx - size;
  const bRight = cx + size;
  const base  = cy + h * 0.4;
  return `${cx},${top} ${bLeft},${base} ${bRight},${base}`;
}

// ── Main component ────────────────────────────────────────────────────────
export default function LiveExecutionTimeline({ eventsByType, descriptor, maxEvents = 60 }) {
  const containerRef = useRef(null);
  const [hovered, setHovered] = useState(null); // { index, anchorRect }
  const [width, setWidth]     = useState(600);

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

  // ── Build excluded topic name set (changelog + agent-error) ──────────────
  const excludedTopicNames = new Set(
    (descriptor?.outboundTopics || [])
      .filter(ot => EXCLUDED_TYPES.has(ot.type))
      .map(ot => ot.name)
  );

  // ── Build input/output topic sets from descriptor ─────────────────────
  const inputNames  = new Set((descriptor?.topics || []).map(t => t.name));
  const outputNames = new Set(
    (descriptor?.outboundTopics || [])
      .filter(ot => !EXCLUDED_TYPES.has(ot.type))
      .map(ot => ot.name)
  );

  // ── Flatten all events, tagging each with its role ────────────────────
  const allEvents = [];
  for (const [topicName, events] of Object.entries(eventsByType || {})) {
    // Skip excluded system topics
    if (excludedTopicNames.has(topicName)) continue;

    const isInput  = inputNames.has(topicName);
    const isOutput = outputNames.has(topicName);
    if (!isInput && !isOutput) continue;

    // Shared topic (input & output): treat as input for visualisation
    const role = isOutput && !isInput ? 'output' : 'input';

    for (let i = 0; i < events.length; i++) {
      const ev = events[i];
      // Normalise to { payload } shape so colorKey (payload.type), legend labels,
      // and tooltip title all resolve the same way as ScenarioTimeline events.
      //
      // Two raw shapes come out of the BFF eventStore:
      //   • Inbound events: the raw Kafka value IS the payload (flat object at root,
      //     e.g. { type: "OPENING", accountNumber, timestamp, _seq }).
      //     The BFF only publishes event.payload to Kafka so there is no wrapping key.
      //   • Outbound EventEnvelope events written by Flink: already have a `.payload`
      //     key ({ agentEventType, entityId, timestamp, payload: { ... }, _seq }).
      //
      // Detect the envelope shape by the presence of agentEventType, and wrap only
      // inbound (non-envelope) events.
      const isEnvelope = Boolean(ev.agentEventType);
      const { _seq, ...rest } = ev;
      const normalised = isEnvelope
        ? { ...rest }                         // already has .payload
        : { payload: rest };                  // wrap flat payload
      allEvents.push({
        ...normalised,
        topic: topicName,
        role,
        _topicIdx: i,
        _seq,
      });
    }
  }

  // ── Order left-to-right by timestamp ────────────────────────────────────
  // extractTimestampMs handles all timestamp field variations (payload.timestamp,
  // root .timestamp, .time, .dateTime, seconds vs ms).
  // When timestamps are absent or identical, fall back to topic-array index.
  allEvents.sort((a, b) => {
    const tsA = extractTimestampMs(a);
    const tsB = extractTimestampMs(b);
    if (tsA !== null && tsB !== null) return tsA - tsB;
    if (tsA !== null) return -1;
    if (tsB !== null) return 1;
    return a._topicIdx - b._topicIdx;
  });

  // Keep only the most recent maxEvents
  const displayed = allEvents.slice(-maxEvents);

  // ── Build colour map keyed by event type (payload.type → schemaType → topic) ──
  const colorMap  = new Map();
  // legendMap: colorKey → { role, schemaType } — mirrors ScenarioTimeline's legendMap
  const legendMap = new Map();
  displayed.forEach(e => {
    const colorKey = e.payload?.type || e.schemaType || e.topic || 'Event';
    typeColor(colorKey, colorMap);
    if (!legendMap.has(colorKey)) legendMap.set(colorKey, { role: e.role, schemaType: e.schemaType || null });
  });

  // ── SVG layout constants ────────────────────────────────────────────────
  const PAD       = 32;
  const INNER_PAD = 40;
  const CY        = 38;
  const R         = 8;
  const SVG_H     = 80; // extra height for duration labels below track

  const n       = displayed.length;
  const xStart  = PAD + INNER_PAD;
  const xEnd    = width - PAD - INNER_PAD;
  const xForIdx = (i) => n <= 1 ? (xStart + xEnd) / 2 : xStart + (i / (n - 1)) * (xEnd - xStart);

  // ── Legend: one entry per distinct event type ────────────────────────
  // Label mirrors ScenarioTimeline: "TYPE (SchemaType)" when they differ, else just "TYPE".
  const legend = [...colorMap.entries()].map(([colorKey, color]) => {
    const { role, schemaType } = legendMap.get(colorKey) || {};
    const label = schemaType && schemaType !== colorKey
      ? `${colorKey} (${schemaType})`
      : colorKey;
    return { colorKey, color, label, role };
  });

  const isEmpty = displayed.length === 0;

  return (
    <div className="live-execution-timeline-panel">
      {/* Panel header */}
      <div className="live-execution-timeline-header">
        <span className="live-execution-timeline-title">Execution Timeline</span>
        <span className="live-execution-timeline-count">
          {allEvents.length} event{allEvents.length !== 1 ? 's' : ''}
        </span>
      </div>

      <div ref={containerRef} className="timeline-container">
        {/* Legend */}
        {!isEmpty && (
          <div className="timeline-legend">
            {legend.map(({ colorKey, color, label, role }) => (
              <span key={colorKey} className="timeline-legend-item">
                {role === 'output' ? (
                  /* Triangle for output — no CSS equivalent, use inline SVG */
                  <svg width="12" height="12" viewBox="0 0 12 12" style={{ flexShrink: 0 }}>
                    <polygon points="6,1 0,11 12,11" fill={color} />
                  </svg>
                ) : (
                  /* Dot for input — matches the ScenarioTimeline legend dot */
                  <span className="timeline-legend-dot" style={{ background: color }} />
                )}
                <span className="timeline-legend-label">{label}</span>
              </span>
            ))}
          </div>
        )}

        {/* SVG track */}
        {isEmpty ? (
          <div className="timeline-empty">
            No events received yet — waiting for Kafka messages…
          </div>
        ) : (
          <div className="timeline-svg-wrap" style={{ position: 'relative' }}>
            <svg
              width="100%"
              height={SVG_H}
              className="timeline-svg"
              style={{ overflow: 'visible', display: 'block' }}
            >
              {/* Track line */}
              <line x1={PAD} y1={CY} x2={width - PAD} y2={CY} stroke="var(--border)" strokeWidth={2} />
              {/* End ticks */}
              <line x1={PAD} y1={CY - 8} x2={PAD} y2={CY + 8} stroke="var(--border)" strokeWidth={1.5} />
              <line x1={width - PAD} y1={CY - 8} x2={width - PAD} y2={CY + 8} stroke="var(--border)" strokeWidth={1.5} />

              {/* Duration labels between consecutive events */}
              {displayed.slice(0, -1).map((evt, i) => {
                const nextEvt = displayed[i + 1];
                const ts1 = extractTimestampMs(evt);
                const ts2 = extractTimestampMs(nextEvt);
                if (ts1 === null || ts2 === null) return null;

                const diffMs = Math.abs(ts2 - ts1);
                const label  = formatDuration(diffMs);
                if (!label) return null;

                const x1   = xForIdx(i);
                const x2   = xForIdx(i + 1);
                const midX = (x1 + x2) / 2;

                return (
                  <text
                    key={`dur-${i}`}
                    x={midX}
                    y={CY + 18}
                    textAnchor="middle"
                    className="timeline-duration-label"
                    fontSize={10}
                    fill="var(--text-muted)"
                    fontFamily="inherit"
                  >
                    {label}
                  </text>
                );
              })}

              {/* Events */}
              {displayed.map((evt, i) => {
                const cx       = xForIdx(i);
                const colorKey = evt.payload?.type || evt.schemaType || evt.topic || 'Event';
                const color    = typeColor(colorKey, colorMap);
                const isHov    = hovered?.index === i;

                return (
                  <g
                    key={i}
                    className="timeline-event-group"
                    onMouseEnter={(e) => {
                      const shape = e.currentTarget.querySelector('circle, polygon');
                      const anchorRect = shape
                        ? shape.getBoundingClientRect()
                        : e.currentTarget.getBoundingClientRect();
                      setHovered({ index: i, anchorRect });
                    }}
                    onMouseLeave={() => setHovered(null)}
                    style={{ cursor: 'pointer' }}
                  >
                    {/* Step number above shape */}
                    <text
                      x={cx} y={CY - R - 5}
                      textAnchor="middle"
                      fontSize={9}
                      fill="var(--text-faint)"
                      fontFamily="inherit"
                    >
                      {i + 1}
                    </text>

                    {evt.role === 'output' ? (
                      <polygon
                        points={trianglePoints(cx, CY, isHov ? R + 3 : R)}
                        fill={color}
                        stroke={isHov ? 'var(--text)' : color}
                        strokeWidth={isHov ? 1.5 : 0}
                        style={{ transition: 'stroke 0.12s' }}
                      />
                    ) : (
                      <circle
                        cx={cx} cy={CY} r={isHov ? R + 3 : R}
                        fill={color}
                        stroke={isHov ? 'var(--text)' : color}
                        strokeWidth={isHov ? 2 : 0}
                        style={{ transition: 'r 0.12s, stroke 0.12s' }}
                      />
                    )}
                  </g>
                );
              })}
            </svg>

            {hovered !== null && (
              <EventTooltip
                event={displayed[hovered.index]}
                anchorRect={hovered.anchorRect}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}
