/*
 * Form widgets of the Agent Diagram Editor detail panel that edit the structured parts of an
 * agents.json: references, enrichment, SQL enrichment and state configuration.
 *
 * They are controlled components with no knowledge of the diagram; styles come from
 * AgentDiagramEditor.css.
 */
import React, { useId, useRef, useState } from 'react';

// Sub-folder of samples/<application>/ holding the reference CSV files.
export const REFERENCE_DIR = 'enrichment';
export const REFERENCE_TYPES = ['csv'];
export const CARDINALITIES = ['', 'one', 'many'];
export const STATE_BACKENDS = ['', 'rocksdb', 'heap'];

const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/** Label + hint + control, in the editor's field layout. */
export function Field({ label, hint, children }) {
  return (
    <div className="ade-field-group">
      <label className="ade-field-label">{label}</label>
      {hint && <div className="ade-field-hint">{hint}</div>}
      {children}
    </div>
  );
}

/** Text input with an optional datalist of suggestions. */
export function TextInput({ value, onChange, placeholder, options, ...rest }) {
  const listId = useId();
  return (
    <>
      <input
        className="ade-field-input"
        type="text"
        value={value ?? ''}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        spellCheck={false}
        autoComplete="off"
        list={options ? listId : undefined}
        {...rest}
      />
      {options && (
        <datalist id={listId}>
          {options.map(o => <option key={o} value={o} />)}
        </datalist>
      )}
    </>
  );
}

export function TextField({ label, hint, ...inputProps }) {
  return (
    <Field label={label} hint={hint}>
      <TextInput {...inputProps} />
    </Field>
  );
}

/** Input that only commits on blur / Enter; used where a keystroke-level change would be destructive. */
export function CommitInput({ value, onCommit, ...rest }) {
  const [draft, setDraft] = useState(null);
  const commit = () => {
    if (draft !== null && draft !== value) onCommit(draft);
    setDraft(null);
  };
  return (
    <input
      className="ade-field-input"
      type="text"
      spellCheck={false}
      value={draft ?? value}
      onChange={e => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={e => {
        if (e.key === 'Enter') { e.preventDefault(); commit(); }
        if (e.key === 'Escape') setDraft(null);
      }}
      {...rest}
    />
  );
}

// ─── References (agents[*].references[*]) ───────────────────────────────────

/**
 * A file picked from disk is kept in `pendingFile` (and `file` gets its future path) until the
 * diagram is saved; the editor then uploads it to <application>/enrichment.
 */
export function ReferencesEditor({ references = [], onChange }) {
  const update = (i, patch) => onChange(references.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  const inputRefs = useRef({});
  const pick = (i, file) => {
    if (!file || !file.name.toLowerCase().endsWith('.csv')) return;
    update(i, {
      file: `${REFERENCE_DIR}/${file.name}`,
      pendingFile: file,
      ...(references[i].name ? {} : { name: file.name.replace(/\.csv$/i, '') }),
    });
  };
  return (
    <>
      <div className="ade-field-section-title">Reference data</div>
      <div className="ade-field-hint">
        CSV files that enrichments look rows up in. The first line holds the column names.
        A file picked from disk is stored in the {REFERENCE_DIR} sub-folder of the application folder when the diagram is saved.
      </div>
      {references.map((r, i) => (
        <div className="ade-pair-row ade-ref-row" key={i}>
          <input
            className="ade-field-input"
            type="text"
            aria-label={`Reference ${i + 1} name`}
            value={r.name || ''}
            placeholder="Name, e.g. flights"
            spellCheck={false}
            onChange={e => update(i, { name: e.target.value })}
          />
          <input
            className="ade-field-input"
            type="text"
            aria-label={`Reference ${i + 1} file`}
            value={r.file || ''}
            placeholder="samples/app/flights.csv"
            spellCheck={false}
            onChange={e => update(i, { file: e.target.value, pendingFile: undefined })}
          />
          <input
            ref={el => { inputRefs.current[i] = el; }}
            type="file"
            accept=".csv,text/csv"
            aria-label={`Reference ${i + 1} file picker`}
            style={{ display: 'none' }}
            onChange={e => { pick(i, e.target.files?.[0]); e.target.value = ''; }}
          />
          <button
            className="ade-pair-add ade-ref-browse"
            title="Pick a CSV file from your disk"
            aria-label={`Browse reference ${i + 1} file`}
            onClick={() => inputRefs.current[i]?.click()}
          >
            Browse
          </button>
          <button
            className="ade-pair-remove"
            title="Remove reference"
            aria-label={`Remove reference ${i + 1}`}
            onClick={() => onChange(references.filter((_, j) => j !== i))}
          >
            ✕
          </button>
        </div>
      ))}
      <button className="ade-pair-add" onClick={() => onChange([...references, { name: '', type: 'csv', file: '' }])}>
        + Add reference
      </button>
    </>
  );
}

// ─── Enrichment (subscriptions[*].enrichment / outputs[*].enrichment) ──

/** ["a", { "b": "c" }]  <->  "a, b=c" */
export function formatEnrichmentFields(fields) {
  return (Array.isArray(fields) ? fields : [])
    .map(f => {
      if (typeof f === 'string') return f;
      if (isObj(f)) {
        const [source, target] = Object.entries(f)[0] || [];
        return source ? `${source}=${target}` : '';
      }
      return '';
    })
    .filter(Boolean)
    .join(', ');
}

export function parseEnrichmentFields(textValue) {
  return String(textValue || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(s => {
      const eq = s.indexOf('=');
      if (eq < 0) return s;
      const source = s.slice(0, eq).trim();
      const target = s.slice(eq + 1).trim();
      if (!source) return null;
      return target && target !== source ? { [source]: target } : source;
    })
    .filter(Boolean);
}

/** Comma separated list of reference columns; "column=newName" renames the written property. */
function EnrichmentFieldsInput({ fields, onChange }) {
  const [draft, setDraft] = useState(() => formatEnrichmentFields(fields));
  return (
    <input
      className="ade-field-input"
      type="text"
      aria-label="Enrichment fields"
      value={draft}
      placeholder="name, loyaltyStatus, checkInStatus=check-inStatus"
      spellCheck={false}
      onChange={e => { setDraft(e.target.value); onChange(parseEnrichmentFields(e.target.value)); }}
    />
  );
}

const EMPTY_STEP = { reference: '', key: '', on: '', when: '', into: '', fields: [] };

export function EnrichmentEditor({ steps = [], onChange, referenceNames = [], title = 'CSV enrichment', hint }) {
  const update = (i, patch) => onChange(steps.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  return (
    <>
      <div className="ade-field-section-title">{title}</div>
      {hint && <div className="ade-field-hint">{hint}</div>}
      {steps.map((s, i) => (
        <div className="ade-card" key={i}>
          <div className="ade-card-header">
            <span>Enrichment {i + 1}</span>
            <button
              className="ade-pair-remove"
              title="Remove enrichment"
              aria-label={`Remove enrichment ${i + 1}`}
              onClick={() => onChange(steps.filter((_, j) => j !== i))}
            >
              ✕
            </button>
          </div>
          <Field
            label="Reference"
            hint={referenceNames.length === 0 ? 'No reference registered. Add one in the Reference data section of the Agent.' : 'Reference data registered in the Agent.'}
          >
            <select
              className="ade-field-input"
              aria-label={`Enrichment ${i + 1} reference`}
              value={s.reference || ''}
              onChange={e => update(i, { reference: e.target.value })}
            >
              <option value="">(select a reference)</option>
              {referenceNames.map(n => <option key={n} value={n}>{n}</option>)}
              {s.reference && !referenceNames.includes(s.reference) && (
                <option value={s.reference}>{s.reference} (not registered)</option>
              )}
            </select>
          </Field>
          <TextField
            label="Key column"
            hint="Column of the reference matched against the looked up value."
            value={s.key}
            placeholder="e.g. passengerId"
            onChange={v => update(i, { key: v })}
          />
          <TextField
            label="On"
            hint="JSONPath of the value to look up in the event."
            value={s.on}
            placeholder="e.g. $.passengerId"
            onChange={v => update(i, { on: v })}
          />
          <TextField
            label="Into"
            hint="Where the fields are written: $, $.a.b or $.a.b[*]."
            value={s.into}
            placeholder="e.g. $.flight.passengers[*]"
            onChange={v => update(i, { into: v })}
          />
          <TextField
            label="When (optional)"
            hint="JSONPath filter; the enrichment applies when it selects something."
            value={s.when}
            placeholder="e.g. $[?(@.type == 'FLIGHTOPEN')]"
            onChange={v => update(i, { when: v })}
          />
          <Field label="Cardinality (optional)" hint="Default: many when the reference holds several rows per key, else one.">
            <select
              className="ade-field-input"
              value={s.cardinality || ''}
              onChange={e => update(i, { cardinality: e.target.value })}
            >
              {CARDINALITIES.map(c => <option key={c} value={c}>{c || '(default)'}</option>)}
            </select>
          </Field>
          <Field label="Fields" hint="Reference columns to copy, comma separated. column=newName renames.">
            <EnrichmentFieldsInput fields={s.fields} onChange={fields => update(i, { fields })} />
          </Field>
        </div>
      ))}
      <button className="ade-pair-add" onClick={() => onChange([...steps, { ...EMPTY_STEP }])}>
        + Add enrichment
      </button>
    </>
  );
}

// ─── SQL enrichment (subscriptions[*] / outputs[*]: schema + sqlEnrichment) ──

/**
 * `data` holds { schema, sqlEnrichment }; `onChange` receives the changed properties.
 * Table API alternative to the csv enrichment: the DataStream engine applies it only.
 */
export function SqlEnrichmentFields({ data = {}, onChange, subject = 'events' }) {
  const se = isObj(data.sqlEnrichment) ? data.sqlEnrichment : {};
  const setSql = (patch) => onChange({ sqlEnrichment: { ...se, ...patch } });
  return (
    <>
      <div className="ade-field-section-title">SQL enrichment</div>
      <div className="ade-field-hint">
        {`Flink SQL script to enrich the ${subject}. Applied by the DataStream engine only.`}
      </div>
      <TextField
        label="Payload schema"
        hint={`JSON Schema of the ${subject}; required when an SQL file is set.`}
        value={data.schema}
        placeholder="e.g. samples/my-app/schemas/event.schema.json"
        onChange={v => onChange({ schema: v })}
      />
      <TextField
        label="SQL file"
        hint="DDL and enrichment view. Leave blank for no SQL enrichment."
        value={se.sqlFile}
        placeholder="e.g. samples/my-app/enrichment/enrich.sql"
        onChange={v => setSql({ sqlFile: v })}
      />
      {se.sqlFile ? (
        <>
          <TextField
            label="Input view"
            hint="Default: raw_events."
            value={se.inputViewName}
            placeholder="raw_events"
            onChange={v => setSql({ inputViewName: v })}
          />
          <TextField
            label="Output view"
            hint="Default: enriched_events."
            value={se.outputViewName}
            placeholder="enriched_events"
            onChange={v => setSql({ outputViewName: v })}
          />
        </>
      ) : null}
    </>
  );
}

// ─── Errors configuration (agents[*].outputs[eventType="agent-error"]) ───────

export function ErrorsEditor({ value = {}, onChange }) {
  const set = (key, v) => onChange({ ...value, eventType: 'agent-error', [key]: v });
  return (
    <>
      <div className="ade-field-section-title">Errors</div>
      <div className="ade-field-hint">
        Configuration for agent processing errors emitted when event execution fails.
      </div>
      <Field label="Event Type" hint="Fixed event type for agent processing errors.">
        <input
          className="ade-field-input"
          type="text"
          value="agent-error"
          readOnly
        />
      </Field>
      <TextField
        label="Kafka Topic Name"
        hint="Kafka topic that receives agent processing errors."
        value={value.topic || ''}
        placeholder="e.g. agent-banking-errors"
        onChange={v => set('topic', v)}
      />
      <TextField
        label="Label (optional)"
        hint="Human-readable label for the error output."
        value={value.label || ''}
        placeholder="e.g. Processing errors for investigation"
        onChange={v => set('label', v)}
      />
      <Field label="Description (optional)">
        <textarea
          className="ade-field-input ade-field-textarea"
          rows={2}
          value={value.description || ''}
          onChange={e => set('description', e.target.value)}
          placeholder="e.g. Details of errors encountered during execution"
        />
      </Field>
    </>
  );
}

// ─── State configuration (agents[*].stateConfig, minus changeLogTopic) ───────

export function StateConfigEditor({ value = {}, onChange }) {
  const set = (key, v) => onChange({ ...value, [key]: v });
  return (
    <>
      <div className="ade-field-section-title">Flink State</div>
      <TextField
        label="State TTL"
        hint="How long idle entity state is kept. Units: s, mn, h, d, m. Blank = forever."
        value={value.ttl}
        placeholder="e.g. 30d"
        onChange={v => set('ttl', v)}
      />
      <Field label="State backend" hint="RocksDB persists to disk; heap keeps state in memory.">
        <select className="ade-field-input" value={value.backend || ''} onChange={e => set('backend', e.target.value)}>
          {STATE_BACKENDS.map(b => <option key={b} value={b}>{b || '(job default)'}</option>)}
        </select>
      </Field>
      <TextField
        label="Checkpoint interval (optional)"
        hint="Hint only; the job's global checkpoint interval takes precedence."
        value={value.checkpointInterval}
        placeholder="e.g. 5m"
        onChange={v => set('checkpointInterval', v)}
      />
    </>
  );
}
