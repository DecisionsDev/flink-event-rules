/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

/**
 * sqlGraph — turns the statements of a PTF deployment into a graph model.
 *
 * Input is the `statements` array served by GET /api/agents/:id/sql:
 *   [{ kind, agentId, sql }]
 * Output (buildSqlGraph) is a laid-out data-flow graph:
 *   Kafka source tables  ->  input views  ->  Process Table Function views  ->  Kafka sinks
 * where every node and edge keeps the index of the statement it was derived from, so the UI
 * can always show (and jump to) the exact SQL behind it.
 *
 * Accuracy rules:
 *  - The statements are parsed with a real tokenizer and a small recursive-descent parser for
 *    the grammar the PTF pipeline builder emits (CREATE TABLE ... WITH, CREATE VIEW over
 *    SELECT / UNION ALL / a table-function call, INSERT INTO ... SELECT ... WHERE).
 *  - Anything outside that grammar is NOT approximated: the statement is reported in
 *    `unsupported` with the reason and gets no node.  Callers show the raw SQL for it.
 *  - Descriptions only restate what the parsed SQL contains; free-form expressions are
 *    quoted verbatim, never paraphrased.
 *
 * This module is pure (no React, no DOM) so it can be unit-tested against real generator output.
 */

export class SqlParseError extends Error {}

/** Upper bound on statements the graphical view will try to lay out. */
export const MAX_GRAPH_STATEMENTS = 400;

// ─── Tokenizer ──────────────────────────────────────────────────────────────────

const TWO_CHAR_PUNCT = new Set(['=>', '<>', '<=', '>=', '!=', '||']);

/**
 * Splits SQL into tokens { type: 'word'|'ident'|'string'|'number'|'punct', text, start, end }.
 * Whitespace and comments are dropped.  Throws SqlParseError on unterminated quotes.
 */
export function tokenizeSql(sql) {
  const tokens = [];
  const n = sql.length;
  let i = 0;
  while (i < n) {
    const c = sql[i];
    if (/\s/.test(c)) { i += 1; continue; }
    if (c === '-' && sql[i + 1] === '-') {
      while (i < n && sql[i] !== '\n') i += 1;
      continue;
    }
    if (c === '/' && sql[i + 1] === '*') {
      const close = sql.indexOf('*/', i + 2);
      if (close < 0) throw new SqlParseError('unterminated comment');
      i = close + 2;
      continue;
    }
    if (c === "'" || c === '`') {
      const start = i;
      i += 1;
      for (;;) {
        if (i >= n) throw new SqlParseError(`unterminated ${c === "'" ? 'string' : 'identifier'}`);
        if (sql[i] === c) {
          if (sql[i + 1] === c) { i += 2; continue; } // doubled quote = escaped quote
          i += 1;
          break;
        }
        i += 1;
      }
      tokens.push({ type: c === "'" ? 'string' : 'ident', text: sql.slice(start, i), start, end: i });
      continue;
    }
    if (/\d/.test(c)) {
      const m = /^\d+(?:\.\d+)?/.exec(sql.slice(i));
      tokens.push({ type: 'number', text: m[0], start: i, end: i + m[0].length });
      i += m[0].length;
      continue;
    }
    if (/[A-Za-z_$]/.test(c)) {
      const m = /^[A-Za-z_$][A-Za-z0-9_$]*/.exec(sql.slice(i));
      tokens.push({ type: 'word', text: m[0], start: i, end: i + m[0].length });
      i += m[0].length;
      continue;
    }
    const two = sql.slice(i, i + 2);
    if (TWO_CHAR_PUNCT.has(two)) {
      tokens.push({ type: 'punct', text: two, start: i, end: i + 2 });
      i += 2;
      continue;
    }
    tokens.push({ type: 'punct', text: c, start: i, end: i + 1 });
    i += 1;
  }
  return tokens;
}

/** Decoded value of a string / identifier / word token. */
function valueOf(t) {
  if (t.type === 'string') return t.text.slice(1, -1).replace(/''/g, "'");
  if (t.type === 'ident')  return t.text.slice(1, -1).replace(/``/g, '`');
  return t.text;
}

// ─── Parser ─────────────────────────────────────────────────────────────────────

/**
 * Parses one statement.
 * @returns {{ok: true, model: object} | {ok: false, reason: string}}
 */
export function parseStatement(sql) {
  try {
    return { ok: true, model: new Parser(sql).statement() };
  } catch (err) {
    if (err instanceof SqlParseError) return { ok: false, reason: err.message };
    // A bug in the parser must degrade to "not drawn, see the raw SQL", never to a crash.
    return { ok: false, reason: 'the statement could not be analysed' };
  }
}

class Parser {
  constructor(sql) {
    this.src = sql;
    this.ts = tokenizeSql(sql);
  }

  // ── cursor helpers ──
  isWord(i, w)  { const t = this.ts[i]; return !!t && t.type === 'word' && t.text.toUpperCase() === w; }
  isPunct(i, p) { const t = this.ts[i]; return !!t && t.type === 'punct' && t.text === p; }
  isName(i)     { const t = this.ts[i]; return !!t && (t.type === 'ident' || t.type === 'word'); }
  describe(i)   { return this.ts[i] ? `'${this.ts[i].text}'` : 'end of statement'; }

  expectWord(i, w) {
    if (!this.isWord(i, w)) throw new SqlParseError(`expected ${w} but found ${this.describe(i)}`);
    return i + 1;
  }
  expectPunct(i, p) {
    if (!this.isPunct(i, p)) throw new SqlParseError(`expected '${p}' but found ${this.describe(i)}`);
    return i + 1;
  }
  expectName(i) {
    if (!this.isName(i)) throw new SqlParseError(`expected a name but found ${this.describe(i)}`);
    return i + 1;
  }
  /** Decoded name at i; throws SqlParseError if there is none. */
  nameAt(i) {
    this.expectName(i);
    return valueOf(this.ts[i]);
  }
  /** Decoded string literal at i; throws SqlParseError if there is none. */
  stringAt(i) {
    this.expectString(i);
    return valueOf(this.ts[i]);
  }
  expectString(i) {
    if (!this.ts[i] || this.ts[i].type !== 'string') throw new SqlParseError(`expected a string but found ${this.describe(i)}`);
    return i + 1;
  }
  expectEnd(i) {
    if (i < this.ts.length && !(this.isPunct(i, ';') && i === this.ts.length - 1)) {
      throw new SqlParseError(`unsupported syntax near ${this.describe(i)}`);
    }
  }

  slice(a, b) { return a >= b ? '' : this.src.slice(this.ts[a].start, this.ts[b - 1].end); }

  /** Index of the ')' matching the '(' at i. */
  matching(i) {
    let depth = 0;
    for (let k = i; k < this.ts.length; k += 1) {
      if (this.isPunct(k, '(')) depth += 1;
      else if (this.isPunct(k, ')')) { depth -= 1; if (depth === 0) return k; }
    }
    throw new SqlParseError("unbalanced '('");
  }

  /** Splits tokens [a, b) at top-level commas into [start, end) ranges. */
  splitCommas(a, b) {
    const parts = [];
    let depth = 0;
    let from = a;
    for (let k = a; k < b; k += 1) {
      if (this.isPunct(k, '(')) depth += 1;
      else if (this.isPunct(k, ')')) depth -= 1;
      else if (depth === 0 && this.isPunct(k, ',')) { parts.push([from, k]); from = k + 1; }
    }
    if (from < b || parts.length > 0) parts.push([from, b]);
    return parts.filter(([s, e]) => e > s);
  }

  // ── statements ──
  statement() {
    if (this.ts.length === 0) throw new SqlParseError('empty statement');
    let i = 0;
    if (this.isWord(i, 'INSERT')) return this.insert(i);
    i = this.expectWord(i, 'CREATE');
    if (this.isWord(i, 'TEMPORARY')) i += 1;
    if (this.isWord(i, 'SYSTEM')) i += 1;
    if (this.isWord(i, 'TABLE'))    return this.createTable(i + 1);
    if (this.isWord(i, 'VIEW'))     return this.createView(i + 1);
    if (this.isWord(i, 'FUNCTION')) return this.createFunction(i + 1);
    throw new SqlParseError(`unsupported statement CREATE ${this.ts[i] ? this.ts[i].text : ''}`.trim());
  }

  createFunction(i) {
    const name = this.nameAt(i); i += 1;
    i = this.expectWord(i, 'AS');
    const className = this.stringAt(i); i += 1;
    if (this.isWord(i, 'LANGUAGE')) i += 2;
    let jar = null;
    if (this.isWord(i, 'USING')) {
      i = this.expectWord(i + 1, 'JAR');
      jar = this.stringAt(i); i += 1;
    }
    this.expectEnd(i);
    return { type: 'function', name, className, jar };
  }

  createTable(i) {
    const name = this.nameAt(i); i += 1;
    const open = i;
    i = this.expectPunct(i, '(');
    const close = this.matching(open);
    const columns = [];
    let watermark = null;
    for (const [s, e] of this.splitCommas(i, close)) {
      if (this.isWord(s, 'WATERMARK')) {
        let k = this.expectWord(s, 'WATERMARK');
        k = this.expectWord(k, 'FOR');
        const column = this.nameAt(k); k += 1;
        k = this.expectWord(k, 'AS');
        watermark = { column, expr: this.slice(k, e) };
        continue;
      }
      if (this.isWord(s, 'PRIMARY') || this.isWord(s, 'CONSTRAINT')) {
        throw new SqlParseError(`unsupported table constraint '${this.ts[s].text}'`);
      }
      let k = s;
      const colName = this.nameAt(k); k += 1;
      if (this.isWord(k, 'AS')) {
        columns.push({ name: colName, kind: 'computed', expr: this.slice(k + 1, e) });
        continue;
      }
      let metaAt = -1;
      for (let m = k; m < e; m += 1) if (this.isWord(m, 'METADATA')) { metaAt = m; break; }
      const typeEnd = metaAt >= 0 ? metaAt : e;
      if (typeEnd <= k) throw new SqlParseError(`column ${colName} has no type`);
      const column = { name: colName, kind: 'physical', dataType: this.slice(k, typeEnd) };
      if (metaAt >= 0) {
        column.kind = 'metadata';
        const f = this.expectWord(metaAt + 1, 'FROM');
        column.metadataKey = this.stringAt(f);
      }
      columns.push(column);
    }
    i = close + 1;
    const options = {};
    if (this.isWord(i, 'WITH')) {
      const wOpen = i + 1;
      this.expectPunct(wOpen, '(');
      const wClose = this.matching(wOpen);
      for (const [s, e] of this.splitCommas(wOpen + 1, wClose)) {
        let k = this.expectString(s);
        k = this.expectPunct(k, '=');
        if (e !== k + 1 || this.ts[k].type !== 'string') throw new SqlParseError('unsupported table option value');
        options[valueOf(this.ts[s])] = valueOf(this.ts[k]);
      }
      i = wClose + 1;
    }
    this.expectEnd(i);
    return { type: 'table', name, columns, watermark, options };
  }

  createView(i) {
    const name = this.nameAt(i); i += 1;
    i = this.expectWord(i, 'AS');
    const { query, next } = this.query(i);
    this.expectEnd(next);
    return { type: 'view', name, query };
  }

  insert(i) {
    i = this.expectWord(i, 'INSERT');
    i = this.expectWord(i, 'INTO');
    const target = this.nameAt(i); i += 1;
    const { query, next } = this.query(i);
    this.expectEnd(next);
    if (query.selects.length !== 1) throw new SqlParseError('INSERT with UNION is not supported');
    const sel = query.selects[0];
    if (sel.from.type !== 'table') throw new SqlParseError('INSERT must select from a single table or view');
    return { type: 'insert', target, query };
  }

  // ── queries ──
  /** query := select (UNION ALL select)* */
  query(i) {
    const selects = [];
    let pos = i;
    for (;;) {
      const r = this.select(pos);
      selects.push(r.select);
      pos = r.next;
      if (this.isWord(pos, 'UNION')) {
        if (!this.isWord(pos + 1, 'ALL')) throw new SqlParseError('UNION without ALL is not supported');
        pos += 2;
        continue;
      }
      break;
    }
    return { query: { selects }, next: pos };
  }

  select(i) {
    const start = i;
    i = this.expectWord(i, 'SELECT');
    if (this.isWord(i, 'DISTINCT')) throw new SqlParseError('SELECT DISTINCT is not supported');
    // Select list: up to the top-level FROM.
    let depth = 0;
    let k = i;
    for (; k < this.ts.length; k += 1) {
      if (this.isPunct(k, '(')) depth += 1;
      else if (this.isPunct(k, ')')) { depth -= 1; if (depth < 0) throw new SqlParseError('SELECT without FROM'); }
      else if (depth === 0 && this.isWord(k, 'FROM')) break;
    }
    if (k >= this.ts.length) throw new SqlParseError('SELECT without FROM');
    const items = this.splitCommas(i, k).map(([s, e]) => this.selectItem(s, e));
    if (items.length === 0) throw new SqlParseError('empty select list');

    // FROM
    let pos = k + 1;
    let from;
    if (this.isPunct(pos, '(')) {
      const close = this.matching(pos);
      const sub = this.query(pos + 1);
      if (sub.next !== close) throw new SqlParseError('unsupported subquery syntax');
      from = { type: 'subquery', query: sub.query };
      pos = close + 1;
      if (this.isWord(pos, 'AS')) pos += 2;
    } else if (this.isName(pos)) {
      const name = valueOf(this.ts[pos]);
      pos += 1;
      if (this.isPunct(pos, '(')) {
        const close = this.matching(pos);
        from = { type: 'call', name, args: this.callArgs(pos + 1, close) };
        pos = close + 1;
      } else {
        from = { type: 'table', name };
      }
    } else {
      throw new SqlParseError(`unsupported FROM clause near ${this.describe(pos)}`);
    }

    // WHERE
    let where = null;
    if (this.isWord(pos, 'WHERE')) {
      const wStart = pos + 1;
      let d = 0;
      let e = wStart;
      for (; e < this.ts.length; e += 1) {
        if (this.isPunct(e, '(')) d += 1;
        else if (this.isPunct(e, ')')) { if (d === 0) break; d -= 1; }
        else if (d === 0 && this.isWord(e, 'UNION')) break;
        else if (d === 0 && this.isPunct(e, ';')) break;
      }
      if (e === wStart) throw new SqlParseError('empty WHERE clause');
      where = this.condition(wStart, e);
      pos = e;
    }

    // Anything else (JOIN, GROUP BY, ORDER BY, LIMIT, ...) is outside the supported grammar.
    if (pos < this.ts.length && !this.isWord(pos, 'UNION') && !this.isPunct(pos, ')') && !this.isPunct(pos, ';')) {
      throw new SqlParseError(`unsupported clause ${this.describe(pos)}`);
    }
    return { select: { items, from, where, text: this.slice(start, pos) }, next: pos };
  }

  /** `col = 'literal'` is understood; any other condition is kept as text. */
  condition(a, b) {
    const text = this.slice(a, b);
    if (b - a === 3 && this.isName(a) && this.isPunct(a + 1, '=') && this.ts[a + 2].type === 'string') {
      return { text, column: valueOf(this.ts[a]), value: valueOf(this.ts[a + 2]) };
    }
    return { text, column: null, value: null };
  }

  selectItem(a, b) {
    let end = b;
    let alias = null;
    if (b - a >= 3 && this.isWord(b - 2, 'AS') && this.isName(b - 1)) {
      alias = valueOf(this.ts[b - 1]);
      end = b - 2;
    }
    const expr = this.expression(a, end);
    if (alias === null && expr.type === 'column') alias = expr.name;
    return { alias, expr, text: this.slice(a, b) };
  }

  /** Classifies an expression; only calls, constants and plain columns are broken down. */
  expression(a, b) {
    const text = this.slice(a, b);
    if (b - a === 1 && this.isName(a)) return { type: 'column', name: valueOf(this.ts[a]), text };
    if (b - a === 1 && this.isPunct(a, '*')) return { type: 'star', text };
    if (this.isWord(a, 'CAST') && this.isPunct(a + 1, '(') && this.matching(a + 1) === b - 1
        && b - a === 6 && this.ts[a + 2].type === 'string' && this.isWord(a + 3, 'AS')) {
      return { type: 'constant', value: valueOf(this.ts[a + 2]), dataType: this.ts[a + 4].text, text };
    }
    if (this.ts[a] && this.ts[a].type === 'word' && this.isPunct(a + 1, '(') && this.matching(a + 1) === b - 1) {
      const args = this.splitCommas(a + 2, b - 1).map(([s, e]) => ({
        text: this.slice(s, e),
        value: e - s === 1 ? valueOf(this.ts[s]) : null,
        literal: e - s === 1 && this.ts[s].type === 'string',
      }));
      return { type: 'call', name: this.ts[a].text.toUpperCase(), args, text };
    }
    return { type: 'expr', text };
  }

  /** Arguments of a table-function call: `name => value`. */
  callArgs(a, b) {
    return this.splitCommas(a, b).map(([s, e]) => {
      if (!this.isName(s) || !this.isPunct(s + 1, '=>')) throw new SqlParseError('unsupported function argument syntax');
      const name = valueOf(this.ts[s]);
      const v = s + 2;
      if (this.isWord(v, 'TABLE') && this.isName(v + 1)) {
        let partitionBy = [];
        let k = v + 2;
        if (this.isWord(k, 'PARTITION')) {
          k = this.expectWord(k + 1, 'BY');
          partitionBy = this.splitCommas(k, e).map(([ps, pe]) => {
            if (pe - ps !== 1 || !this.isName(ps)) throw new SqlParseError('unsupported PARTITION BY expression');
            return valueOf(this.ts[ps]);
          });
          k = e;
        }
        if (k !== e) throw new SqlParseError(`unsupported table argument near ${this.describe(k)}`);
        return { name, type: 'table', table: valueOf(this.ts[v + 1]), partitionBy };
      }
      if (this.isWord(v, 'DESCRIPTOR') && this.isPunct(v + 1, '(') && this.matching(v + 1) === e - 1) {
        const columns = this.splitCommas(v + 2, e - 1).map(([ps, pe]) => {
          if (pe - ps !== 1 || !this.isName(ps)) throw new SqlParseError('unsupported DESCRIPTOR argument');
          return valueOf(this.ts[ps]);
        });
        return { name, type: 'descriptor', columns };
      }
      if (e - v === 1 && this.ts[v].type === 'string') return { name, type: 'literal', value: valueOf(this.ts[v]) };
      return { name, type: 'expr', text: this.slice(v, e) };
    });
  }
}

// ─── Semantic helpers ───────────────────────────────────────────────────────────

/** Decoded EVENT_ENRICH specification, or null if the JSON cannot be read. */
export function parseEnrichmentSpec(json) {
  try {
    const spec = JSON.parse(json);
    if (!spec || typeof spec !== 'object') return null;
    return {
      root: spec.root ?? '',
      references: Array.isArray(spec.references) ? spec.references : [],
      steps: Array.isArray(spec.steps) ? spec.steps : [],
    };
  } catch { return null; }
}

function findCall(items, fnName) {
  for (const it of items) {
    if (it.expr.type === 'call' && it.expr.name === fnName) return it;
  }
  return null;
}

/**
 * One input of an input view, with the wrapper added for enrichment unwrapped:
 *   SELECT ..., EVENT_ENRICH(payload, '<spec>') AS payload, ... FROM ( SELECT ... FROM src )
 */
export function describeBranch(select) {
  let enrich = null;
  let cur = select;
  while (cur.from.type === 'subquery' && cur.from.query.selects.length === 1) {
    const call = findCall(cur.items, 'EVENT_ENRICH');
    if (call && call.expr.args[1] && call.expr.args[1].literal) {
      enrich = { alias: call.alias, spec: parseEnrichmentSpec(call.expr.args[1].value), raw: call.expr.args[1].value };
    }
    cur = cur.from.query.selects[0];
  }
  const field = findCall(cur.items, 'EVENT_FIELD');
  const topicItem = cur.items.find(it => it.alias === 'source_topic' && it.expr.type === 'constant');
  return {
    source:       cur.from.type === 'table' ? cur.from.name : null,
    eventType:    field && field.expr.args[2] ? field.expr.args[2].value : null,
    entityPath:   field && field.expr.args[3] ? field.expr.args[3].value : null,
    sourceTopic:  topicItem ? topicItem.expr.value : null,
    where:        cur.where ? cur.where.text : null,
    whereSimple:  cur.where && cur.where.column ? { column: cur.where.column, value: cur.where.value } : null,
    chained:      !!cur.where || (cur.from.type === 'table' && cur.items.some(it => it.expr.type === 'column' && it.expr.name === 'envelope')),
    enrich,
    timeColumn:   cur.items.find(it => it.alias === 'event_ts')?.expr.text ?? null,
  };
}

/** Human wording for an interval such as `'5.000' SECOND`. */
function watermarkDelay(expr) {
  const m = /INTERVAL\s+'([\d.]+)'\s+(\w+)/i.exec(expr || '');
  if (!m) return null;
  const n = Number(m[1]);
  const unit = m[2].toLowerCase();
  return `${Number.isFinite(n) ? n : m[1]} ${unit}${n === 1 ? '' : 's'}`;
}

// ─── Graph ──────────────────────────────────────────────────────────────────────

export const NODE_W = 264;
export const NODE_H = 64;
const H_GAP    = 70;          // minimum gap between two columns
export const LABEL_MAX_CHARS = 36;
/** Width of an edge-label pill (keep in sync with SqlGraphView). */
export function labelWidth(label) {
  return Math.max(24, Math.min(String(label ?? '').length, LABEL_MAX_CHARS) * 6.1 + 12);
}
const V_GAP    = 18;
const BAND_PAD = 14;
const BAND_HEAD = 24;
const BAND_GAP = 22;
const MARGIN   = 16;

/**
 * @param {Array<{kind: string, agentId: (string|null), sql: string}>} statements
 * @returns {{
 *   nodes: object[], edges: object[], bands: object[], width: number, height: number,
 *   unsupported: Array<{index: number, kind: string, agentId: (string|null), reason: string, sql: string}>,
 *   total: number, represented: number
 * }}
 * @throws {SqlParseError} when the graph cannot be built at all (too large, cyclic)
 */
export function buildSqlGraph(statements) {
  if (!Array.isArray(statements)) throw new SqlParseError('no statements');
  if (statements.length > MAX_GRAPH_STATEMENTS) {
    throw new SqlParseError(`too many statements for the graphical view (${statements.length} > ${MAX_GRAPH_STATEMENTS})`);
  }

  const nodes = new Map();           // name -> node
  const edges = [];
  const unsupported = [];

  const agentOf = st => (st.agentId ?? null);
  const addNode = (node) => { nodes.set(node.id, node); return node; };

  const parsed = statements.map((st, index) => {
    const sql = typeof st.sql === 'string' ? st.sql : '';
    return { index, kind: st.kind, agentId: agentOf(st), sql, ...parseStatement(sql) };
  });

  // Pass 1: definitions.
  for (const p of parsed) {
    if (!p.ok) { unsupported.push({ index: p.index, kind: p.kind, agentId: p.agentId, reason: p.reason, sql: p.sql }); continue; }
    const m = p.model;
    if (m.type === 'insert') continue;
    if (nodes.has(m.name)) {
      p.ok = false;
      p.reason = `'${m.name}' is defined more than once`;
      unsupported.push({ index: p.index, kind: p.kind, agentId: p.agentId, reason: p.reason, sql: p.sql });
      continue;
    }
    addNode({
      id: m.name, name: m.name, type: m.type, agentId: p.agentId,
      statementIndex: p.index, sql: p.sql, model: m, role: null,
      branches: m.type === 'view' ? m.query.selects.map(describeBranch) : null,
    });
  }

  const external = (name, agentId) => {
    if (!nodes.has(name)) {
      addNode({ id: name, name, type: 'external', agentId, statementIndex: null, sql: null, model: null, role: null, branches: null });
    }
    return nodes.get(name);
  };

  // Pass 2: edges.
  for (const p of parsed) {
    if (!p.ok) continue;
    const m = p.model;
    if (m.type === 'view') {
      m.query.selects.forEach((sel, branchIndex) => {
        const refs = [];
        const collect = (s) => {
          if (s.from.type === 'table') refs.push({ name: s.from.name, where: s.where });
          else if (s.from.type === 'call') {
            for (const a of s.from.args) if (a.type === 'table') refs.push({ name: a.table, where: null, via: s.from.name });
          } else s.from.query.selects.forEach(collect);
        };
        collect(sel);
        refs.forEach((r, k) => {
          external(r.name, p.agentId);
          const branch = nodes.get(m.name).branches[branchIndex];
          let label = '';
          if (r.via) label = 'events';
          else if (branch.whereSimple) label = `${branch.whereSimple.column} = '${branch.whereSimple.value}'`;
          else if (branch.where) label = branch.where;
          else if (branch.eventType) label = branch.eventType;
          edges.push({
            id: `read:${p.index}:${branchIndex}:${k}`, kind: 'reads', from: r.name, to: m.name,
            label, statementIndex: p.index, branchIndex,
          });
        });
      });
    } else if (m.type === 'insert') {
      const sel = m.query.selects[0];
      external(sel.from.name, p.agentId);
      external(m.target, p.agentId);
      let label = '';
      if (sel.where) label = sel.where.column ? `${sel.where.column} = '${sel.where.value}'` : sel.where.text;
      edges.push({
        id: `write:${p.index}`, kind: 'writes', from: sel.from.name, to: m.target,
        label, statementIndex: p.index, insert: { items: sel.items, where: sel.where, text: p.sql },
        enrich: (() => {
          const call = findCall(sel.items, 'EVENT_ENRICH');
          return call && call.expr.args[1] && call.expr.args[1].literal
            ? { alias: call.alias, spec: parseEnrichmentSpec(call.expr.args[1].value), raw: call.expr.args[1].value }
            : null;
        })(),
      });
    }
  }

  // Roles of tables: a table written by an INSERT is a sink; one that is read is a source.
  const written = new Set(edges.filter(e => e.kind === 'writes').map(e => e.to));
  const read    = new Set(edges.filter(e => e.kind === 'reads').map(e => e.from));
  for (const n of nodes.values()) {
    if (n.type !== 'table') continue;
    if (written.has(n.id) && read.has(n.id)) n.role = 'table';
    else if (written.has(n.id)) n.role = 'sink';
    else if (read.has(n.id)) n.role = 'source';
    else n.role = 'table';
  }

  // Kafka hand-off: a sink table and a source table on the same topic are the same stream
  // (this is how agents are connected when chaining is off).  Only the tables' own options
  // are compared, nothing is inferred.
  const sinks = [...nodes.values()].filter(n => n.type === 'table' && n.role === 'sink' && n.model.options.connector === 'kafka');
  for (const sink of sinks) {
    for (const src of nodes.values()) {
      if (src.type !== 'table' || src.role !== 'source' || src.id === sink.id) continue;
      if (src.model.options.connector !== 'kafka' || src.model.options.topic !== sink.model.options.topic) continue;
      edges.push({
        id: `topic:${sink.id}:${src.id}`, kind: 'topic', from: sink.id, to: src.id,
        label: sink.model.options.topic, statementIndex: null,
      });
    }
  }

  layout(nodes, edges);

  const placed = [...nodes.values()];
  const layerX = columnPositions(placed, edges, nodes);
  const bands = buildBands(placed, layerX);
  const width  = Math.ceil(Math.max(0, ...placed.map(n => n.x + NODE_W)) + MARGIN);
  const height = Math.ceil(Math.max(0, ...bands.map(b => b.y + b.h)) + MARGIN);

  routeEdges(nodes, edges);

  const total = statements.length;
  return {
    nodes: placed, edges, bands, width, height, unsupported,
    total, represented: total - unsupported.length,
  };
}

// ─── Zoom ───────────────────────────────────────────────────────────────────────

export const ZOOM_MIN  = 0.2;
export const ZOOM_MAX  = 2;
export const ZOOM_STEP = 1.25;
/** Below this scale the text is too small to read, so the initial view scrolls instead. */
export const ZOOM_READABLE = 0.6;

export function clampZoom(z) {
  return Number.isFinite(z) ? Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, z)) : 1;
}

/** Scale at which a diagram `graphWidth` wide exactly fills `viewportWidth` (1 if unknown). */
export function fitZoom(viewportWidth, graphWidth) {
  if (!(viewportWidth > 0) || !(graphWidth > 0)) return 1;
  return clampZoom(viewportWidth / graphWidth);
}

/** Initial scale: fit the width, but never shrink below the readable minimum, never enlarge. */
export function initialZoom(viewportWidth, graphWidth) {
  return Math.min(1, Math.max(ZOOM_READABLE, fitZoom(viewportWidth, graphWidth)));
}

// ─── Layout ─────────────────────────────────────────────────────────────────────

/**
 * Longest-path layering.  SQL references cannot be circular (SqlParseError if they are);
 * Kafka topic hand-offs can legitimately form a loop between agents, so those edges are
 * only used for layering when they do not close a cycle.
 */
function layout(nodes, edges) {
  const deps = new Map([...nodes.keys()].map(k => [k, []]));
  const succ = new Map([...nodes.keys()].map(k => [k, []]));
  const link = (e) => { deps.get(e.to).push(e.from); succ.get(e.from).push(e.to); };
  const reaches = (from, target, seen = new Set()) => {
    if (from === target) return true;
    if (seen.has(from)) return false;
    seen.add(from);
    return succ.get(from).some(n => reaches(n, target, seen));
  };
  for (const e of edges) if (e.kind !== 'topic') link(e);
  for (const e of edges) if (e.kind === 'topic' && !reaches(e.to, e.from)) link(e);

  const layer = new Map();
  const visiting = new Set();
  const depth = (id) => {
    if (layer.has(id)) return layer.get(id);
    if (visiting.has(id)) throw new SqlParseError(`circular reference through '${id}'`);
    visiting.add(id);
    let l = 0;
    for (const d of deps.get(id)) l = Math.max(l, depth(d) + 1);
    visiting.delete(id);
    layer.set(id, l);
    return l;
  };
  for (const id of nodes.keys()) depth(id);

  // Producers with no inputs sit right before their first consumer instead of at column 0.
  for (const id of nodes.keys()) {
    if (deps.get(id).length === 0 && succ.get(id).length > 0) {
      layer.set(id, Math.max(0, Math.min(...succ.get(id).map(s => layer.get(s))) - 1));
    }
  }
  for (const [id, l] of layer) nodes.get(id).layer = l;
}

function agentOrder(placed) {
  const order = [];
  const sorted = [...placed].sort((a, b) => (a.statementIndex ?? Infinity) - (b.statementIndex ?? Infinity));
  for (const n of sorted) if (!order.includes(n.agentId)) order.push(n.agentId);
  return order;
}

/**
 * X of the left side of every column.  The gap in front of a column is wide enough for the
 * widest edge label that ends there, so labels sit between columns and never on a box.
 */
function columnPositions(placed, edges, nodes) {
  const maxLayer = Math.max(0, ...placed.map(n => n.layer));
  const labelW = new Array(maxLayer + 1).fill(0);
  for (const e of edges) {
    if (!e.label) continue;
    const l = nodes.get(e.to).layer;
    labelW[l] = Math.max(labelW[l], labelWidth(e.label));
  }
  const xs = [MARGIN + BAND_PAD];
  for (let l = 1; l <= maxLayer; l += 1) {
    xs.push(xs[l - 1] + NODE_W + Math.max(H_GAP, labelW[l] + 28));
  }
  return xs;
}

function buildBands(placed, layerX) {
  const order = agentOrder(placed);
  const bands = [];
  let y = MARGIN;
  for (const agentId of order) {
    const members = placed.filter(n => n.agentId === agentId)
      .sort((a, b) => (a.statementIndex ?? Infinity) - (b.statementIndex ?? Infinity));
    const rowsInLayer = new Map();
    for (const n of members) {
      const row = rowsInLayer.get(n.layer) ?? 0;
      rowsInLayer.set(n.layer, row + 1);
      n.row = row;
    }
    const rows = Math.max(...rowsInLayer.values());
    const innerH = rows * NODE_H + (rows - 1) * V_GAP;
    const h = BAND_HEAD + innerH + BAND_PAD;
    for (const n of members) {
      n.x = layerX[n.layer];
      n.y = y + BAND_HEAD + n.row * (NODE_H + V_GAP);
      n.w = NODE_W;
      n.h = NODE_H;
    }
    const maxLayer = Math.max(...members.map(n => n.layer));
    bands.push({
      agentId, x: MARGIN, y, h,
      w: layerX[maxLayer] + NODE_W + BAND_PAD - MARGIN,
    });
    y += h + BAND_GAP;
  }
  // Bands share one width so the lanes line up.
  const w = Math.max(...bands.map(b => b.w));
  bands.forEach(b => { b.w = w; });
  return bands;
}

/** Cubic Bézier between the right side of `from` and the left side of `to`. */
function routeEdges(nodes, edges) {
  const outs = new Map();
  const ins  = new Map();
  for (const e of edges) {
    (outs.get(e.from) ?? outs.set(e.from, []).get(e.from)).push(e);
    (ins.get(e.to)    ?? ins.set(e.to,    []).get(e.to)).push(e);
  }
  const spread = (list, side) => {
    const sorted = [...list].sort((a, b) => nodes.get(side === 'out' ? a.to : a.from).y - nodes.get(side === 'out' ? b.to : b.from).y);
    sorted.forEach((e, k) => { e[side === 'out' ? 'outShift' : 'inShift'] = (k - (sorted.length - 1) / 2) * 8; });
  };
  outs.forEach(l => spread(l, 'out'));
  ins.forEach(l => spread(l, 'in'));

  for (const e of edges) {
    const a = nodes.get(e.from);
    const b = nodes.get(e.to);
    // Width of the empty column gap right before the consumer.
    const before = [...nodes.values()].filter(n => n.layer === b.layer - 1);
    e.gap = before.length ? b.x - (before[0].x + before[0].w) : b.x - (a.x + a.w);
    const x1 = a.x + a.w;
    const y1 = a.y + a.h / 2 + (e.outShift ?? 0);
    const x2 = b.x;
    const y2 = b.y + b.h / 2 + (e.inShift ?? 0);
    const dx = Math.max(40, Math.abs(x2 - x1) / 2);
    e.path = `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
    // The label sits in the gap in front of the consumer's column, on the curve.
    const bez = (t, p0, p1, p2, p3) => (1 - t) ** 3 * p0 + 3 * (1 - t) ** 2 * t * p1 + 3 * (1 - t) * t * t * p2 + t ** 3 * p3;
    const gapStart = Math.max(x1, x2 - (e.gap ?? 0));
    const targetX = (gapStart + x2) / 2;
    let lo = 0;
    let hi = 1;
    for (let k = 0; k < 24; k += 1) {
      const mid = (lo + hi) / 2;
      if (bez(mid, x1, x1 + dx, x2 - dx, x2) < targetX) lo = mid; else hi = mid;
    }
    e.labelX = bez(lo, x1, x1 + dx, x2 - dx, x2);
    e.labelY = bez(lo, y1, y1, y2, y2);
  }
}

// ─── Descriptions ───────────────────────────────────────────────────────────────

function quote(s) { return `‘${s}’`; }

/**
 * Plain-language description of a node.  Only restates parsed content.
 * @returns {{title: string, kindLabel: string, summary: string, facts: Array<{label: string, value: string}>}}
 */
export function describeNode(node) {
  const m = node.model;
  if (node.type === 'external') {
    return {
      title: node.name, kindLabel: 'Undefined reference',
      summary: `${quote(node.name)} is referenced by the script but not defined by any statement in it.`,
      facts: [],
    };
  }
  if (node.type === 'function') {
    return {
      title: node.name, kindLabel: 'Function',
      summary: `Registers the function ${quote(node.name)} implemented by ${m.className}.`,
      facts: [{ label: 'Class', value: m.className }, ...(m.jar ? [{ label: 'Jar', value: m.jar }] : [])],
    };
  }
  if (node.type === 'table') return describeTable(node);
  return describeView(node);
}

function describeTable(node) {
  const m = node.model;
  const o = m.options;
  const kafka = o.connector === 'kafka';
  const facts = [];
  const add = (label, value) => { if (value !== undefined && value !== null && value !== '') facts.push({ label, value: String(value) }); };

  let summary;
  if (kafka) {
    if (node.role === 'source') summary = `Reads records from the Kafka topic ${quote(o.topic)}.`;
    else if (node.role === 'sink') summary = `Writes records to the Kafka topic ${quote(o.topic)}.`;
    else summary = `Kafka table on the topic ${quote(o.topic)}.`;
  } else {
    summary = `Table using the ${quote(o.connector ?? 'unspecified')} connector.`;
  }

  add('Connector', o.connector);
  add('Topic', o.topic);
  add('Format', o.format ?? [o['key.format'] && `key ${o['key.format']}`, o['value.format'] && `value ${o['value.format']}`].filter(Boolean).join(', '));
  add('Consumer group', o['properties.group.id']);
  add('Start from', o['scan.startup.mode'] && `${o['scan.startup.mode']}${o['properties.auto.offset.reset'] ? ` (no offset: ${o['properties.auto.offset.reset']})` : ''}`);
  add('Delivery guarantee', o['sink.delivery-guarantee']);
  add('Bootstrap servers', o['properties.bootstrap.servers']);

  const wm = m.watermark;
  if (wm) {
    const timeCol = m.columns.find(c => c.name === wm.column);
    const fromField = timeCol && timeCol.kind === 'computed'
      ? /EVENT_FIELD\(\s*`raw`\s*,\s*'ts'\s*,\s*'([^']*)'/.exec(timeCol.expr) : null;
    if (fromField && /COALESCE\(.*`rec_ts`\)/.test(timeCol.expr)) {
      add('Event time', `${wm.column}: the ‘ts’ field of ${quote(fromField[1])} events, else the Kafka record timestamp`);
    } else if (timeCol && timeCol.kind === 'computed') {
      add('Event time', `${wm.column} AS ${timeCol.expr}`);
    } else {
      add('Event time', wm.column);
    }
    const delay = watermarkDelay(wm.expr);
    add('Out-of-order tolerance', delay ?? `watermark ${wm.expr}`);
  }
  const physical = m.columns.filter(c => c.kind !== 'computed');
  add('Columns', physical.map(c => `${c.name} ${c.dataType}${c.kind === 'metadata' ? ` (metadata ${c.metadataKey})` : ''}`).join(', '));
  return { title: node.name, kindLabel: node.role === 'sink' ? 'Sink table' : node.role === 'source' ? 'Source table' : 'Table', summary, facts };
}

function describeView(node) {
  const m = node.model;
  const sel0 = m.query.selects[0];
  const facts = [];
  const add = (label, value) => { if (value !== undefined && value !== null && value !== '') facts.push({ label, value: String(value) }); };

  if (sel0.from.type === 'call') {
    const call = sel0.from;
    const tableArg = call.args.find(a => a.type === 'table');
    const desc = call.args.find(a => a.type === 'descriptor');
    const uid = call.args.find(a => a.name === 'uid' && a.type === 'literal');
    const config = call.args.find(a => a.name === 'agent_config');
    add('Function', call.name);
    add('Input stream', tableArg ? tableArg.table : null);
    add('Partitioned by', tableArg && tableArg.partitionBy.join(', '));
    add('Event time', desc && desc.columns.join(', '));
    add('Operator uid', uid && uid.value);
    if (config) add('Agent configuration', config.type === 'literal' ? `JSON, ${config.value.length} characters (see the SQL)` : config.text);
    add('Output columns', sel0.items.length === 1 && sel0.items[0].expr.type === 'star' ? 'all columns returned by the function' : sel0.items.map(i => i.text).join(', '));
    const parts = tableArg
      ? `Runs the Process Table Function ${quote(call.name)} on ${quote(tableArg.table)}${tableArg.partitionBy.length ? `, one state per ${tableArg.partitionBy.join(', ')}` : ''}.`
      : `Calls the table function ${quote(call.name)}.`;
    return { title: node.name, kindLabel: 'Processor', summary: parts, facts };
  }

  const n = m.query.selects.length;
  const cols = sel0.items.map(i => i.alias ?? i.expr.text);
  const sameCols = m.query.selects.every(s => s.items.map(i => i.alias ?? i.expr.text).join('\u0000') === cols.join('\u0000'));
  add('Columns', sameCols ? cols.join(', ') : 'differ between inputs, see the SQL');
  add('Inputs', n);
  const summary = n > 1
    ? `Combines ${n} inputs (UNION ALL) into one stream.`
    : `Selects from ${sel0.from.type === 'table' ? quote(sel0.from.name) : 'a subquery'}.`;
  return { title: node.name, kindLabel: n > 1 ? 'Input view' : 'View', summary, facts };
}

/** Plain-language description of an INSERT edge. */
export function describeWrite(edge) {
  const ins = edge.insert;
  const cols = ins.items.map(i => i.alias ?? i.expr.text).join(', ');
  const filter = ins.where ? (ins.where.column ? `${ins.where.column} = '${ins.where.value}'` : ins.where.text) : null;
  const facts = [
    { label: 'From', value: edge.from },
    { label: 'Into', value: edge.to },
    { label: 'Columns', value: cols },
  ];
  if (filter) facts.push({ label: 'Only rows where', value: filter });
  return {
    title: `INSERT INTO ${edge.to}`,
    kindLabel: 'Insert',
    summary: `Writes ${filter ? `the rows of ${quote(edge.from)} where ${filter}` : `all rows of ${quote(edge.from)}`} into ${quote(edge.to)}.`,
    facts,
  };
}

/** Human wording for one enrichment specification (EVENT_ENRICH). */
export function describeEnrichment(spec) {
  if (!spec) return ['The enrichment specification could not be decoded; see the SQL.'];
  const lines = spec.steps.map(s => {
    const fields = (s.fields || []).map(f => (typeof f === 'string' ? f : Object.entries(f).map(([to, from]) => `${from} → ${to}`).join(', '))).join(', ');
    const ref = spec.references.find(r => r.name === s.reference);
    return `Looks up ${quote(s.reference)}${ref && ref.file ? ` (${ref.file.split('/').pop()})` : ''} by ${s.key} = ${s.on}`
      + `${s.when ? ` when ${s.when}` : ''}, writing ${fields || 'no fields'} into ${s.into}`
      + `${s.cardinality ? ` (${s.cardinality})` : ''}.`;
  });
  return lines.length ? lines : ['The specification contains no steps.'];
}
