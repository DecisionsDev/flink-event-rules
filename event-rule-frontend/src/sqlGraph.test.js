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
import { describe, test, expect } from 'vitest';
import {
  tokenizeSql, parseStatement, buildSqlGraph, describeNode, describeWrite, describeEnrichment,
  SqlParseError, MAX_GRAPH_STATEMENTS, LABEL_MAX_CHARS, labelWidth,
  ZOOM_MIN, ZOOM_MAX, ZOOM_READABLE, clampZoom, fitZoom, initialZoom,
} from './sqlGraph.js';
import airline from './__fixtures__/ptf-airline.json';
import bankingFraud from './__fixtures__/ptf-banking-fraud.json';
import airtrafficChained from './__fixtures__/ptf-airtraffic-chained.json';
import airtrafficUnchained from './__fixtures__/ptf-airtraffic-unchained.json';

const FIXTURES = {
  airline, 'banking-fraud': bankingFraud, 'airtraffic (chained)': airtrafficChained, 'airtraffic (unchained)': airtrafficUnchained,
};

describe('tokenizeSql', () => {
  test('handles quoted identifiers, escaped quotes, numbers, comments and operators', () => {
    const t = tokenizeSql("SELECT `a``b`, 'it''s', 4.5 -- c\n, x => y /* z */");
    expect(t.map(x => x.type)).toEqual(['word', 'ident', 'punct', 'string', 'punct', 'number', 'punct', 'word', 'punct', 'word']);
    expect(t[1].text).toBe('`a``b`');
    expect(t[3].text).toBe("'it''s'");
    expect(t[8].text).toBe('=>');
  });

  test('does not see structure inside strings', () => {
    const t = tokenizeSql(`'a;b -- not a comment, FROM'`);
    expect(t).toHaveLength(1);
    expect(t[0].type).toBe('string');
  });

  test('throws on unterminated quotes and comments', () => {
    expect(() => tokenizeSql("SELECT 'oops")).toThrow(SqlParseError);
    expect(() => tokenizeSql('SELECT `oops')).toThrow(SqlParseError);
    expect(() => tokenizeSql('SELECT /* oops')).toThrow(SqlParseError);
  });
});

describe('parseStatement', () => {
  test('CREATE TABLE: columns, metadata, computed column, watermark and options', () => {
    const r = parseStatement(`CREATE TEMPORARY TABLE \`t\` (
      \`raw\` STRING,
      \`rec_ts\` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
      \`event_ts\` AS COALESCE(TO_TIMESTAMP_LTZ(CAST(EVENT_FIELD(\`raw\`, 'ts', 'E', '$.k') AS BIGINT), 3), \`rec_ts\`),
      WATERMARK FOR \`event_ts\` AS \`event_ts\` - INTERVAL '5.000' SECOND
    ) WITH ('connector' = 'kafka', 'topic' = 'a,b(c)')`);
    expect(r.ok).toBe(true);
    const m = r.model;
    expect(m.type).toBe('table');
    expect(m.columns.map(c => [c.name, c.kind])).toEqual([['raw', 'physical'], ['rec_ts', 'metadata'], ['event_ts', 'computed']]);
    expect(m.columns[0].dataType).toBe('STRING');
    expect(m.columns[1].dataType).toBe('TIMESTAMP_LTZ(3)');
    expect(m.columns[1].metadataKey).toBe('timestamp');
    expect(m.watermark.column).toBe('event_ts');
    expect(m.options).toEqual({ connector: 'kafka', topic: 'a,b(c)' });
  });

  test('CREATE VIEW with UNION ALL over a wrapped (enriched) branch', () => {
    const r = parseStatement(`CREATE TEMPORARY VIEW v AS
      SELECT a, EVENT_ENRICH(p, '{"x":1}') AS p FROM (SELECT a, p FROM s1)
      UNION ALL
      SELECT a, p FROM s2 WHERE t = 'X'`);
    expect(r.ok).toBe(true);
    expect(r.model.query.selects).toHaveLength(2);
    expect(r.model.query.selects[0].from.type).toBe('subquery');
    expect(r.model.query.selects[1].where).toMatchObject({ column: 't', value: 'X' });
  });

  test('CREATE VIEW over a table-function call', () => {
    const r = parseStatement(`CREATE VIEW o AS SELECT * FROM fn(events => TABLE i PARTITION BY k, on_time => DESCRIPTOR(ts), uid => 'u')`);
    expect(r.ok).toBe(true);
    const call = r.model.query.selects[0].from;
    expect(call.type).toBe('call');
    expect(call.args).toEqual([
      { name: 'events', type: 'table', table: 'i', partitionBy: ['k'] },
      { name: 'on_time', type: 'descriptor', columns: ['ts'] },
      { name: 'uid', type: 'literal', value: 'u' },
    ]);
  });

  test('INSERT ... SELECT ... WHERE', () => {
    const r = parseStatement("INSERT INTO sink SELECT k, v FROM o WHERE out_type = 'Alert'");
    expect(r.ok).toBe(true);
    expect(r.model).toMatchObject({ type: 'insert', target: 'sink' });
    expect(r.model.query.selects[0].where).toMatchObject({ column: 'out_type', value: 'Alert' });
  });

  test.each([
    ['empty', '', /empty/],
    ['joins', 'CREATE VIEW v AS SELECT a FROM x JOIN y ON x.a = y.a', /unsupported clause 'JOIN'/],
    ['group by', 'CREATE VIEW v AS SELECT a FROM x GROUP BY a', /unsupported clause 'GROUP'/],
    ['limit', 'INSERT INTO s SELECT a FROM x LIMIT 5', /unsupported clause 'LIMIT'/],
    ['UNION without ALL', 'CREATE VIEW v AS SELECT a FROM x UNION SELECT a FROM y', /UNION without ALL/],
    ['DISTINCT', 'CREATE VIEW v AS SELECT DISTINCT a FROM x', /DISTINCT/],
    ['DROP', 'DROP TABLE x', /expected CREATE/],
    ['CREATE CATALOG', 'CREATE CATALOG c WITH (\'type\'=\'x\')', /unsupported statement CREATE CATALOG/],
    ['CTE', 'WITH a AS (SELECT 1) SELECT * FROM a', /expected CREATE/],
    ['trailing garbage', "CREATE TABLE t (a INT) WITH ('x'='y') FOO", /unsupported syntax near 'FOO'/],
    ['non-literal option', "CREATE TABLE t (a INT) WITH ('x' = y)", /unsupported table option/],
    ['unbalanced', 'CREATE TABLE t (a INT', /unbalanced/],
    ['primary key', 'CREATE TABLE t (a INT, PRIMARY KEY (a) NOT ENFORCED)', /constraint/],
    ['insert with union', 'INSERT INTO s SELECT a FROM x UNION ALL SELECT a FROM y', /UNION/],
  ])('reports %s as unsupported with a reason', (_name, sql, reason) => {
    const r = parseStatement(sql);
    expect(r.ok).toBe(false);
    expect(r.reason).toMatch(reason);
  });

  test('never throws on arbitrary garbage', () => {
    for (const sql of ['((((', ')', "'", 'SELECT', 'CREATE', 'CREATE TABLE', 'CREATE VIEW v AS', ';;;', '\u0000', 'INSERT INTO']) {
      expect(() => parseStatement(sql)).not.toThrow();
      expect(parseStatement(sql).ok).toBe(false);
    }
  });
});

describe.each(Object.entries(FIXTURES))('buildSqlGraph on real generator output — %s', (_name, fixture) => {
  const graph = buildSqlGraph(fixture.statements);

  test('every statement is represented, none is dropped or invented', () => {
    expect(graph.unsupported).toEqual([]);
    expect(graph.total).toBe(fixture.statements.length);
    expect(graph.represented).toBe(fixture.statements.length);

    // Every CREATE statement is exactly one node, every INSERT exactly one edge.
    const creates = fixture.statements.filter(s => s.kind !== 'insert');
    const inserts = fixture.statements.filter(s => s.kind === 'insert');
    expect(graph.nodes.filter(n => n.statementIndex !== null)).toHaveLength(creates.length);
    expect(graph.edges.filter(e => e.kind === 'writes')).toHaveLength(inserts.length);
    // Each node points at the statement it was parsed from.
    for (const n of graph.nodes.filter(x => x.statementIndex !== null)) {
      expect(fixture.statements[n.statementIndex].sql).toBe(n.sql);
    }
    // Nothing references an undefined name.
    expect(graph.nodes.filter(n => n.type === 'external')).toEqual([]);
  });

  test('the data flow is source → input view → processor → sink', () => {
    const byId = new Map(graph.nodes.map(n => [n.id, n]));
    for (const e of graph.edges.filter(x => x.kind !== 'topic')) {
      expect(byId.get(e.from).layer).toBeLessThan(byId.get(e.to).layer);
    }
    const kinds = new Set(graph.nodes.map(n => n.role ?? n.type));
    expect(kinds).toEqual(new Set(['source', 'view', 'sink']));
    const processors = graph.nodes.filter(n => n.type === 'view' && n.model.query.selects[0].from.type === 'call');
    const agents = new Set(fixture.statements.map(s => s.agentId));
    expect(processors).toHaveLength(agents.size);
  });

  test('every Kafka topic of the SQL appears on exactly the nodes of that topic', () => {
    const topicsInSql = fixture.statements
      .filter(s => s.kind === 'table')
      .map(s => /'topic' = '([^']*)'/.exec(s.sql)[1]);
    const topicsInGraph = graph.nodes.filter(n => n.type === 'table').map(n => n.model.options.topic);
    expect(topicsInGraph.sort()).toEqual(topicsInSql.sort());
  });

  test('nodes do not overlap and edges have geometry inside the canvas', () => {
    for (const a of graph.nodes) {
      expect(a.x).toBeGreaterThanOrEqual(0);
      expect(a.x + a.w).toBeLessThanOrEqual(graph.width);
      expect(a.y + a.h).toBeLessThanOrEqual(graph.height);
      for (const b of graph.nodes) {
        if (a === b) continue;
        const apart = a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h <= b.y || b.y + b.h <= a.y;
        expect(apart).toBe(true);
      }
    }
    for (const e of graph.edges) {
      expect(e.path).toMatch(/^M[\d.,-]+ C/);
      expect(Number.isFinite(e.labelX) && Number.isFinite(e.labelY)).toBe(true);
    }
  });

  test('edge labels sit in the gaps: over no box and over no other label', () => {
    const hit = (p, q) => !(p.x + p.w <= q.x || q.x + q.w <= p.x || p.y + p.h <= q.y || q.y + q.h <= p.y);
    const pills = graph.edges.filter(e => e.label).map(e => {
      const w = labelWidth(e.label);
      return { x: e.labelX - w / 2, y: e.labelY - 9, w, h: 18, label: e.label };
    });
    expect(pills.length).toBeGreaterThan(0);
    for (const p of pills) {
      expect(p.x).toBeGreaterThanOrEqual(0);
      expect(p.x + p.w).toBeLessThanOrEqual(graph.width);
      for (const n of graph.nodes) expect([p.label, hit(p, n)]).toEqual([p.label, false]);
    }
    for (let i = 0; i < pills.length; i += 1) {
      for (let j = i + 1; j < pills.length; j += 1) expect([pills[i].label, pills[j].label, hit(pills[i], pills[j])]).toEqual([pills[i].label, pills[j].label, false]);
    }
  });

  test('no label of the real generator output is truncated', () => {
    for (const e of graph.edges) expect(e.label.length).toBeLessThanOrEqual(LABEL_MAX_CHARS);
    for (const n of graph.nodes.filter(x => x.type === 'table')) expect(n.model.options.topic.length).toBeLessThanOrEqual(34);
  });

  test('every node and edge can be described', () => {
    for (const n of graph.nodes) {
      const d = describeNode(n);
      expect(d.title).toBeTruthy();
      expect(d.summary).toBeTruthy();
    }
    for (const e of graph.edges.filter(x => x.kind === 'writes')) {
      const d = describeWrite(e);
      expect(d.summary).toContain(e.to);
    }
  });
});

describe('buildSqlGraph — what the graph says is what the SQL says', () => {
  const g = buildSqlGraph(airline.statements);
  const node = (frag) => g.nodes.find(n => n.id.includes(frag));

  test('source table', () => {
    const d = describeNode(node('src_agent_flight_events'));
    expect(d.kindLabel).toBe('Source table');
    expect(d.summary).toBe('Reads records from the Kafka topic ‘agent-flight-events’.');
    const facts = Object.fromEntries(d.facts.map(f => [f.label, f.value]));
    expect(facts['Consumer group']).toBe('agent-airline-agent');
    expect(facts['Out-of-order tolerance']).toBe('5 seconds');
    expect(facts['Event time']).toContain('FlightEvent');
  });

  test('input view combines its three inputs and keeps the event type of each edge', () => {
    const inView = node('airline_agent_in');
    expect(describeNode(inView).summary).toBe('Combines 3 inputs (UNION ALL) into one stream.');
    const labels = g.edges.filter(e => e.to === inView.id).map(e => e.label).sort();
    expect(labels).toEqual(['CateringEvent', 'FlightEvent', 'PassengerEvent']);
  });

  test('processor: function, partitioning and operator uid come from the SQL', () => {
    const d = describeNode(node('airline_agent_out'));
    expect(d.kindLabel).toBe('Processor');
    const facts = Object.fromEntries(d.facts.map(f => [f.label, f.value]));
    expect(facts.Function).toBe('event_rule_airline_agent');
    expect(facts['Partitioned by']).toBe('entity_id');
    expect(facts['Operator uid']).toBe('agent-processor-airline-agent');
    expect(facts['Event time']).toBe('event_ts');
  });

  test('inserts keep their filter', () => {
    const w = g.edges.filter(e => e.kind === 'writes').map(e => [e.to.replace('airline_agent_sink_', ''), e.label]);
    expect(w).toEqual([
      ['agent_flight_notifications', "out_type = 'FlightNotification'"],
      ['agent_passenger_notifications', "out_type = 'PassengerNotification'"],
      ['agent_airline_errors', "out_type = 'agent-error'"],
      ['changelog', "out_type = 'entity-changelog'"],
    ]);
    const d = describeWrite(g.edges.find(e => e.to.endsWith('passenger_notifications')));
    expect(d.summary).toBe("Writes the rows of ‘airline_agent_out’ where out_type = 'PassengerNotification' into ‘airline_agent_sink_agent_passenger_notifications’.");
  });

  test('enrichment of an output is decoded from the JSON the SQL contains', () => {
    const w = g.edges.find(e => e.kind === 'writes' && e.enrich);
    expect(w.to).toContain('passenger_notifications');
    const lines = describeEnrichment(w.enrich.spec);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('‘passengers’');
    expect(lines[0]).toContain('passengerId = $.passengerNumber');
    expect(lines[0]).toContain('name, loyaltyStatus, email, phone, contactPreference');
  });

  test('enrichment of an input is found behind the wrapping subquery', () => {
    const inView = node('airline_agent_in');
    const enriched = inView.branches.filter(b => b.enrich);
    expect(enriched).toHaveLength(1);
    expect(enriched[0].eventType).toBe('FlightEvent');
    expect(describeEnrichment(enriched[0].enrich.spec)).toHaveLength(4);
  });
});

describe('buildSqlGraph — chaining', () => {
  test('chained: the downstream input view reads the upstream output view, filtered', () => {
    const g = buildSqlGraph(airtrafficChained.statements);
    const e = g.edges.find(x => x.from === 'aircraft_agent_out' && x.to === 'airport_agent_in');
    expect(e).toBeTruthy();
    expect(e.label).toBe("out_type = 'TrackedAircraftEvent'");
    expect(g.edges.some(x => x.kind === 'topic')).toBe(false);
    expect(g.bands.map(b => b.agentId)).toEqual(['aircraft-agent', 'airport-agent']);
  });

  test('unchained: agents are linked through the Kafka topic only', () => {
    const g = buildSqlGraph(airtrafficUnchained.statements);
    const topic = g.edges.filter(x => x.kind === 'topic');
    expect(topic).toHaveLength(1);
    expect(topic[0].label).toBe('agent-tracked-aircraft-events');
    expect(g.edges.some(x => x.from === 'aircraft_agent_out' && x.to === 'airport_agent_in')).toBe(false);
  });
});

describe('buildSqlGraph — unsupported and invalid input', () => {
  const ok = airline.statements.slice(0, 3);

  test('unsupported statements are reported with index, reason and SQL, the rest still drawn', () => {
    const odd = { kind: 'view', agentId: 'airline-agent', sql: 'CREATE VIEW j AS SELECT a FROM x JOIN y ON x.a = y.a' };
    const g = buildSqlGraph([...ok, odd]);
    expect(g.unsupported).toEqual([{ index: 3, kind: 'view', agentId: 'airline-agent', reason: expect.stringMatching(/JOIN/), sql: odd.sql }]);
    expect(g.total).toBe(4);
    expect(g.represented).toBe(3);
    expect(g.nodes.some(n => n.id === 'j')).toBe(false);
  });

  test('a duplicate definition is flagged, not silently merged', () => {
    const g = buildSqlGraph([ok[0], ok[0]]);
    expect(g.unsupported).toHaveLength(1);
    expect(g.unsupported[0].reason).toMatch(/defined more than once/);
  });

  test('a reference to an undefined name becomes a visibly undefined node', () => {
    const g = buildSqlGraph([{ kind: 'insert', agentId: 'a', sql: "INSERT INTO s SELECT k FROM missing WHERE t = 'x'" }]);
    expect(g.nodes.filter(n => n.type === 'external').map(n => n.id).sort()).toEqual(['missing', 's']);
    expect(describeNode(g.nodes.find(n => n.id === 'missing')).kindLabel).toBe('Undefined reference');
  });

  test('statements without an agent form their own lane', () => {
    const g = buildSqlGraph([{ kind: 'table', agentId: null, sql: "CREATE TABLE t (a INT) WITH ('connector' = 'kafka', 'topic' = 'x')" }]);
    expect(g.bands.map(b => b.agentId)).toEqual([null]);
  });

  test('a circular view definition is rejected, not looped on', () => {
    expect(() => buildSqlGraph([
      { kind: 'view', agentId: 'a', sql: 'CREATE VIEW a AS SELECT x FROM b' },
      { kind: 'view', agentId: 'a', sql: 'CREATE VIEW b AS SELECT x FROM a' },
    ])).toThrow(/circular/);
  });

  test('too many statements are refused', () => {
    const many = Array.from({ length: MAX_GRAPH_STATEMENTS + 1 }, () => ok[0]);
    expect(() => buildSqlGraph(many)).toThrow(/too many statements/);
  });

  test('statements that are not a list, or have no SQL text, do not crash', () => {
    expect(() => buildSqlGraph(null)).toThrow(SqlParseError);
    const g = buildSqlGraph([{ kind: 'table', agentId: 'a' }]);
    expect(g.unsupported).toHaveLength(1);
  });

  test('is fast enough for large scripts', () => {
    const many = Array.from({ length: MAX_GRAPH_STATEMENTS }, (_, i) => ({
      kind: 'table', agentId: `a${i % 20}`,
      sql: `CREATE TEMPORARY TABLE \`t${i}\` (\`k\` STRING, \`v\` STRING) WITH ('connector' = 'kafka', 'topic' = 'x${i}', 'format' = 'raw')`,
    }));
    const t = performance.now();
    const g = buildSqlGraph(many);
    expect(g.nodes).toHaveLength(MAX_GRAPH_STATEMENTS);
    expect(performance.now() - t).toBeLessThan(2000);
  });
});

describe('describeEnrichment', () => {
  test('falls back when the specification cannot be decoded', () => {
    expect(describeEnrichment(null)[0]).toMatch(/could not be decoded/);
  });
});

describe('zoom helpers', () => {
  test('clampZoom keeps the scale within the limits and ignores garbage', () => {
    expect(clampZoom(0.01)).toBe(ZOOM_MIN);
    expect(clampZoom(50)).toBe(ZOOM_MAX);
    expect(clampZoom(1.5)).toBe(1.5);
    expect(clampZoom(NaN)).toBe(1);
    expect(clampZoom(undefined)).toBe(1);
  });

  test('fitZoom fills the viewport width, and is 1 when a size is unknown', () => {
    expect(fitZoom(1000, 2000)).toBe(0.5);
    expect(fitZoom(1000, 500)).toBe(2);
    expect(fitZoom(0, 2000)).toBe(1);
    expect(fitZoom(1000, 0)).toBe(1);
    expect(fitZoom(1000, 100000)).toBe(ZOOM_MIN);
  });

  test('initialZoom shrinks a wide diagram to fit, but not below readable and never enlarges', () => {
    expect(initialZoom(1000, 800)).toBe(1);                       // already fits: actual size
    expect(initialZoom(1000, 1250)).toBe(0.8);                    // fit
    expect(initialZoom(1000, 3390)).toBe(ZOOM_READABLE);          // too wide: stay readable, scroll
    expect(initialZoom(0, 3390)).toBe(1);                         // not measured yet
  });

  test('the widest real diagram (unchained airtraffic) is readable in a 1100px window', () => {
    const g = buildSqlGraph(airtrafficUnchained.statements);
    expect(g.width).toBeGreaterThan(3000);
    expect(initialZoom(1100, g.width)).toBe(ZOOM_READABLE);
  });
});

