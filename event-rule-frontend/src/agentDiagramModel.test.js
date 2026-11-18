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
import fs from 'node:fs';
import path from 'node:path';
import { describe, test, expect } from 'vitest';
import {
  buildGraph, lineage, matchNodes, operationFocus, formatHorizon, eventKind, agentHeight, GEO,
} from './agentDiagramModel';

const sample = (name) => JSON.parse(fs.readFileSync(
  path.resolve(__dirname, `../../samples/${name}/agents.json`), 'utf8'));

describe('formatHorizon / eventKind', () => {
  test('formats count, time and hybrid horizons', () => {
    expect(formatHorizon(null)).toBeNull();
    expect(formatHorizon({ type: 'count', value: 100 })).toBe('100 events');
    expect(formatHorizon({ type: 'time', timeLimit: '4h' })).toBe('4h');
    expect(formatHorizon({ type: 'hybrid', countLimit: 5, timeLimit: '1d' })).toBe('5 events / 1d');
  });
  test('classifies event types', () => {
    expect(eventKind('agent-error')).toBe('error');
    expect(eventKind('FraudAlert')).toBe('alert');
    expect(eventKind('Transaction')).toBe('data');
  });
});

describe('buildGraph', () => {
  test('returns an empty graph without agents', () => {
    const g = buildGraph({});
    expect(g.nodes).toEqual([]);
    expect(g.edges).toEqual([]);
    expect(g.stats.agents).toBe(0);
  });

  test('single agent: topics on both sides of the agent', () => {
    const g = buildGraph(sample('banking-fraud-agent'));
    const agent = g.nodes.find(n => n.type === 'agent');
    expect(agent.data.entityType).toBe('Account');
    const inTopic = g.nodes.find(n => n.id === 'topic:agent-transactions');
    const outTopic = g.nodes.find(n => n.id === 'topic:agent-fraud-alerts');
    expect(inTopic.data.kind).toBe('source');
    expect(outTopic.data.kind).toBe('alert');
    expect(inTopic.position.x).toBeLessThan(agent.position.x);
    expect(outTopic.position.x).toBeGreaterThan(agent.position.x);
    expect(g.edges.every(e => e.type === 'flow')).toBe(true);
    for (const n of g.nodes) {
      expect(Number.isFinite(n.position.x)).toBe(true);
      expect(Number.isFinite(n.position.y)).toBe(true);
    }
  });

  test('chained agents share a topic and are laid out in successive columns', () => {
    const g = buildGraph(sample('airtraffic-agent'));
    const link = g.nodes.find(n => n.id === 'topic:agent-tracked-aircraft-events');
    expect(link.data.kind).toBe('link');
    expect(link.data.chained).toBe(true);
    expect(link.data.producers.map(p => p.agentId)).toEqual(['aircraft-agent']);
    expect(link.data.consumers.map(c => c.agentId)).toEqual(['airport-agent']);
    const a = g.nodes.find(n => n.id === 'agent:aircraft-agent');
    const b = g.nodes.find(n => n.id === 'agent:airport-agent');
    expect(link.position.x).toBeGreaterThan(a.position.x);
    expect(b.position.x).toBeGreaterThan(link.position.x);
    expect(g.stats.chains).toBe(1);
  });

  test('nodes of the same column never overlap', () => {
    const g = buildGraph(sample('airtraffic-agent'), { expanded: new Set(['aircraft-agent', 'airport-agent']) });
    for (const n of g.nodes) for (const m of g.nodes) {
      if (n === m || n.position.x !== m.position.x) continue;
      const apart = n.position.y + n.height <= m.position.y || m.position.y + m.height <= n.position.y;
      expect(apart).toBe(true);
    }
  });

  test('hiding alert/error outputs removes only unconsumed sinks', () => {
    const full = buildGraph(sample('airtraffic-agent'));
    const lean = buildGraph(sample('airtraffic-agent'), { hidden: new Set(['alert', 'error']) });
    expect(full.nodes.some(n => n.id === 'topic:agent-atc-alerts')).toBe(true);
    expect(lean.nodes.some(n => n.id === 'topic:agent-atc-alerts')).toBe(false);
    expect(lean.nodes.some(n => n.id === 'topic:agent-tracked-aircraft-events')).toBe(true);
  });

  test('expanding an agent adds operation rows and routes edges to operation ports', () => {
    const collapsed = buildGraph(sample('banking-fraud-agent'));
    const open = buildGraph(sample('banking-fraud-agent'), { expanded: new Set(['banking-fraud-agent']) });
    const c = collapsed.nodes.find(n => n.type === 'agent');
    const o = open.nodes.find(n => n.type === 'agent');
    expect(c.height).toBe(agentHeight(o.data.ops.length, false));
    expect(o.height).toBeGreaterThan(c.height);
    expect(collapsed.edges.every(e => [e.sourceHandle, e.targetHandle].some(h => h === 'in' || h === 'out'))).toBe(true);
    expect(open.edges.some(e => e.targetHandle === 'in:EvaluateTransaction')).toBe(true);
    expect(o.data.ops[0].handleY).toBeGreaterThan(GEO.headH);
  });

  test('a subscription without a topic becomes a scheduler node', () => {
    const g = buildGraph({
      agents: [{
        agentId: 'a', name: 'A',
        subscriptions: [{ eventType: 'Schedule', operation: 'Tick' }, { eventType: 'E', topic: 't', operation: 'Do' }],
        outputs: [],
      }],
    });
    expect(g.nodes.find(n => n.id === 'schedule:a').data.kind).toBe('schedule');
  });

  test('tolerates cyclic chaining without hanging', () => {
    const g = buildGraph({
      agents: [
        { agentId: 'a', subscriptions: [{ eventType: 'X', topic: 'x', operation: 'o' }], outputs: [{ eventType: 'Y', topic: 'y' }] },
        { agentId: 'b', subscriptions: [{ eventType: 'Y', topic: 'y', operation: 'o' }], outputs: [{ eventType: 'X', topic: 'x' }] },
      ],
    });
    expect(g.nodes).toHaveLength(4);
  });
});

describe('lineage / focus / search', () => {
  const g = buildGraph(sample('airtraffic-agent'));

  test('lineage of the airport agent walks back through the chain to the aircraft sources', () => {
    const l = lineage(g, 'agent:airport-agent');
    expect(l.nodes.has('agent:aircraft-agent')).toBe(true);
    expect(l.nodes.has('topic:agent-aircraft-events')).toBe(true);
    expect(l.nodes.has('topic:agent-atc-alerts')).toBe(true);
  });

  test('operationFocus keeps only that operation\'s links', () => {
    const f = operationFocus(g, 'airport-agent', 'ProcessAirportEvent');
    expect(f.nodes.has('topic:agent-airport-events')).toBe(true);
    expect(f.nodes.has('topic:agent-airport-init-events')).toBe(false);
  });

  test('matchNodes requires every term and returns null for an empty query', () => {
    expect(matchNodes(g, '  ')).toBeNull();
    const ids = matchNodes(g, 'airport init');
    expect(ids.has('topic:agent-airport-init-events')).toBe(true);
    expect(ids.has('topic:agent-aircraft-events')).toBe(false);
  });
});
