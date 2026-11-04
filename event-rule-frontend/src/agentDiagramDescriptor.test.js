import fs from 'node:fs';
import path from 'node:path';
import { describe, test, expect } from 'vitest';
import {
  descriptorToDiagram,
  buildDiagramDescriptor,
  layoutNodes,
  HISTORY_IN_HANDLE,
} from './agentDiagramDescriptor';

const SAMPLES_DIR = path.resolve(__dirname, '../../samples');
const samples = fs.readdirSync(SAMPLES_DIR)
  .map(d => path.join(SAMPLES_DIR, d, 'agents.json'))
  .filter(f => fs.existsSync(f));

const roundTrip = (descriptor) => {
  const { meta, nodes, edges } = descriptorToDiagram(descriptor);
  return buildDiagramDescriptor(meta, nodes, edges);
};

// topics[] only lists what the author declared; the loader keeps it as is
describe('descriptor <-> diagram round trip', () => {
  test('the samples directory contains descriptors', () => {
    expect(samples.length).toBeGreaterThan(0);
  });

  test.each(samples.map(f => [path.basename(path.dirname(f)), f]))('%s is preserved', (_name, file) => {
    const descriptor = JSON.parse(fs.readFileSync(file, 'utf8'));
    expect(roundTrip(descriptor)).toEqual(descriptor);
  });

  test('unknown properties at every level are preserved', () => {
    const descriptor = {
      name: 'App', icon: 'x',
      topics: [{ name: 't', type: 'T', futureTopicProp: 1 }],
      agents: [{
        agentId: 'a', name: 'A', futureAgentProp: { k: 1 }, entityType: 'E',
        references: [{ name: 'r', type: 'csv', file: 'f.csv', types: { c: 'long' } }],
        subscriptions: [{ eventType: 'T', topic: 't', operation: 'Op', futureSub: true,
          horizon: { type: 'count', value: 5 } }],
        ruleset: { path: 'p.jar', futureRuleset: 1, operations: [{
          operation: 'Op', entityField: 'e', futureOp: 1, outbounds: [{ field: 'f', eventType: 'O', futureOb: 1 }] }] },
        outputs: [{ eventType: 'O', topic: 'out', filterKey: 'k', filterValue: 'v', futureOut: 1 }],
        stateConfig: { ttl: '1d', checkpointInterval: '5m', changeLogTopic: 'cl', futureState: 1 },
      }],
    };
    expect(roundTrip(descriptor)).toEqual(descriptor);
  });

  test('the Decision Service project (ruleset.project) is loaded, edited and saved', () => {
    const descriptor = {
      agents: [{ agentId: 'a', name: 'A', ruleset: { path: 'p.jar', project: 'samples/app/di-project/S.zip' } }],
    };
    const { meta, nodes, edges } = descriptorToDiagram(descriptor);
    const agent = nodes.find(n => n.type === 'agent');
    expect(agent.data.ruleProject).toBe('samples/app/di-project/S.zip');
    agent.data.ruleProject = 'samples/app/di-project/T.zip';
    expect(buildDiagramDescriptor(meta, nodes, edges).agents[0].ruleset.project)
      .toBe('samples/app/di-project/T.zip');
  });

  test('a history-only subscription uses the dedicated handle', () => {
    const { edges } = descriptorToDiagram({
      agents: [{ agentId: 'a', name: 'A', subscriptions: [{ eventType: 'T', topic: 't', operation: '' }] }],
    });
    expect(edges.find(e => e.source.startsWith('topic')).targetHandle).toBe(HISTORY_IN_HANDLE);
  });

  test('the agent-error output is stored on the agent data and not represented as a diagram node or edge', () => {
    const { nodes, edges } = descriptorToDiagram({
      agents: [{ agentId: 'a', name: 'A', outputs: [{ eventType: 'agent-error', topic: 'errs', label: 'Errors', description: 'Agent errors' }] }],
    });
    expect(nodes.filter(n => n.type === 'outbound')).toEqual([]);
    expect(edges).toEqual([]);
    const agent = nodes.find(n => n.type === 'agent');
    expect(agent.data.errorOutput).toEqual({
      eventType: 'agent-error',
      topic: 'errs',
      label: 'Errors',
      description: 'Agent errors',
      extra: undefined,
    });
  });

  test('an operation outbound without a matching output does not invent an output', () => {
    const descriptor = {
      agents: [{ agentId: 'a', name: 'A', ruleset: { path: 'p', operations: [
        { operation: 'Op', outbounds: [{ field: 'f', eventType: 'Ghost' }] }] } }],
    };
    expect(roundTrip(descriptor)).toEqual(descriptor);
  });

  test('an output linked twice is written once', () => {
    const { meta, nodes, edges } = descriptorToDiagram({
      agents: [{ agentId: 'a', name: 'A',
        ruleset: { path: 'p', operations: [
          { operation: 'One', outbounds: [{ field: 'f', eventType: 'O' }] },
          { operation: 'Two', outbounds: [{ field: 'g', eventType: 'O' }] }] },
        outputs: [{ eventType: 'O', topic: 'out' }] }],
    });
    expect(buildDiagramDescriptor(meta, nodes, edges).agents[0].outputs).toEqual([{ eventType: 'O', topic: 'out' }]);
  });

  test('editing an implied topic makes it explicit', () => {
    const { meta, nodes, edges } = descriptorToDiagram({
      agents: [{ agentId: 'a', name: 'A', subscriptions: [{ eventType: 'T', topic: 't', operation: 'Op' }] }],
    });
    expect(buildDiagramDescriptor(meta, nodes, edges).topics).toBeUndefined();
    const edited = nodes.map(n => (n.type === 'topic' ? { ...n, data: { ...n.data, implicit: false, idleness: '10s' } } : n));
    expect(buildDiagramDescriptor(meta, edited, edges).topics).toEqual([{ name: 't', type: 'T', label: 'T', idleness: '10s' }]);
  });

  test('layoutNodes orders topics and outbounds so that no links cross', () => {
    const sub = (topic, operation) => ({ eventType: topic, topic, operation, entityKeyExtractor: 'id' });
    const { nodes, edges } = descriptorToDiagram({
      topics: [{ name: 'tA' }, { name: 'tB' }, { name: 'tC' }, { name: 'tD' }],
      agents: [
        { agentId: 'a1', name: 'A1', subscriptions: [sub('tB', 'OpB'), sub('tD', 'OpD')],
          ruleset: { path: 'p', operations: [{ operation: 'OpB' }, { operation: 'OpD', outbounds: [{ field: 'f', eventType: 'O1' }] }] },
          outputs: [{ eventType: 'O1', topic: 'o1' }] },
        { agentId: 'a2', name: 'A2', subscriptions: [sub('tA', 'OpA'), sub('tC', 'OpC')],
          ruleset: { path: 'p', operations: [{ operation: 'OpA', outbounds: [{ field: 'f', eventType: 'O2' }] }, { operation: 'OpC' }] },
          outputs: [{ eventType: 'O2', topic: 'o2' }] },
      ],
    });
    const laid = layoutNodes(nodes, edges);
    // topics feeding the same agent are adjacent (the input order tA,tB,tC,tD would interleave them)
    const order = ['tA', 'tB', 'tC', 'tD'].map(name => laid.find(n => n.type === 'topic' && n.data.name === name))
      .sort((p, q) => p.position.y - q.position.y).map(n => n.data.name);
    expect(['tBtD', 'tDtB', 'tAtC', 'tCtA']).toContain(order[0] + order[1]);
    expect(['tBtD', 'tDtB', 'tAtC', 'tCtA']).toContain(order[2] + order[3]);
  });

  test('layoutNodes places columns without overlap and does not mutate its input', () => {
    const { nodes, edges } = descriptorToDiagram(JSON.parse(fs.readFileSync(samples[0], 'utf8')));
    const before = JSON.stringify(nodes);
    const laid = layoutNodes(nodes, edges);
    expect(JSON.stringify(nodes)).toBe(before);
    const keys = laid.map(n => `${n.position.x},${n.position.y}`);
    expect(new Set(keys).size).toBe(keys.length);
  });
});
