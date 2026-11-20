/*
 *   Copyright IBM Corp. 2026
 *   Licensed under the Apache License, Version 2.0
 */

'use strict';

const { archiveStatusTopic, evaluateAcks, createArchiveAckWaiter } = require('./archive_status');

const ack = (agentId, status, extra = {}) => ({ agentId, status, subtask: 0, parallelism: 1, requestId: 'r', ...extra });

describe('archiveStatusTopic', () => {
  test('appends -status to the control topic', () => {
    expect(archiveStatusTopic('agent-retail-archive-control')).toBe('agent-retail-archive-control-status');
  });
});

describe('evaluateAcks', () => {
  test('pending until every expected agent answered', () => {
    expect(evaluateAcks([], ['a']).state).toBe('pending');
    expect(evaluateAcks([ack('a', 'LOADED')], ['a', 'b'])).toMatchObject({ state: 'pending', missing: ['b'] });
  });

  test('nothing expected is never "loaded"', () => {
    expect(evaluateAcks([ack('a', 'LOADED')], []).state).toBe('pending');
  });

  test('loaded when all answered LOADED', () => {
    expect(evaluateAcks([ack('a', 'LOADED'), ack('b', 'LOADED')], ['a', 'b'])).toMatchObject({ state: 'loaded', loaded: 2, failures: [] });
  });

  test('failed when all answered FAILED, with the reasons', () => {
    const r = evaluateAcks([ack('a', 'FAILED', { error: 'bad sha' })], ['a']);
    expect(r.state).toBe('failed');
    expect(r.failures).toEqual([{ agentId: 'a', subtask: 0, error: 'bad sha' }]);
  });

  test('partial when agents disagree', () => {
    expect(evaluateAcks([ack('a', 'LOADED'), ack('b', 'FAILED')], ['a', 'b']).state).toBe('partial');
  });

  test('every parallel subtask must answer', () => {
    const one = ack('a', 'LOADED', { subtask: 0, parallelism: 3 });
    expect(evaluateAcks([one], ['a']).state).toBe('pending');
    const all = [0, 1, 2].map(i => ack('a', 'LOADED', { subtask: i, parallelism: 3 }));
    expect(evaluateAcks(all, ['a'])).toMatchObject({ state: 'loaded', loaded: 3 });
  });

  test('one failing subtask makes the result partial', () => {
    const acks = [ack('a', 'LOADED', { subtask: 0, parallelism: 2 }), ack('a', 'FAILED', { subtask: 1, parallelism: 2, error: 'x' })];
    expect(evaluateAcks(acks, ['a']).state).toBe('partial');
  });

  test('the last answer of a subtask wins', () => {
    expect(evaluateAcks([ack('a', 'FAILED'), ack('a', 'LOADED')], ['a']).state).toBe('loaded');
  });

  test('acks of agents that are not expected are ignored', () => {
    expect(evaluateAcks([ack('zzz', 'FAILED'), ack('a', 'LOADED')], ['a']).state).toBe('loaded');
  });

  test('a FAILED ack without a message still reports a reason', () => {
    expect(evaluateAcks([ack('a', 'FAILED')], ['a']).failures[0].error).toBe('unknown error');
  });
});

describe('createArchiveAckWaiter', () => {
  const makeKafka = () => {
    const state = { handler: null, subscribed: [], created: [], consumerDisconnects: 0, groupId: null };
    const kafka = {
      admin: () => ({
        connect: async () => {},
        createTopics: async (o) => { state.created.push(...o.topics.map(t => t.topic)); },
        disconnect: async () => {},
      }),
      consumer: ({ groupId }) => {
        state.groupId = groupId;
        return {
          connect: async () => {},
          subscribe: async ({ topic, fromBeginning }) => { state.subscribed.push({ topic, fromBeginning }); },
          run: async ({ eachMessage }) => { state.handler = eachMessage; },
          disconnect: async () => { state.consumerDisconnects += 1; },
        };
      },
    };
    const send = (payload, wrap = true) => state.handler({
      message: { value: Buffer.from(JSON.stringify(wrap ? { payload } : payload)) },
    });
    return { kafka, state, send };
  };
  const quiet = { warn: () => {} };

  test('listens on the status topic from the end, before anything is published', async () => {
    const { kafka, state } = makeKafka();
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t', 't'], requestId: 'r', expectedAgents: ['a'] });
    expect(state.created).toEqual(['t-status']);
    expect(state.subscribed).toEqual([{ topic: 't-status', fromBeginning: false }]);
    expect(state.groupId).toContain('r');
    await w.close();
  });

  test('resolves as soon as every expected ack is in, and closes the consumer', async () => {
    const { kafka, state, send } = makeKafka();
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a', 'b'] });
    const pending = w.done(5000);
    await send(ack('a', 'LOADED'));
    await send(ack('b', 'LOADED'));
    const acks = await pending;
    expect(acks).toHaveLength(2);
    expect(state.consumerDisconnects).toBe(1);
  });

  test('resolves with what it has on timeout', async () => {
    const { kafka, send } = makeKafka();
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a', 'b'] });
    await send(ack('a', 'LOADED'));
    const acks = await w.done(20);
    expect(evaluateAcks(acks, ['a', 'b']).state).toBe('pending');
  });

  test('ignores other requests, unwrapped payloads are accepted, garbage is skipped', async () => {
    const { kafka, state, send } = makeKafka();
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a'] });
    await send(ack('a', 'LOADED', { requestId: 'other' }));
    await state.handler({ message: { value: Buffer.from('not json') } });
    await send(ack('a', 'LOADED'), false);
    expect(await w.done(1000)).toHaveLength(1);
  });

  test('acks already received when done() is called resolve immediately', async () => {
    const { kafka, send } = makeKafka();
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a'] });
    await send(ack('a', 'LOADED'));
    const started = Date.now();
    await w.done(5000);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  test('a topic that cannot be created is not fatal', async () => {
    const { kafka } = makeKafka();
    const base = kafka.admin;
    kafka.admin = () => ({ ...base(), createTopics: async () => { throw new Error('exists'); } });
    const w = await createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a'] });
    await w.close();
  });

  test('a consumer that cannot start is closed and the error propagates', async () => {
    const { kafka, state } = makeKafka();
    const base = kafka.consumer;
    kafka.consumer = (o) => ({ ...base(o), connect: async () => { throw new Error('no broker'); } });
    await expect(createArchiveAckWaiter({ kafka, log: quiet })({ topics: ['t'], requestId: 'r', expectedAgents: ['a'] }))
      .rejects.toThrow('no broker');
    expect(state.consumerDisconnects).toBe(1);
  });
});
