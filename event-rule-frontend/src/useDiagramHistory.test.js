// @vitest-environment jsdom
import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import {
  deepClone,
  areSnapshotsEqual,
  useDiagramHistory,
  MAX_HISTORY,
} from './useDiagramHistory';

describe('useDiagramHistory', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('deepClone', () => {
    test('clones primitives', () => {
      expect(deepClone(42)).toBe(42);
      expect(deepClone('hello')).toBe('hello');
      expect(deepClone(null)).toBe(null);
      expect(deepClone(undefined)).toBe(undefined);
      expect(deepClone(true)).toBe(true);
    });

    test('clones nested objects and arrays without shared references', () => {
      const orig = { a: [1, 2, { b: 'test' }], c: { d: 3 } };
      const copy = deepClone(orig);
      expect(copy).toEqual(orig);
      expect(copy).not.toBe(orig);
      expect(copy.a).not.toBe(orig.a);
      expect(copy.a[2]).not.toBe(orig.a[2]);
      expect(copy.c).not.toBe(orig.c);
    });

    test('preserves File and Blob instances', () => {
      const file = new File(['content'], 'test.txt', { type: 'text/plain' });
      const orig = { file, name: 'file-container' };
      const copy = deepClone(orig);
      expect(copy.file).toBe(file);
      expect(copy.name).toBe('file-container');
    });
  });

  describe('areSnapshotsEqual', () => {
    test('returns true for structurally identical snapshots', () => {
      const s1 = {
        nodes: [{ id: '1', data: { label: 'Node 1' } }],
        edges: [{ id: 'e1', source: '1', target: '2' }],
        agentsMeta: { name: 'App' },
      };
      const s2 = {
        nodes: [{ id: '1', data: { label: 'Node 1' } }],
        edges: [{ id: 'e1', source: '1', target: '2' }],
        agentsMeta: { name: 'App' },
      };
      expect(areSnapshotsEqual(s1, s2)).toBe(true);
    });

    test('returns false when nodes, edges, or agentsMeta differ', () => {
      const s1 = {
        nodes: [{ id: '1', data: { label: 'Node 1' } }],
        edges: [],
        agentsMeta: { name: 'App' },
      };
      const s2 = {
        nodes: [{ id: '1', data: { label: 'Node 2' } }],
        edges: [],
        agentsMeta: { name: 'App' },
      };
      expect(areSnapshotsEqual(s1, s2)).toBe(false);
    });

    test('handles null or undefined correctly', () => {
      expect(areSnapshotsEqual(null, null)).toBe(true);
      expect(areSnapshotsEqual(null, {})).toBe(false);
    });
  });

  describe('hook operations', () => {
    const initialSnapshot = {
      nodes: [{ id: 'node-1', type: 'topic', position: { x: 0, y: 0 }, data: { label: 'Topic 1' } }],
      edges: [],
      agentsMeta: { name: 'Initial App', folderName: 'app' },
    };

    test('initializes with canUndo=false and canRedo=false', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));
      expect(result.current.canUndo).toBe(false);
      expect(result.current.canRedo).toBe(false);
      expect(result.current.undo()).toBeNull();
      expect(result.current.redo()).toBeNull();
    });

    test('pushSnapshot enables undo, and undo reverts to previous snapshot', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      const step2Nodes = [
        ...initialSnapshot.nodes,
        { id: 'node-2', type: 'agent', position: { x: 100, y: 100 }, data: { label: 'Agent 1' } },
      ];

      act(() => {
        result.current.pushSnapshot(step2Nodes, [], initialSnapshot.agentsMeta);
      });

      expect(result.current.canUndo).toBe(true);
      expect(result.current.canRedo).toBe(false);

      let reverted;
      act(() => {
        reverted = result.current.undo();
      });

      expect(reverted.nodes).toEqual(initialSnapshot.nodes);
      expect(result.current.canUndo).toBe(false);
      expect(result.current.canRedo).toBe(true);

      let redone;
      act(() => {
        redone = result.current.redo();
      });

      expect(redone.nodes).toEqual(step2Nodes);
      expect(result.current.canUndo).toBe(true);
      expect(result.current.canRedo).toBe(false);
    });

    test('pushSnapshot ignores identical snapshots without adding history', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      act(() => {
        result.current.pushSnapshot(initialSnapshot.nodes, initialSnapshot.edges, initialSnapshot.agentsMeta);
      });

      expect(result.current.canUndo).toBe(false);
    });

    test('pushing a new snapshot after undo clears future stack (branching)', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      const step2Nodes = [
        ...initialSnapshot.nodes,
        { id: 'node-2', type: 'agent', position: { x: 100, y: 100 }, data: { label: 'Agent 1' } },
      ];
      act(() => {
        result.current.pushSnapshot(step2Nodes, [], initialSnapshot.agentsMeta);
      });

      act(() => {
        result.current.undo();
      });
      expect(result.current.canRedo).toBe(true);

      const branchNodes = [
        ...initialSnapshot.nodes,
        { id: 'node-3', type: 'entity', position: { x: 50, y: 50 }, data: { label: 'Entity 1' } },
      ];
      act(() => {
        result.current.pushSnapshot(branchNodes, [], initialSnapshot.agentsMeta);
      });

      expect(result.current.canRedo).toBe(false);

      let reverted;
      act(() => {
        reverted = result.current.undo();
      });
      expect(reverted.nodes).toEqual(initialSnapshot.nodes);
    });

    test('caps past history at MAX_HISTORY', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      for (let i = 0; i < MAX_HISTORY + 10; i++) {
        act(() => {
          result.current.pushSnapshot(
            [{ id: `node-${i}`, type: 'topic', position: { x: i, y: i }, data: { label: `Node ${i}` } }],
            [],
            initialSnapshot.agentsMeta,
          );
        });
      }

      let undoCount = 0;
      while (result.current.canUndo) {
        act(() => {
          result.current.undo();
        });
        undoCount++;
      }

      expect(undoCount).toBe(MAX_HISTORY);
    });

    test('updateDebouncedSnapshot batches rapid text changes into single undo step', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      // Simulate typing 'A', 'AB', 'ABC' in rapid succession
      act(() => {
        result.current.updateDebouncedSnapshot(
          [{ ...initialSnapshot.nodes[0], data: { label: 'A' } }],
          [],
          initialSnapshot.agentsMeta,
        );
      });
      expect(result.current.canUndo).toBe(true);

      act(() => {
        result.current.updateDebouncedSnapshot(
          [{ ...initialSnapshot.nodes[0], data: { label: 'AB' } }],
          [],
          initialSnapshot.agentsMeta,
        );
      });

      act(() => {
        result.current.updateDebouncedSnapshot(
          [{ ...initialSnapshot.nodes[0], data: { label: 'ABC' } }],
          [],
          initialSnapshot.agentsMeta,
        );
      });

      // Advance time to trigger debounce commit
      act(() => {
        vi.advanceTimersByTime(500);
      });

      expect(result.current.canUndo).toBe(true);

      let reverted;
      act(() => {
        reverted = result.current.undo();
      });

      // Should revert all the way back to initial label 'Topic 1' in ONE undo step
      expect(reverted.nodes[0].data.label).toBe('Topic 1');
      expect(result.current.canUndo).toBe(false);

      let redone;
      act(() => {
        redone = result.current.redo();
      });
      expect(redone.nodes[0].data.label).toBe('ABC');
    });

    test('immediate undo before debounce timer finishes commits and reverts properly', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      act(() => {
        result.current.updateDebouncedSnapshot(
          [{ ...initialSnapshot.nodes[0], data: { label: 'Quick Change' } }],
          [],
          initialSnapshot.agentsMeta,
        );
      });

      // User hits undo immediately (no timer advance)
      let reverted;
      act(() => {
        reverted = result.current.undo();
      });

      expect(reverted.nodes[0].data.label).toBe('Topic 1');
      expect(result.current.canUndo).toBe(false);
      expect(result.current.canRedo).toBe(true);
    });

    test('startDrag and stopDrag records move only when position changed', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      // Start drag at (0, 0)
      act(() => {
        result.current.startDrag(initialSnapshot);
      });

      // Stop drag without position change
      act(() => {
        result.current.stopDrag(initialSnapshot);
      });
      expect(result.current.canUndo).toBe(false);

      // Start drag again and stop with new position
      const movedSnapshot = {
        ...initialSnapshot,
        nodes: [{ ...initialSnapshot.nodes[0], position: { x: 200, y: 150 } }],
      };

      act(() => {
        result.current.startDrag(initialSnapshot);
      });
      act(() => {
        result.current.stopDrag(movedSnapshot);
      });

      expect(result.current.canUndo).toBe(true);

      let reverted;
      act(() => {
        reverted = result.current.undo();
      });
      expect(reverted.nodes[0].position).toEqual({ x: 0, y: 0 });
    });

    test('setBaseline updates present snapshot without modifying history', () => {
      const { result } = renderHook(() => useDiagramHistory(initialSnapshot));

      const enriched = {
        ...initialSnapshot,
        nodes: [{ ...initialSnapshot.nodes[0], data: { ...initialSnapshot.nodes[0].data, enriched: true } }],
      };

      act(() => {
        result.current.setBaseline(enriched);
      });

      expect(result.current.canUndo).toBe(false);
      expect(result.current.presentRef.current).toEqual(enriched);
    });

    test('deleting a node and its edges in a delete cycle produces a single undo step restoring both', () => {
      const graphSnapshot = {
        nodes: [
          { id: 'topic-1', type: 'topic', position: { x: 0, y: 0 }, data: { label: 'T1' } },
          { id: 'agent-1', type: 'agent', position: { x: 100, y: 100 }, data: { label: 'A1' } },
          { id: 'outbound-1', type: 'outbound', position: { x: 200, y: 200 }, data: { label: 'O1' } },
        ],
        edges: [
          { id: 'edge-in', source: 'topic-1', target: 'agent-1' },
          { id: 'edge-out', source: 'agent-1', target: 'outbound-1' },
        ],
        agentsMeta: { name: 'App' },
      };

      const { result } = renderHook(() => useDiagramHistory(graphSnapshot));

      // 1. React Flow fires node removal for agent-1 (which filters out agent-1 and all edges connected to it)
      const nextNodes = graphSnapshot.nodes.filter(n => n.id !== 'agent-1');
      const nextEdges = graphSnapshot.edges.filter(e => e.source !== 'agent-1' && e.target !== 'agent-1');

      act(() => {
        result.current.pushSnapshot(nextNodes, nextEdges, graphSnapshot.agentsMeta);
      });

      // 2. React Flow also fires edge removal for edge-in and edge-out
      act(() => {
        const afterEdges = result.current.presentRef.current.edges.filter(
          e => e.id !== 'edge-in' && e.id !== 'edge-out',
        );
        result.current.pushSnapshot(result.current.presentRef.current.nodes, afterEdges, graphSnapshot.agentsMeta);
      });

      // Exactly ONE undo step should exist
      let restored;
      act(() => {
        restored = result.current.undo();
      });

      // Both the agent node and both incoming/outgoing edges are restored in one go
      expect(restored.nodes).toHaveLength(3);
      expect(restored.edges).toHaveLength(2);
      expect(restored.edges.map(e => e.id)).toEqual(['edge-in', 'edge-out']);
      expect(result.current.canUndo).toBe(false);
      expect(result.current.canRedo).toBe(true);

      // Redo deletes both node and edges in one go
      let redone;
      act(() => {
        redone = result.current.redo();
      });
      expect(redone.nodes).toHaveLength(2);
      expect(redone.edges).toHaveLength(0);
    });
  });
});
