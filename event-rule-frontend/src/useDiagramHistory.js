import { useCallback, useRef, useState } from 'react';

export const MAX_HISTORY = 50;

/** Deep clone that preserves File and Blob instances. */
export function deepClone(obj) {
  if (obj === null || typeof obj !== 'object') return obj;
  if (typeof File !== 'undefined' && obj instanceof File) return obj;
  if (typeof Blob !== 'undefined' && obj instanceof Blob) return obj;
  if (Array.isArray(obj)) return obj.map(deepClone);
  const copy = {};
  for (const [k, v] of Object.entries(obj)) {
    copy[k] = deepClone(v);
  }
  return copy;
}

/** Structural equality check between two diagram snapshots. */
export function areSnapshotsEqual(s1, s2) {
  if (s1 === s2) return true;
  if (!s1 || !s2) return false;
  return (
    JSON.stringify(s1.agentsMeta) === JSON.stringify(s2.agentsMeta) &&
    JSON.stringify(s1.edges) === JSON.stringify(s2.edges) &&
    JSON.stringify(s1.nodes) === JSON.stringify(s2.nodes)
  );
}

/**
 * Custom hook managing undo/redo history for diagram nodes, edges, and metadata.
 */
export function useDiagramHistory(initialSnapshot) {
  const [past, setPast] = useState([]);
  const [future, setFuture] = useState([]);
  const [hasPendingDebounce, setHasPendingDebounce] = useState(false);

  const pastRef = useRef([]);
  const futureRef = useRef([]);
  const presentRef = useRef(deepClone(initialSnapshot));

  const debounceTimerRef = useRef(null);
  const debounceStartSnapshotRef = useRef(null);
  const dragStartSnapshotRef = useRef(null);

  // Commit any in-progress debounced text edit before performing another action.
  const commitDebounced = useCallback(() => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = null;
    }
    if (debounceStartSnapshotRef.current) {
      const start = debounceStartSnapshotRef.current;
      debounceStartSnapshotRef.current = null;
      setHasPendingDebounce(false);
      const current = presentRef.current;
      if (!areSnapshotsEqual(start, current)) {
        const newPast = [...pastRef.current.slice(Math.max(0, pastRef.current.length - (MAX_HISTORY - 1))), start];
        pastRef.current = newPast;
        setPast(newPast);
        futureRef.current = [];
        setFuture([]);
      }
    }
  }, []);

  // Push a discrete, immediate change onto the history stack.
  const pushSnapshot = useCallback((newNodes, newEdges, newMeta) => {
    commitDebounced();

    const current = presentRef.current;
    const nextSnapshot = {
      nodes: deepClone(newNodes ?? current.nodes),
      edges: deepClone(newEdges ?? current.edges),
      agentsMeta: deepClone(newMeta ?? current.agentsMeta),
    };

    if (areSnapshotsEqual(current, nextSnapshot)) {
      return;
    }

    const newPast = [...pastRef.current.slice(Math.max(0, pastRef.current.length - (MAX_HISTORY - 1))), current];
    pastRef.current = newPast;
    setPast(newPast);

    futureRef.current = [];
    setFuture([]);

    presentRef.current = nextSnapshot;
  }, [commitDebounced]);

  // Record continuous updates (e.g. typing in text fields) batched with a debounce timer.
  const updateDebouncedSnapshot = useCallback((newNodes, newEdges, newMeta, delay = 400) => {
    if (!debounceStartSnapshotRef.current) {
      debounceStartSnapshotRef.current = presentRef.current;
      setHasPendingDebounce(true);
    }
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }

    const current = presentRef.current;
    const nextSnapshot = {
      nodes: deepClone(newNodes ?? current.nodes),
      edges: deepClone(newEdges ?? current.edges),
      agentsMeta: deepClone(newMeta ?? current.agentsMeta),
    };
    presentRef.current = nextSnapshot;

    debounceTimerRef.current = setTimeout(() => {
      commitDebounced();
    }, delay);
  }, [commitDebounced]);

  // Node drag start / stop tracking
  const startDrag = useCallback((currentSnapshot) => {
    commitDebounced();
    dragStartSnapshotRef.current = deepClone(currentSnapshot ?? presentRef.current);
  }, [commitDebounced]);

  const stopDrag = useCallback((currentSnapshot) => {
    if (!dragStartSnapshotRef.current) return;
    const start = dragStartSnapshotRef.current;
    dragStartSnapshotRef.current = null;
    const current = currentSnapshot ?? presentRef.current;
    if (!areSnapshotsEqual(start, current)) {
      const newPast = [...pastRef.current.slice(Math.max(0, pastRef.current.length - (MAX_HISTORY - 1))), start];
      pastRef.current = newPast;
      setPast(newPast);
      futureRef.current = [];
      setFuture([]);
      presentRef.current = deepClone(current);
    }
  }, []);

  // Update current baseline without affecting undo/redo stacks (e.g. after async project load)
  const setBaseline = useCallback((snapshot) => {
    presentRef.current = deepClone(snapshot);
  }, []);

  // Undo: reverts to the previous snapshot in past stack
  const undo = useCallback(() => {
    commitDebounced();
    if (pastRef.current.length === 0) return null;

    const previous = pastRef.current[pastRef.current.length - 1];
    const newPast = pastRef.current.slice(0, -1);
    const current = presentRef.current;

    pastRef.current = newPast;
    setPast(newPast);

    const newFuture = [current, ...futureRef.current];
    futureRef.current = newFuture;
    setFuture(newFuture);

    presentRef.current = deepClone(previous);
    return deepClone(previous);
  }, [commitDebounced]);

  // Redo: advances to the next snapshot in future stack
  const redo = useCallback(() => {
    commitDebounced();
    if (futureRef.current.length === 0) return null;

    const next = futureRef.current[0];
    const newFuture = futureRef.current.slice(1);
    const current = presentRef.current;

    futureRef.current = newFuture;
    setFuture(newFuture);

    const newPast = [...pastRef.current, current];
    pastRef.current = newPast;
    setPast(newPast);

    presentRef.current = deepClone(next);
    return deepClone(next);
  }, [commitDebounced]);

  const canUndo = past.length > 0 || hasPendingDebounce;
  const canRedo = future.length > 0;

  return {
    canUndo,
    canRedo,
    undo,
    redo,
    pushSnapshot,
    updateDebouncedSnapshot,
    commitDebounced,
    startDrag,
    stopDrag,
    setBaseline,
    presentRef,
  };
}
