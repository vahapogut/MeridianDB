/**
 * Meridian React Hooks
 *
 * Drop-in hooks for React apps using Meridian sync engine.
 * No useEffect boilerplate needed — hooks handle subscriptions
 * and cleanup automatically.
 *
 * Usage:
 * ```tsx
 * const todos = useQuery(db.todos.find());
 * const liveTodos = useLiveQuery(db.todos, { where: { done: false } });
 * const todo = useDoc(db.todos, id);
 * ```
 */

import { useState, useEffect, useCallback, useRef, useSyncExternalStore } from 'react';
import type { MeridianClient, Query, CollectionProxy, LiveQueryOptions } from 'meridian-client';

// ─── useStore (internal) ────────────────────────────────────────────────────

/**
 * Minimal React 18+ external store adapter.
 * For React 19 with useSyncExternalStore built-in, this just wraps the subscribe pattern.
 */
function useStore<T>(subscribe: (cb: () => void) => () => void, getSnapshot: () => T): T {
  return useSyncExternalStore(subscribe, getSnapshot);
}

// ─── useQuery ───────────────────────────────────────────────────────────────

/**
 * Subscribe to a reactive query.
 *
 * ```tsx
 * const todos = useQuery(db.todos.find());
 * const openIssues = useQuery(db.issues.find({ status: 'open' }));
 * ```
 */
export function useQuery<T>(query: Query<T>): T | undefined {
  const [data, setData] = useState<T>();

  useEffect(() => {
    let mounted = true;
    const unsub = query.subscribe((result) => {
      if (mounted) setData(result);
    });
    return () => { unsub(); mounted = false; };
  }, []);

  return data;
}

// ─── useLiveQuery ───────────────────────────────────────────────────────────

/**
 * V2: Subscribe to a live query with ordering and limit.
 *
 * ```tsx
 * const recentTodos = useLiveQuery(db.todos, {
 *   where: { done: false },
 *   orderBy: 'createdAt',
 *   limit: 50,
 * });
 * ```
 */
export function useLiveQuery(
  collection: CollectionProxy,
  options: LiveQueryOptions = {}
): Record<string, unknown>[] | undefined {
  const queryRef = useRef<Query<Record<string, unknown>[]>>();
  const [data, setData] = useState<Record<string, unknown[]>>();
  // Destructure to primitive values for stable dependency comparison
  const { where: _w, orderBy: _ob, limit: _lim } = options;
  const depKey = `${_ob || ''}-${_lim || 0}-${Object.entries(_w || {}).sort().join(',')}`;

  useEffect(() => {
    const query = collection.live(options);
    queryRef.current = query;
    let mounted = true;
    const unsub = query.subscribe((result) => {
      if (mounted) setData(result);
    });
    return () => { unsub(); mounted = false; };
  }, [depKey]);

  return data;
}

// ─── useDoc ─────────────────────────────────────────────────────────────────

/**
 * Subscribe to a single document by ID.
 *
 * ```tsx
 * const todo = useDoc(db.todos, 'todo-123');
 * if (!todo) return <p>Not found</p>;
 * return <p>{todo.title}</p>;
 * ```
 */
export function useDoc(
  collection: CollectionProxy,
  docId: string | null
): Record<string, unknown> | null | undefined {
  const [doc, setDoc] = useState<Record<string, unknown> | null>();

  useEffect(() => {
    if (!docId) { setDoc(null); return; }
    let mounted = true;
    const unsub = collection.findOne(docId).subscribe((result) => {
      if (mounted) setDoc(result);
    });
    return () => { unsub(); mounted = false; };
  }, [docId]);

  return doc;
}

// ─── useSync ────────────────────────────────────────────────────────────────

/**
 * Access sync engine state: connection status, pending ops, sync time.
 *
 * ```tsx
 * const { connected, pendingCount, lastSync } = useSync(db);
 * ```
 */
export function useSync(client: MeridianClient) {
  const [state, setState] = useState({
    connected: false,
    pendingCount: 0,
    lastSync: null as Date | null,
  });

  useEffect(() => {
    const unsub = client.onSyncChange((nextState) => {
      setState(nextState);
    });
    return unsub;
  }, [client]);

  return {
    connected: state.connected,
    pendingCount: state.pendingCount,
    lastSync: state.lastSync,
  };
}

// ─── usePresence ────────────────────────────────────────────────────────────

/**
 * Subscribe to peer presence for collaborative cursors/avatars.
 *
 * ```tsx
 * const peers = usePresence(db);
 * {Object.entries(peers).map(([id, data]) => (
 *   <Cursor key={id} x={data.x} y={data.y} />
 * ))}
 * ```
 */
export function usePresence(client: MeridianClient): Record<string, Record<string, unknown>> {
  const [peers, setPeers] = useState<Record<string, Record<string, unknown>>>({});

  useEffect(() => {
    return client.presence.subscribe((p) => setPeers({ ...p }));
  }, []);

  return peers;
}

// ─── useQueryOptimized (Fine-Grained Reactivity) ────────────────────────────

/**
 * Fine-grained reactive query — only re-renders when individual documents change.
 *
 * Uses a stable Map keyed by document ID. Components that read from this Map
 * via useDoc() will only re-render when their specific document changes,
 * not the entire collection.
 *
 * ```tsx
 * const docs = useQueryOptimized(db.todos.find());
 * // docs is a Map<string, Record<string, unknown>>
 * // Only components reading docs.get('specific-id') will re-render on change
 * ```
 */
export function useQueryOptimized(
  query: Query<Record<string, unknown>[]>
): Map<string, Record<string, unknown>> {
  const [docMap, setDocMap] = useState<Map<string, Record<string, unknown>>>(new Map());

  useEffect(() => {
    let mounted = true;
    const unsub = query.subscribe((docs) => {
      if (!mounted) return;
      setDocMap(prev => {
        const next = new Map(prev);
        const newIds = new Set<string>();

        // Add/update changed docs
        for (const doc of docs) {
          const id = doc.id as string;
          newIds.add(id);
          const existing = prev.get(id);
          if (!existing || !shallowEqual(existing, doc)) {
            next.set(id, { ...doc });
          }
        }

        // Remove deleted docs
        for (const id of prev.keys()) {
          if (!newIds.has(id)) {
            next.delete(id);
          }
        }

        // Skip re-render if nothing changed
        if (next.size === prev.size) {
          let allSame = true;
          for (const [id, doc] of next) {
            if (prev.get(id) !== doc) { allSame = false; break; }
          }
          if (allSame) return prev;
        }

        return next;
      });
    });
    return () => { unsub(); mounted = false; };
  }, []);

  return docMap;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function shallowEqual(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const key of keysA) {
    if (a[key] !== b[key]) return false;
  }
  return true;
}

// ─── useMutation ────────────────────────────────────────────────────────────

/**
 * Get mutation functions for a collection.
 *
 * ```tsx
 * const { put, update, remove } = useMutation(db.todos);
 * await put({ id: '1', title: 'Task' });
 * await update('1', { done: true });
 * await remove('1');
 * ```
 */
export function useMutation(collection: CollectionProxy) {
  const put = useCallback(
    (doc: Record<string, unknown>) => collection.put(doc),
    []
  );
  const update = useCallback(
    (id: string, fields: Record<string, unknown>) => collection.update(id, fields),
    []
  );
  const remove = useCallback(
    (id: string) => collection.delete(id),
    []
  );

  return { put, update, remove };
}
