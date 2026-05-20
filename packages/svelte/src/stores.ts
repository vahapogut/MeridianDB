/**
 * MeridianDB Svelte 5 Stores
 *
 * Svelte 5 runes-based reactive stores for MeridianDB sync engine.
 *
 * Usage (Svelte 5 .svelte file):
 * ```svelte
 * <script>
 * import { useQuery, useMutation } from 'meridian-svelte';
 * const todos = useQuery(db.todos.find());
 * const { put, update, remove } = useMutation(db.todos);
 * </script>
 * ```
 */

import type { MeridianClient, Query, CollectionProxy, LiveQueryOptions } from 'meridian-client';

// ─── useQuery ─────────────────────────────────────────────────────────────

export function useQuery<T>(query: Query<T>) {
  let data = $state<T | undefined>(undefined);

  $effect(() => {
    const unsub = query.subscribe((result) => {
      data = result;
    });
    return unsub;
  });

  return { get value() { return data; } };
}

// ─── useLiveQuery ─────────────────────────────────────────────────────────

export function useLiveQuery(collection: CollectionProxy, options: LiveQueryOptions = {}) {
  let data = $state<Record<string, unknown>[] | undefined>(undefined);

  $effect(() => {
    // Read options properties inside the effect to trigger Svelte 5 reactive tracking
    options.orderBy;
    options.limit;
    if (options.where) {
      JSON.stringify(options.where);
    }

    const query = collection.live(options);
    const unsub = query.subscribe((result) => {
      data = result;
    });
    return unsub;
  });

  return { get value() { return data; } };
}

// ─── useDoc ───────────────────────────────────────────────────────────────

export function useDoc(collection: CollectionProxy, getDocId: () => string | null) {
  let doc = $state<Record<string, unknown> | null | undefined>(undefined);

  $effect(() => {
    const docId = getDocId();
    if (!docId) { doc = null; return; }
    const unsub = collection.findOne(docId).subscribe((result) => {
      doc = result;
    });
    return unsub;
  });

  return { get value() { return doc; } };
}

// ─── useSync ──────────────────────────────────────────────────────────────

export function useSync(client: MeridianClient) {
  let connected = $state(false);
  let pendingCount = $state(0);
  let lastSync = $state<Date | null>(null);

  $effect(() => {
    const unsub = client.onSyncChange((state) => {
      connected = state.connected;
      pendingCount = state.pendingCount;
      lastSync = state.lastSync;
    });
    return unsub;
  });

  return {
    get connected() { return connected; },
    get pendingCount() { return pendingCount; },
    get lastSync() { return lastSync; },
  };
}

// ─── usePresence ─────────────────────────────────────────────────────────

export function usePresence(client: MeridianClient) {
  let peers = $state<Record<string, Record<string, unknown>>>({});

  $effect(() => {
    return client.presence.subscribe((p) => {
      peers = { ...p };
    });
  });

  return { get peers() { return peers; } };
}

// ─── useMutation ─────────────────────────────────────────────────────────

export function useMutation(collection: CollectionProxy) {
  return {
    put: (doc: Record<string, unknown>) => collection.put(doc),
    update: (id: string, fields: Record<string, unknown>) => collection.update(id, fields),
    remove: (id: string) => collection.delete(id),
  };
}
