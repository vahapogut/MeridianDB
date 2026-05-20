/**
 * Meridian Vue 3 Composables
 *
 * Drop-in composables for Vue 3 apps using Meridian sync engine.
 *
 * Usage:
 * ```vue
 * <script setup>
 * import { useQuery, useMutation } from 'meridian-vue';
 *
 * const todos = useQuery(db.todos.find());
 * const { put, update, remove } = useMutation(db.todos);
 * </script>
 * ```
 */

import { ref, watch, onMounted, onUnmounted, shallowRef, type Ref } from 'vue';
import type { MeridianClient, Query, CollectionProxy, LiveQueryOptions } from 'meridian-client';

// ─── useQuery ──────────────────────────────────────────────────────────────

export function useQuery<T>(query: Query<T>): Ref<T | undefined> {
  const data = shallowRef<T | undefined>(undefined);

  onMounted(() => {
    const unsub = query.subscribe((result) => {
      data.value = result;
    });
    onUnmounted(unsub);
  });

  return data;
}

// ─── useLiveQuery ──────────────────────────────────────────────────────────

export function useLiveQuery(
  collection: CollectionProxy,
  options: LiveQueryOptions = {}
): Ref<Record<string, unknown>[] | undefined> {
  const data = shallowRef<Record<string, unknown>[] | undefined>(undefined);

  watch(
    () => [options.where, options.orderBy, options.limit],
    (newVal, oldVal, onCleanup) => {
      const query = collection.live(options);
      const unsub = query.subscribe((result) => {
        data.value = result;
      });
      onCleanup(unsub);
    },
    { immediate: true }
  );

  return data;
}

// ─── useDoc ────────────────────────────────────────────────────────────────

export function useDoc(
  collection: CollectionProxy,
  docId: Ref<string | null>
): Ref<Record<string, unknown> | null | undefined> {
  const doc = shallowRef<Record<string, unknown> | null | undefined>(undefined);

  watch(docId, (id, _oldId, onCleanup) => {
    if (!id) { doc.value = null; return; }
    const unsub = collection.findOne(id).subscribe((result) => {
      doc.value = result;
    });
    onCleanup(unsub);
  }, { immediate: true });

  return doc;
}

// ─── useSync ───────────────────────────────────────────────────────────────

export function useSync(client: MeridianClient) {
  const connected = ref(false);
  const pendingCount = ref(0);
  const lastSync = ref<Date | null>(null);

  onMounted(() => {
    const unsub = client.onSyncChange((state) => {
      connected.value = state.connected;
      pendingCount.value = state.pendingCount;
      lastSync.value = state.lastSync;
    });
    onUnmounted(unsub);
  });

  return { connected, pendingCount, lastSync };
}

// ─── usePresence ───────────────────────────────────────────────────────────

export function usePresence(client: MeridianClient): Ref<Record<string, Record<string, unknown>>> {
  const peers = ref<Record<string, Record<string, unknown>>>({});

  onMounted(() => {
    return client.presence.subscribe((p) => {
      peers.value = { ...p };
    });
  });

  return peers;
}

// ─── useMutation ───────────────────────────────────────────────────────────

export function useMutation(collection: CollectionProxy) {
  const put = (doc: Record<string, unknown>) => collection.put(doc);
  const update = (id: string, fields: Record<string, unknown>) => collection.update(id, fields);
  const remove = (id: string) => collection.delete(id);

  return { put, update, remove };
}
