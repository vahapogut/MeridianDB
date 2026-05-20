/**
 * Meridian Client — IndexedDB Store
 *
 * Persistent storage layer using IndexedDB (via `idb` library).
 * Manages three object stores:
 * - Per-collection stores: Documents with CRDT metadata
 * - `_meridian_meta`: CRDT metadata (HLC per field) per document
 * - `_meridian_pending`: Pending operations queue for offline sync
 */

import { openDB, type IDBPDatabase } from 'idb';
import {
  type LWWMap,
  type CRDTOperation,
  type PendingOp,
  type SchemaDefinition,
  type CollectionSchema,
  createLWWMap,
  mergeLWWMaps,
  extractValues,
  extractMetadata,
  reconstructLWWMap,
  isDeleted,
  getLatestHLC,
  DELETED_FIELD,
  getDefaults,
  encryptFields,
  decryptFields,
} from 'meridian-shared';

const DB_NAME_PREFIX = 'meridian';
const META_STORE = '_meridian_meta';
const PENDING_STORE = '_meridian_pending';
const SYNC_STATE_STORE = '_meridian_sync';

/** Check if error is an IndexedDB quota exceeded error */
function isQuotaError(err: unknown): boolean {
  if (err instanceof DOMException) {
    return err.name === 'QuotaExceededError' ||
           err.name === 'UnknownError' && err.message.includes('quota');
  }
  return false;
}

/** Wrapper for IndexedDB operations that handles quota and common errors */
function wrapIDBError(err: unknown, operation: string): never {
  if (isQuotaError(err)) {
    throw new Error(
      `[Meridian Store] Storage quota exceeded during ${operation}. ` +
      `Clear some browser storage or increase the quota.`
    );
  }
  throw new Error(
    `[Meridian Store] IndexedDB error during ${operation}: ${String(err)}`
  );
}

export interface EncryptionConfig {
  /** AES-256-GCM CryptoKey for field-level encryption */
  key: CryptoKey;
  /** Fields to encrypt before IndexedDB storage */
  fields: string[];
}

export interface StoreConfig {
  /** Database name (derived from server URL or custom) */
  dbName: string;
  /** Optional E2E encryption config */
  encryption?: EncryptionConfig;
  /** Schema definition */
  schema: SchemaDefinition;
  /** Node ID for this client */
  nodeId: string;
  /** Optional callback for local store changes to broadcast to other tabs */
  onLocalChange?: (collection: string, docId: string) => void;
}

export interface DocWithMeta {
  /** Plain document values */
  doc: Record<string, unknown>;
  /** CRDT metadata map */
  crdtMap: LWWMap;
}

/**
 * IndexedDB-backed store for Meridian client.
 *
 * Handles:
 * - Document CRUD with CRDT metadata
 * - Pending operations queue (offline writes)
 * - Sync state persistence (last known seqNum)
 */
export class MeridianStore {
  private db: IDBPDatabase | null = null;
  private initPromise: Promise<void> | null = null;
  public readonly config: StoreConfig;
  private changeListeners: Map<string, Set<(docId: string) => void>> = new Map();
  private pendingOpsChangeCallbacks: Set<() => void> = new Set();

  constructor(config: StoreConfig) {
    this.config = config;
  }

  /** Encrypt sensitive fields before IndexedDB storage */
  private async encryptDoc(doc: Record<string, unknown>): Promise<Record<string, unknown>> {
    const enc = this.config.encryption;
    if (!enc || enc.fields.length === 0) return doc;
    return encryptFields(doc, enc.key, enc.fields);
  }

  /** Decrypt sensitive fields after IndexedDB read */
  private async decryptDoc(doc: Record<string, unknown>): Promise<Record<string, unknown>> {
    const enc = this.config.encryption;
    if (!enc || enc.fields.length === 0) return doc;
    return decryptFields(doc, enc.key, enc.fields);
  }

  /**
   * Set callback for local store changes to notify other tabs
   */
  public setOnLocalChange(fn: (collection: string, docId: string) => void): void {
    this.config.onLocalChange = fn;
  }

  /**
   * Initialize IndexedDB — creates/upgrades stores as needed.
   */
  async init(): Promise<void> {
    if (this.initPromise) return this.initPromise;

    const { dbName, schema } = this.config;
    const collectionNames = Object.keys(schema.collections);

    this.initPromise = (async () => {
      try {
        this.db = await openDB(`${DB_NAME_PREFIX}_${dbName}`, schema.version, {
          upgrade(db, oldVersion, newVersion) {
            // Create collection stores
            for (const name of collectionNames) {
              if (!db.objectStoreNames.contains(name)) {
                const store = db.createObjectStore(name, { keyPath: 'id' });
                // Create index for every field except id to enable O(1) finds
                const schemaDef = schema.collections[name];
                if (schemaDef) {
                  for (const field of Object.keys(schemaDef)) {
                    if (field !== 'id') store.createIndex(field, field);
                  }
                }
              }
            }

            // Create metadata store
            if (!db.objectStoreNames.contains(META_STORE)) {
              db.createObjectStore(META_STORE);
            }

            // Create pending ops store
            if (!db.objectStoreNames.contains(PENDING_STORE)) {
              const store = db.createObjectStore(PENDING_STORE, { keyPath: 'id' });
              store.createIndex('status', 'status');
              store.createIndex('createdAt', 'createdAt');
            }

            // Create sync state store
            if (!db.objectStoreNames.contains(SYNC_STATE_STORE)) {
              db.createObjectStore(SYNC_STATE_STORE);
            }
          },
        });
      } catch (err) {
        wrapIDBError(err, 'init');
      }
    })();

    return this.initPromise;
  }

  private async ensureReady(): Promise<IDBPDatabase> {
    if (!this.initPromise) {
      await this.init();
    } else {
      await this.initPromise;
    }
    if (!this.db) {
      throw new Error('[Meridian Store] Database not initialized.');
    }
    return this.db;
  }

  // ─── Document Operations ───────────────────────────────────────────────────

  /**
   * Get a single document by ID.
   * Returns null if not found or soft-deleted.
   */
  async getDoc(collection: string, docId: string): Promise<Record<string, unknown> | null> {
    const db = await this.ensureReady();
    const doc = await db.get(collection, docId);
    if (!doc) return null;

    // Check if soft-deleted
    const meta = await this.getMeta(collection, docId);
    if (meta && isDeleted(meta)) return null;

    return this.decryptDoc(doc);
  }

  /**
   * Get a document with its CRDT metadata.
   */
  async getDocWithMeta(collection: string, docId: string): Promise<DocWithMeta | null> {
    const db = await this.ensureReady();
    const doc = await db.get(collection, docId);
    if (!doc) return null;

    const meta = await this.getMeta(collection, docId);
    if (!meta) return null;

    return { doc, crdtMap: meta };
  }

  /**
   * Put a document (create or update).
   * Writes both the document and CRDT metadata.
   *
   * @returns The pending operation created
   */
  async putDoc(
    collection: string,
    doc: Record<string, unknown>,
    hlc: string,
    nodeId: string
  ): Promise<PendingOp[]> {
    const db = await this.ensureReady();
    const docId = doc.id as string;
    const schema = this.config.schema.collections[collection];
    const pendingOps: PendingOp[] = [];

    const clonedDoc = { ...doc };

    // Apply defaults from schema
    if (schema) {
      const defaults = getDefaults(schema);
      for (const [field, defaultValue] of Object.entries(defaults)) {
        if (!(field in clonedDoc) || clonedDoc[field] === undefined) {
          clonedDoc[field] = defaultValue;
        }
      }
    }

    // Encrypt sensitive fields before IndexedDB storage
    const docToStore = await this.encryptDoc(clonedDoc);

    // Get existing metadata for rollback
    const existingMeta = await this.getMeta(collection, docId);

    // Create new CRDT map (encrypted for storage, unencrypted for sync)
    const newMap = createLWWMap(docToStore, hlc, nodeId);

    // Merge with existing if present
    let finalMap: LWWMap;
    if (existingMeta) {
      const { merged } = mergeLWWMaps(existingMeta, newMap);
      finalMap = merged;
    } else {
      finalMap = newMap;
    }

    // Write document + metadata in a transaction
    const tx = db.transaction([collection, META_STORE, PENDING_STORE], 'readwrite');
    const values = extractValues(finalMap);
    values.id = docId;
    await tx.objectStore(collection).put(values);
    await tx.objectStore(META_STORE).put(finalMap, `${collection}:${docId}`);

    // Create pending ops for each field
    for (const [field, value] of Object.entries(clonedDoc)) {
      if (field === 'id') continue;

      const previousValue = existingMeta?.[field]?.value ?? null;
      const previousHlc = existingMeta?.[field]?.hlc ?? null;

      const pendingOp: PendingOp = {
        id: `${docId}-${field}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        op: {
          id: `${docId}-${field}-${hlc}`,
          collection,
          docId,
          field,
          value,
          hlc,
          nodeId,
        },
        previousValue,
        previousHlc,
        status: 'pending',
        createdAt: Date.now(),
        retries: 0,
      };

      await tx.objectStore(PENDING_STORE).put(pendingOp);
      pendingOps.push(pendingOp);
    }

    await tx.done;
    this.notifyPendingOpsChange();

    // Notify listeners
    this.notifyChange(collection, docId);

    return pendingOps;
  }

  /**
   * Update specific fields of a document.
   */
  async updateDoc(
    collection: string,
    docId: string,
    fields: Record<string, unknown>,
    hlc: string,
    nodeId: string
  ): Promise<PendingOp[]> {
    const existing = await this.getDoc(collection, docId);
    if (!existing) {
      throw new Error(`[Meridian Store] Document "${docId}" not found in "${collection}".`);
    }

    // Merge existing with updates
    const merged = { ...existing, ...fields, id: docId };
    return this.putDoc(collection, merged, hlc, nodeId);
  }

  /**
   * Soft-delete a document (tombstone).
   */
  async deleteDoc(
    collection: string,
    docId: string,
    hlc: string,
    nodeId: string
  ): Promise<PendingOp[]> {
    const db = await this.ensureReady();
    const existingMeta = await this.getMeta(collection, docId);

    if (!existingMeta) {
      return []; // Nothing to delete
    }

    // Set __deleted flag
    const deleteMap = createLWWMap({ [DELETED_FIELD]: true }, hlc, nodeId);
    const { merged } = mergeLWWMaps(existingMeta, deleteMap);

    // Write
    const tx = db.transaction([collection, META_STORE, PENDING_STORE], 'readwrite');
    await tx.objectStore(META_STORE).put(merged, `${collection}:${docId}`);
    await tx.objectStore(collection).delete(docId); // Remove from primary store

    const pendingOp: PendingOp = {
      id: `${docId}-${DELETED_FIELD}-${Date.now()}`,
      op: {
        id: `${docId}-${DELETED_FIELD}-${hlc}`,
        collection,
        docId,
        field: DELETED_FIELD,
        value: true,
        hlc,
        nodeId,
      },
      previousValue: false,
      previousHlc: existingMeta[DELETED_FIELD]?.hlc ?? null,
      status: 'pending',
      createdAt: Date.now(),
      retries: 0,
    };

    await tx.objectStore(PENDING_STORE).put(pendingOp);
    await tx.done;
    this.notifyPendingOpsChange();

    this.notifyChange(collection, docId);

    return [pendingOp];
  }

  /**
   * Query all non-deleted documents in a collection.
   * Supports simple field-value filtering.
   */
  async queryDocs(
    collection: string,
    filter?: Record<string, unknown>
  ): Promise<Record<string, unknown>[]> {
    const db = await this.ensureReady();
    
    // Check if we can optimize with an IDBIndex
    let allDocs: any[];
    const filterKeys = filter ? Object.keys(filter) : [];
    
    if (filterKeys.length === 1) {
      const key = filterKeys[0];
      const value = filter![key];
      // Note: idb throws if index doesn't exist, we check if it's in schema
      const schemaDef = this.config.schema.collections[collection];
      if (schemaDef && key in schemaDef && key !== 'id') {
        allDocs = await db.getAllFromIndex(collection, key, IDBKeyRange.only(value));
      } else if (key === 'id') {
        const doc = await db.get(collection, value as string);
        allDocs = doc ? [doc] : [];
      } else {
        allDocs = await db.getAll(collection);
      }
    } else {
      allDocs = await db.getAll(collection);
    }

    const results: Record<string, unknown>[] = [];

    for (const doc of allDocs) {
      const docId = doc.id as string;

      // We no longer check isDeleted(meta) here because deleteDoc and 
      // applyRemoteChanges both remove tombstoned documents directly from the 
      // collection store. This eliminates the O(N) getMeta transactions!

      // Apply filter
      if (filter) {
        let matches = true;
        for (const [key, value] of Object.entries(filter)) {
          if (doc[key] !== value) {
            matches = false;
            break;
          }
        }
        if (!matches) continue;
      }

      results.push(await this.decryptDoc(doc));
    }

    return results;
  }

  /**
   * Apply remote changes from server to local store.
   * Performs CRDT merge — only accepts changes with higher HLC.
   */
  async applyRemoteChanges(ops: CRDTOperation[]): Promise<string[]> {
    const db = await this.ensureReady();
    const affectedDocs: Set<string> = new Set();

    // Group ops by collection:docId
    const grouped = new Map<string, CRDTOperation[]>();
    for (const op of ops) {
      const key = `${op.collection}:${op.docId}`;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push(op);
    }

    for (const [key, docOps] of grouped) {
      const firstColon = key.indexOf(':');
      const collection = key.slice(0, firstColon);
      const docId = key.slice(firstColon + 1);

      // Build remote LWW-Map from ops
      const remoteMap: LWWMap = {};
      for (const op of docOps) {
        remoteMap[op.field] = {
          value: op.value,
          hlc: op.hlc,
          nodeId: op.nodeId,
        };
      }

      // Merge with existing
      const existingMeta = await this.getMeta(collection, docId);
      let finalMap: LWWMap;

      if (existingMeta) {
        const { merged } = mergeLWWMaps(existingMeta, remoteMap);
        finalMap = merged;
      } else {
        finalMap = remoteMap;
      }

      // Write if not deleted
      const tx = db.transaction([collection, META_STORE], 'readwrite');

      if (!isDeleted(finalMap)) {
        let values = extractValues(finalMap);
        values.id = docId;
        // Encrypt sensitive fields from server before IndexedDB storage
        values = await this.encryptDoc(values);
        await tx.objectStore(collection).put(values);
      } else {
        // Remove from collection store if deleted
        await tx.objectStore(collection).delete(docId);
      }

      await tx.objectStore(META_STORE).put(finalMap, key);
      await tx.done;

      affectedDocs.add(key);
      this.notifyChange(collection, docId, true);
    }

    return Array.from(affectedDocs);
  }

  // ─── CRDT Metadata ─────────────────────────────────────────────────────────

  private async getMeta(collection: string, docId: string): Promise<LWWMap | null> {
    const db = await this.ensureReady();
    return (await db.get(META_STORE, `${collection}:${docId}`)) ?? null;
  }

  // ─── Pending Operations ────────────────────────────────────────────────────

  /**
   * Get all pending operations (not yet acknowledged by server).
   */
  async getPendingOps(): Promise<PendingOp[]> {
    const db = await this.ensureReady();
    const all = await db.getAll(PENDING_STORE);
    return all.filter(op => op.status === 'pending' || op.status === 'sending');
  }

  /**
   * Mark operations as sending (in-flight to server).
   */
  async markOpsSending(opIds: string[]): Promise<void> {
    const db = await this.ensureReady();
    const tx = db.transaction(PENDING_STORE, 'readwrite');

    for (const id of opIds) {
      const op = await tx.store.get(id);
      if (op) {
        op.status = 'sending';
        op.retries++;
        await tx.store.put(op);
      }
    }

    await tx.done;
    this.notifyPendingOpsChange();
  }

  /**
   * Acknowledge operations (server confirmed persistence).
   */
  async ackOps(opIds: string[]): Promise<void> {
    const db = await this.ensureReady();
    const tx = db.transaction(PENDING_STORE, 'readwrite');

    for (const id of opIds) {
      await tx.store.delete(id);
    }

    await tx.done;
    this.notifyPendingOpsChange();
  }

  /**
   * Rollback rejected operations to their previous values.
   */
  async rollbackOp(opId: string): Promise<PendingOp | null> {
    const db = await this.ensureReady();
    const pendingOp = await db.get(PENDING_STORE, opId);

    if (!pendingOp) return null;

    const { collection, docId, field } = pendingOp.op;

    if (pendingOp.previousHlc && pendingOp.previousValue !== null) {
      // Restore previous value
      const meta = await this.getMeta(collection, docId);
      if (meta && meta[field]) {
        meta[field] = {
          value: pendingOp.previousValue,
          hlc: pendingOp.previousHlc,
          nodeId: meta[field].nodeId,
        };

        const tx = db.transaction([collection, META_STORE, PENDING_STORE], 'readwrite');
        await tx.objectStore(META_STORE).put(meta, `${collection}:${docId}`);

        const values = extractValues(meta);
        values.id = docId;
        await tx.objectStore(collection).put(values);
        await tx.objectStore(PENDING_STORE).delete(opId);
        await tx.done;
        this.notifyPendingOpsChange();

        this.notifyChange(collection, docId);
      }
    } else {
      // No previous value — just remove the pending op
      await db.delete(PENDING_STORE, opId);
      this.notifyPendingOpsChange();
    }

    return pendingOp;
  }

  /**
   * Reset all pending ops to 'pending' status (e.g., on reconnect).
   */
  async resetPendingStatus(): Promise<void> {
    const db = await this.ensureReady();
    const all = await db.getAll(PENDING_STORE);
    const tx = db.transaction(PENDING_STORE, 'readwrite');

    for (const op of all) {
      if (op.status === 'sending') {
        op.status = 'pending';
        await tx.store.put(op);
      }
    }

    await tx.done;
    this.notifyPendingOpsChange();
  }

  // ─── Sync State ─────────────────────────────────────────────────────────────

  /**
   * Get the last known server sequence number.
   */
  async getLastSeq(): Promise<number> {
    const db = await this.ensureReady();
    return (await db.get(SYNC_STATE_STORE, 'lastSeq')) ?? 0;
  }

  /**
   * Update the last known server sequence number.
   */
  async setLastSeq(seq: number): Promise<void> {
    const db = await this.ensureReady();
    await db.put(SYNC_STATE_STORE, seq, 'lastSeq');
  }

  // ─── Full Reset ─────────────────────────────────────────────────────────────

  /**
   * Clear all data (used for full re-sync after compaction gap).
   */
  async clearAll(): Promise<void> {
    const db = await this.ensureReady();
    const storeNames = Array.from(db.objectStoreNames);
    const tx = db.transaction(storeNames, 'readwrite');

    for (const name of storeNames) {
      await tx.objectStore(name).clear();
    }

    await tx.done;
    this.notifyPendingOpsChange();
  }

  // ─── Change Listeners ──────────────────────────────────────────────────────

  onPendingOpsChange(cb: () => void): () => void {
    this.pendingOpsChangeCallbacks.add(cb);
    return () => this.pendingOpsChangeCallbacks.delete(cb);
  }

  private notifyPendingOpsChange(): void {
    for (const cb of this.pendingOpsChangeCallbacks) {
      try {
        cb();
      } catch (e) {
        console.error('[Meridian Store] Pending ops change listener error:', e);
      }
    }
  }

  /**
   * Register a listener for document changes in a collection.
   */
  onCollectionChange(collection: string, listener: (docId: string) => void): () => void {
    if (!this.changeListeners.has(collection)) {
      this.changeListeners.set(collection, new Set());
    }
    this.changeListeners.get(collection)!.add(listener);

    // Return unsubscribe function
    return () => {
      this.changeListeners.get(collection)?.delete(listener);
    };
  }

  public notifyChange(collection: string, docId: string, isRemote = false): void {
    const listeners = this.changeListeners.get(collection);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(docId);
        } catch (e) {
          console.error('[Meridian Store] Change listener error:', e);
        }
      }
    }

    // Broadcast local changes to other tabs
    if (!isRemote && this.config.onLocalChange) {
      this.config.onLocalChange(collection, docId);
    }
  }

  // ─── Cleanup ───────────────────────────────────────────────────────────────

  /**
   * Close the database connection.
   */
  close(): void {
    this.db?.close();
    this.db = null;
    this.changeListeners.clear();
  }
}
