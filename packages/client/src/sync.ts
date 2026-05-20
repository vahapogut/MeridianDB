/**
 * Meridian Client — WebSocket Sync Engine
 *
 * Handles:
 * - Transport connection with auto-reconnect (exponential backoff)
 * - Online/offline detection
 * - Push: Send pending operations to server (with E2E transit encryption)
 * - Pull: Request changes since last known seqNum
 * - Ack: Mark operations as confirmed
 * - Reject: Rollback operations
 * - Auth: Token management with auto-refresh
 */

import {
  type ClientMessage,
  type ServerMessage,
  type CRDTOperation,
  type PendingOp,
  type ConnectionState,
  type AckMessage,
  type RejectMessage,
  type ChangesMessage,
  type FullSyncRequiredMessage,
  type AuthExpiringMessage,
  type Transport,
  WebSocketTransport,
  encryptValue,
  uint8ToBase64,
  base64ToUint8,
} from 'meridian-shared';
import type { MeridianStore } from './store.js';

export interface SyncConfig {
  /** WebSocket server URL */
  serverUrl?: string;
  /** Custom transport instance */
  transport?: Transport;
  /** Store instance */
  store: MeridianStore;
  /** Auth token provider */
  auth?: { getToken: () => Promise<string> };
  /** Schema version for compatibility check */
  schemaVersion: number;
  /** Debug mode */
  debug: boolean;
  onConnectionChange?: (state: ConnectionState) => void;
  onRollback?: (op: PendingOp, reason: string) => void;
  onConflict?: (details: { field: string; localValue: unknown; remoteValue: unknown }) => void;
  onPresence?: (peers: Record<string, Record<string, unknown>>) => void;
}

const INITIAL_RETRY_DELAY = 1000;
const MAX_RETRY_DELAY = 30000;
const HEARTBEAT_INTERVAL = 25000;
const PUSH_BATCH_SIZE = 50;

/** Known server message types for validation */
const VALID_SERVER_MESSAGE_TYPES = new Set([
  'changes', 'ack', 'reject', 'presence', 'compaction',
  'full-sync-required', 'auth-expiring', 'auth-expired', 'auth-ack', 'error',
]);

/** Validate incoming server message structure */
function validateMessage(msg: unknown): msg is ServerMessage {
  if (typeof msg !== 'object' || msg === null) return false;
  const m = msg as Record<string, unknown>;
  return typeof m.type === 'string' && VALID_SERVER_MESSAGE_TYPES.has(m.type);
}

/**
 * WebSocket sync engine for Meridian client using pluggable transport.
 */
export class SyncEngine {
  private transport: Transport | null = null;
  private readonly config: SyncConfig;
  private state: ConnectionState = 'disconnected';
  private retryDelay = INITIAL_RETRY_DELAY;
  private retryTimeout: ReturnType<typeof setTimeout> | null = null;
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;
  private lastPongTime: number = Date.now();
  private pushInProgress = false;
  private destroyed = false;
  private stateListeners: Set<(state: ConnectionState) => void> = new Set();

  constructor(config: SyncConfig) {
    this.config = config;
  }

  onStateChange(cb: (state: ConnectionState) => void): () => void {
    this.stateListeners.add(cb);
    cb(this.state);
    return () => this.stateListeners.delete(cb);
  }

  // ─── Connection Management ──────────────────────────────────────────────────

  /**
   * Start the sync engine — connect to server.
   */
  async start(): Promise<void> {
    if (this.destroyed) return;

    // Listen for online/offline events
    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.handleOnline);
      window.addEventListener('offline', this.handleOffline);
    }

    await this.connect();
  }

  /**
   * Stop the sync engine — disconnect and clean up.
   */
  stop(): void {
    this.destroyed = true;
    this.clearRetry();
    this.clearHeartbeat();

    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.handleOnline);
      window.removeEventListener('offline', this.handleOffline);
    }

    if (this.transport) {
      this.transport.onClose(() => {}); // Clear listener
      this.transport.close();
      this.transport = null;
    }

    this.setState('disconnected');
  }

  /**
   * Force a manual sync cycle (push + pull).
   */
  async sync(): Promise<void> {
    if (this.state !== 'connected') {
      this.log('⚠️ Cannot sync — not connected');
      return;
    }

    await this.pushPendingOps();
    await this.pullChanges();
  }

  /**
   * Check if connected.
   */
  get isConnected(): boolean {
    return this.state === 'connected';
  }

  /**
   * Current connection state.
   */
  get connectionState(): ConnectionState {
    return this.state;
  }

  // ─── Internal Connection ────────────────────────────────────────────────────

  private async connect(): Promise<void> {
    if (this.destroyed || this.transport) return;

    this.setState('connecting');

    try {
      if (this.config.transport) {
        this.transport = this.config.transport;
      } else if (this.config.serverUrl) {
        this.transport = new WebSocketTransport(this.config.serverUrl);
      } else {
        throw new Error('[Meridian Sync] Either serverUrl or transport must be provided.');
      }

      this.transport.onOpen(async () => {
        this.log('🔌 Transport connected');
        this.retryDelay = INITIAL_RETRY_DELAY;
        this.startHeartbeat();

        // Authenticate
        if (this.config.auth) {
          this.setState('authenticating');
          const token = await this.config.auth.getToken();
          this.send({
            type: 'auth',
            token,
            schemaVersion: this.config.schemaVersion,
          });
          // Wait for auth-ack before syncing
        } else {
          this.setState('connected');
          await this.config.store.resetPendingStatus();
          await this.sync();
        }
      });

      this.transport.onMessage(async (msg: any) => {
        if (msg.type === 'pong') {
          this.lastPongTime = Date.now();
          return;
        }

        try {
          if (!validateMessage(msg)) {
            this.log('❌ Invalid message format, ignoring');
            return;
          }
          await this.handleMessage(msg);
        } catch (e) {
          this.log('❌ Failed to handle message:', e);
        }
      });

      this.transport.onError((err) => {
        this.log('❌ Transport error:', err.message);
      });

      this.transport.onClose((code, reason) => {
        this.log(`🔌 Transport closed: ${code} - ${reason}`);
        this.transport = null;
        this.clearHeartbeat();
        this.setState('disconnected');

        if (!this.destroyed) {
          this.scheduleReconnect();
        }
      });

      await this.transport.connect();
    } catch (e) {
      this.log('❌ Connection failed:', e);
      this.transport = null;
      this.setState('disconnected');

      if (!this.destroyed) {
        this.scheduleReconnect();
      }
    }
  }

  private scheduleReconnect(): void {
    this.clearRetry();

    this.log(`⏳ Reconnecting in ${this.retryDelay}ms...`);

    this.retryTimeout = setTimeout(() => {
      this.retryTimeout = null;
      this.connect();
    }, this.retryDelay);

    // Exponential backoff with jitter
    this.retryDelay = Math.min(
      this.retryDelay * 2 + Math.random() * 1000,
      MAX_RETRY_DELAY
    );
  }

  private clearRetry(): void {
    if (this.retryTimeout) {
      clearTimeout(this.retryTimeout);
      this.retryTimeout = null;
    }
  }

  // ─── Heartbeat ──────────────────────────────────────────────────────────────

  private startHeartbeat(): void {
    this.clearHeartbeat();
    this.lastPongTime = Date.now();
    this.heartbeatInterval = setInterval(() => {
      if (this.transport?.connected) {
        this.send('ping' as any);
        
        if (Date.now() - this.lastPongTime > HEARTBEAT_INTERVAL * 2) {
          this.log('⚠️ Dead connection detected (no pong received)');
          this.transport.close();
        }
      }
    }, HEARTBEAT_INTERVAL);
  }

  private clearHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  // ─── Message Handling ───────────────────────────────────────────────────────

  private async handleMessage(msg: ServerMessage): Promise<void> {
    switch (msg.type) {
      case 'ack':
        await this.handleAck(msg);
        break;

      case 'reject':
        await this.handleReject(msg);
        break;

      case 'changes':
        await this.handleChanges(msg);
        break;

      case 'full-sync-required':
        await this.handleFullSyncRequired(msg);
        break;

      case 'auth-expiring':
        await this.handleAuthExpiring(msg);
        break;

      case 'auth-expired':
        this.log('🔒 Auth expired — disconnecting');
        this.transport?.close();
        break;

      case 'auth-ack':
        this.log('🔓 Auth successful');
        this.setState('connected');
        await this.config.store.resetPendingStatus();
        await this.sync();
        break;

      case 'presence':
        if (this.config.onPresence && 'peers' in msg) {
          this.config.onPresence(msg.peers as Record<string, Record<string, unknown>>);
        }
        break;

      case 'compaction':
        this.log(`🧹 Compaction: minSeq=${msg.minSeq}`);
        break;

      case 'error':
        this.log(`❌ Server error [${msg.code}]: ${msg.message}`);
        break;
    }
  }

  private async handleAck(msg: AckMessage): Promise<void> {
    this.log(`✅ Ack: ${msg.opIds.length} ops confirmed, lastSeq=${msg.lastSeq}`);
    await this.config.store.ackOps(msg.opIds);
    await this.config.store.setLastSeq(msg.lastSeq);
  }

  private async handleReject(msg: RejectMessage): Promise<void> {
    this.log(`❌ Reject: op=${msg.opId} code=${msg.code} reason=${msg.reason}`);

    const rolledBack = await this.config.store.rollbackOp(msg.opId);
    if (rolledBack && this.config.onRollback) {
      this.config.onRollback(rolledBack, msg.reason);
    }
  }

  private async handleChanges(msg: ChangesMessage): Promise<void> {
    if (msg.changes.length === 0) return;

    this.setState('syncing');
    this.log(`⬇️ Received ${msg.changes.length} changes`);

    // Integrate E2E Transit Decryption
    const enc = this.config.store.config.encryption;
    const ops = await Promise.all(msg.changes.map(async (c) => {
      let opToApply = c.op;
      if (enc && enc.fields.includes(c.op.field) && typeof c.op.value === 'string') {
        try {
          const uint8 = base64ToUint8(c.op.value);
          opToApply = { ...c.op, value: uint8 };
        } catch (e) {
          this.log(`❌ Failed to decode/decrypt received op value for field ${c.op.field}:`, e);
        }
      }
      return opToApply;
    }));

    await this.config.store.applyRemoteChanges(ops);

    // Update lastSeq to highest received
    const maxSeq = Math.max(...msg.changes.map(c => c.seq));
    await this.config.store.setLastSeq(maxSeq);

    this.setState('connected');
  }

  private async handleFullSyncRequired(msg: FullSyncRequiredMessage): Promise<void> {
    this.log(`🔄 Full re-sync required: ${msg.reason} (minSeq=${msg.minSeq})`);

    // Clear local data and pull everything
    await this.config.store.clearAll();
    await this.pullChanges();
  }

  private async handleAuthExpiring(msg: AuthExpiringMessage): Promise<void> {
    this.log(`🔑 Auth expiring in ${msg.expiresIn}s — refreshing`);

    if (this.config.auth) {
      try {
        const newToken = await this.config.auth.getToken();
        this.send({ type: 'auth', token: newToken });
        this.log('🔑 Auth refreshed');
      } catch (e) {
        this.log('❌ Auth refresh failed:', e);
      }
    }
  }

  // ─── Push / Pull ────────────────────────────────────────────────────────────

  /**
   * Push all pending operations to the server.
   */
  async pushPendingOps(): Promise<void> {
    if (this.pushInProgress || !this.transport || !this.transport.connected) return;

    this.pushInProgress = true;

    try {
      const pendingOps = await this.config.store.getPendingOps();

      if (pendingOps.length === 0) {
        return;
      }

      this.log(`⬆️ Pushing ${pendingOps.length} pending ops`);

      // Send in batches
      for (let i = 0; i < pendingOps.length; i += PUSH_BATCH_SIZE) {
        const batch = pendingOps.slice(i, i + PUSH_BATCH_SIZE);
        const opIds = batch.map(p => p.id);

        await this.config.store.markOpsSending(opIds);

        // Integrate E2E Transit Encryption
        const enc = this.config.store.config.encryption;
        const processedOps = await Promise.all(batch.map(async (p) => {
          if (enc && enc.fields.includes(p.op.field) && typeof p.op.value === 'string') {
            const encrypted = await encryptValue(enc.key, p.op.value);
            const base64 = uint8ToBase64(encrypted);
            return { ...p.op, value: base64 };
          }
          return p.op;
        }));

        this.send({
          type: 'push',
          ops: processedOps,
        });
      }
    } finally {
      this.pushInProgress = false;
    }
  }

  /**
   * Pull changes from server since last known seqNum.
   */
  async pullChanges(): Promise<void> {
    if (!this.transport || !this.transport.connected) return;

    const lastSeq = await this.config.store.getLastSeq();
    this.log(`⬇️ Pulling changes since seq=${lastSeq}`);

    this.send({
      type: 'pull',
      since: lastSeq,
    });
  }

  // ─── Online/Offline ─────────────────────────────────────────────────────────

  private handleOnline = (): void => {
    this.log('🌐 Online — reconnecting');
    if (!this.transport && !this.destroyed) {
      this.retryDelay = INITIAL_RETRY_DELAY;
      this.connect();
    }
  };

  private handleOffline = (): void => {
    this.log('📴 Offline');
    this.clearRetry();
    this.setState('disconnected');
  };

  // ─── Utilities ──────────────────────────────────────────────────────────────

  public send(msg: ClientMessage): void {
    if (this.transport?.connected) {
      this.transport.send(msg);
    }
  }

  private setState(state: ConnectionState): void {
    if (this.state === state) return;
    this.state = state;
    this.config.onConnectionChange?.(state);
    for (const listener of this.stateListeners) {
      try {
        listener(state);
      } catch (e) {
        console.error('[SyncEngine] State listener error:', e);
      }
    }
  }

  private log(...args: unknown[]): void {
    if (this.config.debug) {
      console.log('[Meridian]', ...args);
    }
  }
}
