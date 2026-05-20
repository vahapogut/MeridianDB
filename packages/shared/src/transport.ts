/**
 * Meridian — Transport Abstraction Layer
 *
 * Meridian is transport-agnostic. The sync engine works with any
 * bidirectional message channel — not just WebSocket.
 *
 * Supported transports:
 * - WebSocket (default, browser + Node.js)
 * - WebRTC DataChannel (P2P, no server needed)
 * - TCP Socket (native apps, embedded systems)
 * - Redis Pub/Sub (server-to-server replication)
 * - NATS / Kafka (enterprise message brokers)
 *
 * Implement this interface to add a new transport.
 */

import type { ClientMessage, ServerMessage } from './protocol.js';

// ─── Transport Interface ────────────────────────────────────────────────────

export interface Transport {
  /** Send a message to the other side */
  send(msg: ClientMessage | ServerMessage): void;

  /** Called when a message is received */
  onMessage(callback: (msg: ClientMessage | ServerMessage) => void): void;

  /** Called when the connection opens */
  onOpen(callback: () => void): void;

  /** Called when the connection closes */
  onClose(callback: (code?: number, reason?: string) => void): void;

  /** Called on error */
  onError(callback: (err: Error) => void): void;

  /** Open the connection */
  connect(): Promise<void>;

  /** Close the connection */
  close(): void;

  /** Whether the transport is connected */
  readonly connected: boolean;
}

// ─── WebSocket Transport ────────────────────────────────────────────────────

export class WebSocketTransport implements Transport {
  private ws: WebSocket | null = null;
  private url: string;
  private _onMessage?: (msg: ClientMessage | ServerMessage) => void;
  private _onOpen?: () => void;
  private _onClose?: (code?: number, reason?: string) => void;
  private _onError?: (err: Error) => void;

  constructor(url: string) {
    this.url = url;
  }

  get connected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  send(msg: ClientMessage | ServerMessage | string): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      if (typeof msg === 'string') {
        this.ws.send(msg);
      } else {
        this.ws.send(JSON.stringify(msg));
      }
    }
  }

  onMessage(callback: (msg: ClientMessage | ServerMessage) => void): void {
    this._onMessage = callback;
  }

  onOpen(callback: () => void): void {
    this._onOpen = callback;
  }

  onClose(callback: (code?: number, reason?: string) => void): void {
    this._onClose = callback;
  }

  onError(callback: (err: Error) => void): void {
    this._onError = callback;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);
      this.ws.onopen = () => { this._onOpen?.(); resolve(); };
      this.ws.onerror = (e) => { this._onError?.(new Error('WebSocket error')); reject(e); };
      this.ws.onclose = (e) => { this._onClose?.(e.code, e.reason); };
      this.ws.onmessage = (event) => {
        const rawData = event.data as string;
        if (rawData === 'pong') {
          this._onMessage?.({ type: 'pong' } as any);
          return;
        }
        try {
          const msg = JSON.parse(rawData);
          this._onMessage?.(msg);
        } catch {
          // Ignore non-JSON messages
        }
      };
    });
  }

  close(): void {
    this.ws?.close();
    this.ws = null;
  }
}

// ─── Transport Factory ──────────────────────────────────────────────────────

/** Currently supported transports */
export type TransportType = 'websocket';

/** Planned transports (not yet implemented) */
export type ExperimentalTransportType = 'webrtc' | 'tcp' | 'redis' | 'nats' | 'kafka';

export interface TransportConfig {
  type: TransportType | ExperimentalTransportType;
  url: string;
  options?: Record<string, unknown>;
}

/** Create a transport by type. Experimental transports throw with a clear message. */
export function createTransport(config: TransportConfig): Transport {
  switch (config.type) {
    case 'websocket':
      return new WebSocketTransport(config.url);
    case 'webrtc':
    case 'tcp':
    case 'redis':
    case 'nats':
    case 'kafka':
      throw new Error(
        `Transport "${config.type}" is not yet implemented. ` +
        `WebSocket is the default. Others are on the roadmap.`
      );
    default:
      throw new Error(`Unknown transport type: ${config.type}`);
  }
}
