<div align="center">
  <img src="./assets/ipeclabs-logo-light.svg" alt="Meridian Logo" width="120" />
</div>

<h1 align="center">Meridian</h1>

<p align="center">
  <b>High-performance local-first sync engine for realtime collaborative apps.</b><br/><br/>
  Offline-first. Real-time. Conflict-resolved.<br/>
  Works with PostgreSQL, SQLite, and any transport.<br/>
  No sync backend to write.
</p>

<p align="center">
  <a href="https://meridiandb-demo.vercel.app">
    <img src="https://img.shields.io/badge/Live_Demo-→-blue?style=for-the-badge" alt="Live Demo">
  </a>
</p>

<p align="center">
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache-2.0"></a>
  <img src="https://img.shields.io/badge/TypeScript-007ACC?logo=typescript&logoColor=white" alt="TypeScript">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Local--First-Success?style=flat&color=10b981" alt="Local-First">
  <img src="https://img.shields.io/badge/tests-76_passed-brightgreen" alt="Tests: 48 passed">
  <img src="https://img.shields.io/badge/benchmarks-20M_ops%2Fs-blue" alt="Benchmarks">
  <img src="https://img.shields.io/badge/React-hooks-blue?logo=react" alt="React">
  <img src="https://img.shields.io/badge/React_Native-mobile-blue?logo=react" alt="React Native">
  <img src="https://img.shields.io/badge/SQLite-adapter-orange?logo=sqlite" alt="SQLite">
  <img src="https://img.shields.io/badge/Vue-composables-green?logo=vue.js" alt="Vue">
  <img src="https://img.shields.io/badge/Flutter-SDK-blue?logo=flutter" alt="Flutter">
  <img src="https://img.shields.io/badge/Rust-WASM-orange?logo=rust" alt="Rust">
  <img src="https://img.shields.io/badge/Svelte-5-orange?logo=svelte" alt="Svelte">
  <img src="https://img.shields.io/badge/WebRTC-P2P-red" alt="WebRTC">
  <img src="https://img.shields.io/badge/E2E-encrypted-green" alt="E2E Encryption">
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#how-it-works">How it Works</a> •
  <a href="#architecture">Architecture</a>
</p>

---

Meridian gives your app **Firebase-like sync** on top of your own **PostgreSQL** database. 

No custom WebSocket infrastructure.  
No reconciliation logic.  
No offline queue implementation.  
No cache invalidation headaches.

<p align="center">
  <a href="https://meridiandb-demo.vercel.app">
    <img src="./assets/demo-ui.png" alt="MeridianDB Multiplayer Sync Demo" width="800" />
  </a>
  <br/>
  <b>Live demo:</b> <a href="https://meridiandb-demo.vercel.app">meridiandb-demo.vercel.app</a> — open in two tabs to see real-time sync.
</p>

## The Meridian DX

Define your schema, connect to the database, and start writing. That's it.

```typescript
import { createClient } from 'meridian-client';

const db = createClient({
  schema,
  serverUrl: 'wss://api.yourdomain.com/sync',
});

// 1. Reactive Queries (Auto-updates UI on any change)
db.todos.find().subscribe(renderTodos);

// 2. Write Data (Instantly updates UI, persists offline, syncs when online)
await db.todos.put({
  id: crypto.randomUUID(),
  title: "Build sync engine",
});

// 3. Update Fields (CRDT field-level merge prevents conflicts)
await db.todos.update(id, { done: true });
```

### What just happened?
Meridian automatically handled:
- [x] IndexedDB persistence
- [x] Offline queueing
- [x] WebSocket sync
- [x] Multi-tab coordination
- [x] Conflict resolution
- [x] Reconnect recovery
- [x] Optimistic updates
- [x] PostgreSQL persistence

---

## Why Meridian?

If you've built collaborative or offline-capable apps before, you know the pain. You start with a simple `fetch`, then add websockets for real-time, then add local caching, then try to handle offline writes, and suddenly you have a messy, state-inconsistent nightmare.

Meridian solves this by abstracting the entire sync layer into a single, cohesive engine.

* **For Supabase/Firebase Developers:** Keep the magical Developer Experience, but own your data in a standard PostgreSQL database.
* **For React/Vue Developers:** Stop writing `useEffect` blocks to fetch data. Just `.subscribe()` and let the engine handle the rest.
* **For Local-First Believers:** Build apps that are instantly interactive, even on a subway with zero connection.

## Features

* **Zero-Config PostgreSQL:** Meridian automatically creates your tables and adds `_meridian` system columns. No migration scripts needed for new collections.
* **Multi-Tab Leader Election:** Only one browser tab maintains the WebSocket connection. Other tabs coordinate locally via `BroadcastChannel`, drastically reducing server load.
* **Seamless Reconnects:** Exponential backoff with jitter, offline queues, and automatic replay of missed operations upon reconnection.
* **Additive Migrations:** Add new fields with `.default()` values. Clients running v1 and v2 schemas can seamlessly collaborate without breaking.
* **Real-time Presence:** Built-in ephemeral cursor and status tracking that bypasses the database for maximum performance.
* **Live Queries (V2):** `db.todos.live({ where: { done: false }, orderBy: 'createdAt', limit: 50 })` — reactive queries with filtering, sorting, and pagination.
* **Permission Rules DSL (V2):** Firebase-like security rules with `defineRules()` + `RuleEvaluator` for row-level read/write access control.
* **Storage Adapter Interface (V2):** Abstract `StorageAdapter` interface enabling PostgreSQL, SQLite (Turso), and MySQL backends.
* **Message Validation:** All incoming WebSocket messages are validated against known types — malformed data is rejected early.
* **IndexedDB Quota Handling:** Graceful error reporting when browser storage limits are exceeded.
* **React Hooks:** `useQuery`, `useLiveQuery`, `useDoc`, `useSync`, `usePresence`, `useMutation` — zero boilerplate React integration.
* **E2E Encryption:** AES-256-GCM field-level encryption. IndexedDB at-rest + WebSocket in-transit. Server stores only ciphertext.
* **CLI Tools:** `meridian migrate`, `inspect`, `replay`, `status`, `compact` — manage sync infrastructure from the command line.
* **WAL Streaming:** PostgreSQL LISTEN/NOTIFY + logical replication for real-time change data capture at scale.
* **Partial Sync:** Row-level `WHERE` filters on subscriptions — clients only sync matching data, not entire collections.
* **Row-Level Permissions:** Server-side `RuleEvaluator` filters changes by auth context. Users only see authorized rows.
* **SQLite Adapter:** Full `SQLiteStore` implementing the StorageAdapter interface — works with better-sqlite3, sql.js, and Turso.
* **React Native SDK:** `meridian-react-native` — AsyncStorage-backed client with the same hooks API as the browser.
* **Svelte 5 Stores:** `meridian-svelte` — $state runes, reactive queries, presence, mutations.
* **Auth Adapters:** Drop-in Supabase Auth, Auth0, Clerk, and generic JWT verification.
* **Observability:** Prometheus-compatible metrics — P95/P99 latency, ops/sec, connected clients.
* **GraphQL Subscriptions:** Apollo/yoga compatible async iterator bridge.
* **WebRTC P2P:** Serverless peer-to-peer sync via DataChannel. No central server needed.

## Quick Start

### 1. Install

```bash
# Core
npm install meridian-shared meridian-client meridian-server

# Framework bindings
npm install meridian-react-sync   # React hooks
npm install meridian-vue          # Vue composables
npm install meridian-react-native # React Native
npm install meridian-svelte       # Svelte 5

# CLI
npm install -g meridiandb-cli

# Flutter
dart pub add meridian_sync
```

| Package | npm |
|---|---|
| Core — CRDT, HLC, protocol | [meridian-shared](https://www.npmjs.com/package/meridian-shared) |
| Browser client | [meridian-client](https://www.npmjs.com/package/meridian-client) |
| Node.js server | [meridian-server](https://www.npmjs.com/package/meridian-server) |
| React hooks | [meridian-react-sync](https://www.npmjs.com/package/meridian-react-sync) |
| Vue composables | [meridian-vue](https://www.npmjs.com/package/meridian-vue) |
| React Native | [meridian-react-native](https://www.npmjs.com/package/meridian-react-native) |
| CLI tools | [meridiandb-cli](https://www.npmjs.com/package/meridiandb-cli) |
| Svelte 5 | [meridian-svelte](https://www.npmjs.com/package/meridian-svelte) |
| Flutter | [meridian_sync](https://pub.dev/packages/meridian_sync) |

### 2. Define your Schema (Shared)

```typescript
// shared/schema.ts
import { defineSchema, z } from 'meridian-shared';

export const schema = defineSchema({
  version: 1,
  collections: {
    issues: {
      id: z.string(),
      title: z.string(),
      status: z.string().default('open'),
      createdAt: z.number(),
    },
  },
});
```

### 3. Start the Server

```typescript
// server.ts
import { createServer } from 'meridian-server';
import { schema } from './shared/schema';

const server = createServer({
  port: 3000,
  database: 'postgresql://postgres:postgres@localhost:5432/mydb',
  schema,
  auth: async (token) => {
    // Validate JWT and return userId
    const user = verifyJWT(token);
    return { userId: user.id };
  }
});

await server.start();
```

### 4. Connect the Client

```typescript
// client.ts
import { createClient } from 'meridian-client';
import { schema } from './shared/schema';

const db = createClient({
  schema,
  serverUrl: 'ws://localhost:3000/sync',
  auth: { getToken: () => localStorage.getItem('jwt') }
});

// Done. Start building!
db.issues.find().subscribe(issues => console.log(issues));
```

### 5. React Integration (Optional)

```tsx
import { useQuery, useLiveQuery, useMutation } from 'meridian-react';

function TodoList() {
  const todos = useQuery(db.todos.find());
  const { put, update, remove } = useMutation(db.todos);

  if (!todos) return <p>Loading...</p>;

  return todos.map(todo => (
    <div key={todo.id}>
      <span>{todo.title}</span>
      <button onClick={() => update(todo.id, { done: !todo.done })}>
        {todo.done ? 'Undo' : 'Done'}
      </button>
    </div>
  ));
}
```

### 6. CLI Management

```bash
npx meridian-cli status   --url wss://api.example.com/sync
npx meridian-cli inspect  --db postgresql://... --collection todos
npx meridian-cli compact  --db postgresql://... --max-age 30
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT (Browser / RN)                     │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌─────────────┐  │
│  │ React    │  │ Reactive  │  │ CRDT Merge │  │ Persistence │  │
│  │ Hooks    │→ │ Queries   │→ │ Engine     │→ │ IndexedDB   │  │
│  │ useQuery │  │ .live()   │  │ HLC + LWW  │  │ SQLite/OPFS │  │
│  └──────────┘  └───────────┘  └────────────┘  └─────────────┘  │
│                                                      ↓          │
│                                              ┌─────────────┐    │
│                                              │ Offline     │    │
│                                              │ Queue       │    │
│                                              └─────────────┘    │
└──────────────────────────────────────────────────────────────────┘
                               │ Transport
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                        SERVER (Node.js)                          │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌─────────────┐  │
│  │ WebSocket│  │ CRDT      │  │ Permission │  │ Persistence │  │
│  │ Hub      │→ │ Merge     │→ │ Rules      │→ │ PostgreSQL  │  │
│  │          │  │ Engine    │  │ Evaluator  │  │ SQLite      │  │
│  └──────────┘  └───────────┘  └────────────┘  └─────────────┘  │
│                                                      ↓          │
│                                              ┌─────────────┐    │
│                                              │ WAL Stream  │    │
│                                              │ Compaction  │    │
│                                              └─────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Use Cases

**What can you build with Meridian?**

| Use Case | Why Meridian? |
|---|---|
| **Collaborative Editor** (Figma/Notion-like) | Field-level CRDT — two users editing different parts of the same doc never conflict |
| **Offline-First Notes App** | Writes persist to IndexedDB immediately, sync when back online — zero UX delay |
| **Multiplayer Game State** | HLC-based ordering + presence API — all clients see consistent game world |
| **Real-Time Dashboard** | Reactive queries push updates to UI, no polling needed |
| **Mobile App with Spotty Connection** | SQLite on-device → sync when online. Subway-proof |
| **Edge Cache Replication** | WAL streaming pushes changes to edge nodes in real-time |
| **Supabase Realtime Alternative** | Own your Postgres. No vendor lock-in. Same DX |

## CRDT Ecosystem Comparison

| Engine | Language | CRDT | Field-Level | Offline | Transport Agnostic | Self-Hosted |
|---|---|---|---|---|---|---|
| **Meridian** | TypeScript | HLC + LWW | ✅ | ✅ | ✅ | ✅ |
| Yjs | JavaScript | YATA | ❌ | ✅ | ⚠️ | ✅ |
| Automerge | Rust/JS | RGA | ❌ | ✅ | ✅ | ✅ |
| Liveblocks | SaaS | ❌ | ❌ | ⚠️ | ❌ | ❌ |
| ElectricSQL | Elixir/TS | LWW | ❌ | ✅ | ❌ | ✅ |
| PowerSync | Dart/TS | Custom | ❌ | ✅ | ❌ | ⚠️ |

> **Meridian's edge:** Field-level CRDT means no data loss when two users edit the same row. Transport-agnostic means you're not locked into WebSocket. Self-hosted means you own your data.

## How it Works (Under the Hood)

For the curious engineers, Meridian is built on robust distributed systems principles.

### Deterministic Conflict Resolution
Meridian uses **Hybrid Logical Clocks (HLC)** combined with **Last-Writer-Wins (LWW)** CRDTs at the *field level*. This means User A can edit the `title` while User B edits the `status` of the same row while both are offline. When they reconnect, both edits merge flawlessly. No data loss.

### Reliable Pull via Sequence Numbers
The PostgreSQL server assigns a monotonically increasing `SeqNum` to every operation. Clients track their last seen `SeqNum`. On reconnect, the client simply asks: *"Give me everything since SeqNum 42."* This avoids clock drift issues and guarantees consistency.

### Compaction & Tombstones
Deleted rows are converted to tombstones (soft-deleted). A background compaction scheduler periodically cleans up tombstones older than 30 days. If an offline client connects after a compaction event, Meridian detects the gap and triggers a `full-sync-required` event to self-heal.

### E2E In-Transit Encryption
In addition to at-rest document encryption, Meridian v2.0 supports strict End-to-End (E2E) in-transit encryption. When enabled, operation payloads are encrypted using AES-256-GCM prior to being pushed over the `Transport`. Pull operations are decrypted locally upon receipt. Because the sync server only stores and routes the encrypted base64 ciphertext, it acts as a completely blind broker, preventing eavesdropping or data leakage on the server side.

### Pluggable Transports & Heartbeats
The v2.0 client decouples direct dependencies on browser WebSocket globals by operating through a generic `Transport` interface. This allows syncing over WebSockets, WebRTC, custom TCP sockets, or custom simulated testing layers. The default `WebSocketTransport` is hardened to automatically parse and respond to raw, non-JSON string `'ping'` / `'pong'` heartbeats initiated by the server without disrupting message parsing.

### Race-Condition Free Initialization (`ready()`)
To eliminate timing issues where an app attempts database operations before storage adapters are fully open, Meridian v2.0 client features a cached `ready()` promise. CRUD operations initiated during the boot phase are automatically queued internally and resolved once the underlying IndexedDB/SQLite connection is established, ensuring flawless developer experience.

### Event-Driven Framework Subscriptions
Rather than using resource-intensive periodic polling (`setInterval`) to update connection and sync state in UI frameworks, the v2.0 sync engine uses an event-driven pub/sub model via `client.onSyncChange(callback)`. React, Vue, and Svelte framework packages are fully updated to utilize this interface, dropping CPU usage during idle connection periods to zero.

## V2 Roadmap

Meridian is evolving to become the ultimate infra product for local-first development.

### Done in v0.2.0
- [x] **Live Query Layer:** `db.todos.live({ where: { status: 'open' }, limit: 50 })` for partial hydration.
- [x] **Permission Rules DSL:** Firebase-like security rules with `defineRules()` and `RuleEvaluator`.
- [x] **Storage Adapter Interface:** Abstract adapter for PostgreSQL, SQLite, MySQL backends.
- [x] **Comprehensive Tests:** 48 unit tests covering HLC and CRDT modules.
- [x] **Performance Benchmarks:** 20M HLC ops/s, 3M LWWMap creates/s, 580K ops/s throughput.

### Done in v0.3.0
- [x] **React Hooks:** `useQuery`, `useLiveQuery`, `useDoc`, `useSync`, `usePresence`, `useMutation`.
- [x] **E2E Encryption:** AES-256-GCM field-level encryption for IndexedDB + WebSocket.
- [x] **CLI Tools:** `meridian migrate`, `inspect`, `replay`, `status`, `compact`.
- [x] **WAL Streaming:** PostgreSQL LISTEN/NOTIFY + logical replication polling consumer.

### Done in v0.4.0
- [x] **Partial Sync:** Row-level `WHERE` filters on subscriptions — clients only sync matching data.
- [x] **Row-Level Permissions:** Server-side `RuleEvaluator` filters changes by auth context.
- [x] **Custom Conflict Hooks:** `onConflict` callback on MergeEngine for custom merge logic.
- [x] **Fine-Grained Reactivity:** `useQueryOptimized` — only changed documents trigger re-renders.
- [x] **SQLite Adapter:** `SQLiteStore` with better-sqlite3, sql.js WASM, and Turso/libsql support.
- [x] **React Native SDK:** `meridian-react-native` with AsyncStorage + same hooks API.

### Done in v0.5.0
- [x] **Sync Compression:** Debouncing + delta encoding + batched push. 150ms window, 100 ops/batch.
- [x] **MySQL Adapter:** `MySQLStore` with mysql2 driver. Full CRDT merge, compaction, indexes.
- [x] **Vue Composables:** `meridian-vue` — useQuery, useLiveQuery, useDoc, useSync, useMutation.
- [x] **Snapshot Recovery:** Periodic snapshots. O(snapshot + delta) sync instead of O(all_ops).
- [x] **Transport Abstraction:** `Transport` interface + `WebSocketTransport`. WebRTC/TCP/Redis ready.
- [x] **Multiplayer Demo:** `demo/multiplayer.html` — interactive field-level CRDT merge visualization.
- [x] **Flutter SDK:** `meridian_sync` Dart package — HLC + LWW CRDT + sqflite + WebSocket.
- [x] **Rust WASM Core:** `meridian-core` crate — HlcClock + LwwMap compiled to WebAssembly.

### Done in v0.7.0
- [x] **Svelte 5 Stores:** `meridian-svelte` — $state runes, useQuery, useLiveQuery, useDoc, useSync, useMutation.
- [x] **Auth Adapters:** Supabase Auth, Auth0, Clerk, generic JWT — drop-in ready.
- [x] **Observability:** Prometheus-compatible MetricsCollector — P95/P99 latency, ops/sec, uptime.
- [x] **GraphQL Subscriptions:** `MeridianPubSub` — Apollo/yoga compatible async iterator.
- [x] **WebRTC P2P Transport:** `WebRTCTransport` — serverless peer-to-peer sync via DataChannel.

### Done in v2.0.0 (Event-Driven & Pluggable Sync Architecture)
- [x] **Asynchronous Client Queueing:** Eliminates boot race conditions by caching initialization in `db.ready()` and queuing CRUD operations automatically until store readiness.
- [x] **Pluggable Transport Layer:** Decouples the client sync loop from hardcoded WebSocket instances, accepting any `Transport` compliant provider.
- [x] **Raw Heartbeat Support:** Strengthened `WebSocketTransport` to handle raw string `'ping'` and `'pong'` frames, avoiding JSON parser failures on server heartbeats.
- [x] **End-to-End (E2E) In-Transit Encryption:** Integrated AES-256-GCM encryption/decryption directly into the transport push and pull pipelines. The central server acts only as a blind ciphertext storage.
- [x] **Zero-Polling Reactivity:** Refactored React, Vue, and Svelte 5 `useSync` bindings from continuous 1000ms `setInterval` polling to an efficient event-driven pub/sub pattern (`onSyncChange`).

### Coming Next
- [ ] **TypeDoc API docs:** Full generated API documentation site.
- [ ] **SQLite WASM (browser):** sql.js-backed browser persistence for larger datasets.
- [ ] **Benchmark CI:** Automated benchmark tracking on every commit.

## Performance

Benchmarks from `tests/bench.ts` on a standard machine (Intel i7, 16GB RAM):

### Micro-Benchmarks

| Operation | Throughput |
|---|---|
| HLC `now()` | 20M ops/s |
| HLC `serialize` | 94M ops/s |
| LWWMap create (5 fields) | 3M ops/s |
| LWWMap merge (3 fields) | 870K ops/s |

### Scale Benchmarks

| Scenario | Result |
|---|---|
| 1,500 document creates + merges | **2.6ms** |
| 100K document merges | **119ms** |
| Memory / 10K documents | **~16MB heap** (~1.6KB/doc) |
| Sync payload (single field update) | **138 bytes** |
| Replication latency (local) | **<1ms** |

Run locally: `npx tsx tests/bench.ts`

### vs Yjs & Automerge

Competitive benchmark from `tests/competitive-bench.ts`:

| Metric | MeridianDB | Yjs | Automerge |
|---|---|---|---|
| Merge throughput | **688K ops/s** | ~275K ops/s | ~458K ops/s |
| Memory (10K docs) | **12MB** | ~17MB | ~15MB |
| Payload size | **150 B** | ~178 B | ~146 B |
| Field-level merge | ✅ | ❌ | ❌ |
| E2E encryption | ✅ | ❌ | ❌ |
| Transport agnostic | ✅ | ⚠️ | ✅ |
| WASM core | ✅ | ❌ | ✅ |
| npm packages | **7** | 2 | 2 |

> **MeridianDB is 2-3x faster than Yjs** thanks to field-level CRDT (only changed fields merge, not entire documents) and Rust WASM core.

Run: `npx tsx tests/competitive-bench.ts`

## Persistence Backends

Meridian supports multiple persistence backends. Choose based on your platform:

| Backend | Platform | Best For |
|---|---|---|
| **IndexedDB** | Browser | Default. Good for typical web apps (hundreds of MB) |
| **SQLite** | Server / Mobile / Browser (WASM) | Large datasets (GB scale). Full SQL query power |
| **OPFS** | Browser | High-performance file I/O. Large binary data |
| **PostgreSQL** | Server | Production sync server. WAL streaming |
| **AsyncStorage** | React Native | Simple key-value for mobile apps |
| **In-Memory** | Testing / Edge | Zero persistence. Ephemeral data |

## Testing

76 tests (48 unit + 28 integration) across all packages:

```bash
pnpm test
```

Tests cover: HLC initialization, counter management, recv monotonicity, serialization round-trips, LWW Register merge, field-level convergence, tombstone handling, conflict detection, metadata extraction, and reconstruction.

## License

Apache-2.0. Built for the community by IPEC Labs.
