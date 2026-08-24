# @swift-flight/channels

The JS/TS reference client for Flight Channels: WebSocket management, the
envelope protocol, ref/reply correlation (`push()` returns a promise resolving
on the matching `flight:reply`), the heartbeat, and automatic
reconnect-with-backoff-and-rejoin. Deliberately small and dependency-free —
protocol plumbing, not a framework.

ESM, zero dependencies, typed via `types/index.d.ts`. Works in browsers and
in Node ≥ 18 (inject a WebSocket implementation where there's no global).

> phoenix.js does not work against Flight, by design: Flight owns
> both server and clients, and this is the client it versions with the
> protocol.

## Usage

```js
import { FlightSocket, ChannelError, TimeoutError } from "@swift-flight/channels";

const socket = new FlightSocket("wss://example.app/socket?token=…");
await socket.connect();

const room = socket.channel("room:42");
const initialState = await room.join();          // the join is the gate

room.on("new_msg", (payload) => render(payload)); // server pushes
room.on("*", (payload, event) => log(event));     // everything, incl. rejoins

const reply = await room.push("new_msg", { body: "hi" }); // awaits flight:reply
room.send("typing", { on: true });                        // fire-and-forget (ref: null)

await room.leave();
socket.disconnect();
```

### Errors

`push()`/`join()` reject with:

- `ChannelError` — the server answered `flight:error`; `.reason` is the
  wire reason (`"forbidden"`, `"not_joined"`, …).
- `TimeoutError` — no reply before the deadline (`pushTimeoutMs`, default
  10 s). A handler that returns `.none` never replies — use `send()` for
  those events.
- `DisconnectedError` — the connection dropped while awaiting the reply.
- `NotConnectedError` — pushed before `connect()` (or while closed).

### Reconnection

Reconnection is client-driven; the server holds no resumable session. On a
drop the socket re-dials per `reconnectDelayMs` (default: doubling backoff,
100 ms → 10 s, forever) and rejoins every joined channel. The fresh initial
state is delivered to listeners as a `"flight:join"` message. A rejected
rejoin (the gate closed while you were away) arrives as `"flight:error"`
and stops retrying that topic. `disconnect()` and a server `flight:close`
are terminal — no reconnection until `connect()` is called again.

```js
const socket = new FlightSocket(url, {
  heartbeatIntervalMs: 25_000, // keep well inside the server's 60 s timeout
  pushTimeoutMs: 10_000,
  reconnectDelayMs: exponentialBackoff({ initialMs: 100, maxMs: 10_000 }),
  webSocket: WebSocket,        // injectable (Node, tests)
});
socket.onStateChange((state) => {
  // "connecting" | "connected" | "disconnected" | "closed"
});
```

Heartbeats ride `flight:heartbeat` on the reserved `"flight"` topic; an
unanswered heartbeat is treated as a dead connection and triggers the
reconnect path.

## Tests

```sh
npm test   # node --test; zero dependencies
```

The suite drives the client against a scripted in-memory server speaking
the same wire fixtures the Swift test suite asserts on — one protocol,
three artifacts, versioned together.
