import test from "node:test";
import assert from "node:assert/strict";
import { FlightPresence, PRESENCE_EVENTS } from "../src/flight-presence.js";
import { FlightSocket } from "../src/flight-channels.js";
import { MockWebSocket, scriptServer } from "./mock-websocket.js";

// These cases mirror the Swift `PresenceSyncTests` — one rulebook, two
// implementations, the same assertions ( ).

/** A minimal channel double: just the `on` registry FlightPresence uses. */
function stubChannel() {
  const listeners = new Map();
  return {
    on(event, listener) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(listener);
      return () => listeners.get(event).delete(listener);
    },
    deliver(event, payload) {
      for (const listener of listeners.get(event) ?? []) listener(payload, event);
    },
  };
}

const state = (entries) => entries;
const meta = (ref, rest = {}) => ({ ref, ...rest });
const entry = (metas) => ({ metas });

test("state replaces the view and reports the difference", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);
  const changes = [];
  presence.onChange((change) => changes.push(change));

  channel.deliver(PRESENCE_EVENTS.state, state({ "user:7": entry([meta("a1", { status: "online" })]) }));
  assert.deepEqual(presence.list(), [{ key: "user:7", metas: [meta("a1", { status: "online" })] }]);
  assert.deepEqual(Object.keys(changes[0].joins), ["user:7"]);
  assert.deepEqual(changes[0].leaves, {});

  // A later state (rejoin) reports exactly who came and went.
  channel.deliver(PRESENCE_EVENTS.state, state({ "user:9": entry([meta("b1")]) }));
  assert.deepEqual(Object.keys(changes[1].joins), ["user:9"]);
  assert.deepEqual(Object.keys(changes[1].leaves), ["user:7"]);
  assert.deepEqual(presence.list().map((e) => e.key), ["user:9"]);
});

test("join adds a meta; leave removes by ref; key gone on last meta", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);

  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:7": entry([meta("a1"), meta("a2")]) }, leaves: {} });
  assert.equal(presence.list()[0].metas.length, 2);

  channel.deliver(PRESENCE_EVENTS.diff, { joins: {}, leaves: { "user:7": entry([meta("a1")]) } });
  assert.deepEqual(presence.list()[0].metas.map((m) => m.ref), ["a2"]);

  channel.deliver(PRESENCE_EVENTS.diff, { joins: {}, leaves: { "user:7": entry([meta("a2")]) } });
  assert.deepEqual(presence.list(), []);
});

test("an update diff (leave+join, same ref) normalizes to an in-place change — no flap", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);
  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:7": entry([meta("a1", { status: "online" })]) }, leaves: {} });

  const changes = [];
  presence.onChange((change) => changes.push(change));
  channel.deliver(PRESENCE_EVENTS.diff, {
    joins: { "user:7": entry([meta("a1", { status: "away" })]) },
    leaves: { "user:7": entry([meta("a1", { status: "online" })]) },
  });

  assert.deepEqual(changes[0].leaves, {}, "an update must not report a leave");
  assert.deepEqual(changes[0].joins["user:7"], [meta("a1", { status: "away" })]);
  assert.deepEqual(presence.list()[0].metas, [meta("a1", { status: "away" })]);
});

test("a re-delivered join for a known ref upserts and reports nothing", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);
  const changes = [];
  presence.onChange((change) => changes.push(change));

  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:7": entry([meta("a1", { s: "x" })]) }, leaves: {} });
  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:7": entry([meta("a1", { s: "x" })]) }, leaves: {} });

  assert.equal(changes.length, 1);
  assert.equal(presence.list()[0].metas.length, 1);
});

test("a leave for an unknown ref is a silent no-op", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);
  const changes = [];
  presence.onChange((change) => changes.push(change));

  channel.deliver(PRESENCE_EVENTS.diff, { joins: {}, leaves: { "user:7": entry([meta("zz")]) } });
  assert.equal(changes.length, 0);
  assert.deepEqual(presence.list(), []);
});

test("malformed payloads are tolerated (dropped members, not exceptions)", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);

  channel.deliver(PRESENCE_EVENTS.state, null);
  channel.deliver(PRESENCE_EVENTS.state, { "user:7": { metas: "nope" } });
  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:8": { metas: [{ noRef: true }] } } });
  assert.deepEqual(presence.list(), []);
});

test("destroy stops updates", () => {
  const channel = stubChannel();
  const presence = new FlightPresence(channel);
  presence.destroy();
  channel.deliver(PRESENCE_EVENTS.diff, { joins: { "user:7": entry([meta("a1")]) }, leaves: {} });
  assert.deepEqual(presence.list(), []);
});

// ---- against the real client -------------------------------------------

test("works over a real FlightChannel: server frames drive the list", async () => {
  MockWebSocket.reset();
  scriptServer();
  const socket = new FlightSocket("ws://example.test/socket", {
    webSocket: MockWebSocket,
    heartbeatIntervalMs: 60_000,
    pushTimeoutMs: 200,
    reconnectDelayMs: () => 5,
  });
  await socket.connect();
  const room = socket.channel("room:42");
  const presence = new FlightPresence(room);
  await room.join();

  const ws = MockWebSocket.current;
  ws.receive({
    ref: null,
    topic: "room:42",
    event: PRESENCE_EVENTS.state,
    payload: { "user:7": { metas: [{ ref: "a1", status: "online" }] } },
  });
  ws.receive({
    ref: null,
    topic: "room:42",
    event: PRESENCE_EVENTS.diff,
    payload: { joins: { "user:9": { metas: [{ ref: "b1" }] } }, leaves: {} },
  });

  assert.deepEqual(
    presence.list().map((entry) => entry.key),
    ["user:7", "user:9"],
  );
  socket.disconnect();
});
