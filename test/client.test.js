import test from "node:test";
import assert from "node:assert/strict";
import {
  FlightSocket,
  ChannelError,
  TimeoutError,
  DisconnectedError,
  NotConnectedError,
  exponentialBackoff,
} from "../src/flight-channels.js";
import { MockWebSocket, scriptServer } from "./mock-websocket.js";

function makeSocket(options = {}) {
  return new FlightSocket("ws://example.test/socket", {
    webSocket: MockWebSocket,
    heartbeatIntervalMs: 60_000, // out of the way unless a test wants it
    pushTimeoutMs: 200,
    reconnectDelayMs: () => 5,
    ...options,
  });
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test.beforeEach(() => {
  MockWebSocket.reset();
  scriptServer();
});

test("join sends the exact envelope and resolves with initial state", async () => {
  const socket = makeSocket();
  await socket.connect();

  const state = await socket.channel("room:42").join();
  assert.deepEqual(state, { count: 0 });
  assert.equal(socket.channel("room:42").joined, true);

  const frame = JSON.parse(MockWebSocket.current.sent[0]);
  assert.deepEqual(frame, { ref: "1", topic: "room:42", event: "flight:join", payload: {} });
  socket.disconnect();
});

test("rejected join rejects with ChannelError and its wire reason", async () => {
  scriptServer({ rejectTopics: ["room:locked"] });
  const socket = makeSocket();
  await socket.connect();

  await assert.rejects(socket.channel("room:locked").join(), (error) => {
    assert.ok(error instanceof ChannelError);
    assert.equal(error.reason, "forbidden");
    return true;
  });
  assert.equal(socket.channel("room:locked").joined, false);
  socket.disconnect();
});

test("push resolves on the matching flight:reply — refs correlate", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  const replies = await Promise.all(
    [0, 1, 2, 3, 4].map((n) => room.push("echo", { n })),
  );
  assert.deepEqual(replies, [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);
  socket.disconnect();
});

test("a handler that never replies rejects with TimeoutError", async () => {
  const socket = makeSocket({ pushTimeoutMs: 30 });
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  await assert.rejects(room.push("silent"), TimeoutError);
  socket.disconnect();
});

test("flight:error on a pushed ref rejects with ChannelError", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  await assert.rejects(room.push("fail"), (error) => {
    assert.equal(error.reason, "boom");
    return true;
  });
  socket.disconnect();
});

test("pushing while closed rejects with NotConnectedError", async () => {
  const socket = makeSocket();
  await assert.rejects(socket.channel("room:1").push("echo"), NotConnectedError);
});

test("server pushes reach on() listeners; '*' hears everything", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  const named = [];
  const everything = [];
  room.on("new_msg", (payload) => named.push(payload));
  room.on("*", (payload, event) => everything.push(event));

  MockWebSocket.current.receive({ ref: null, topic: "room:1", event: "new_msg", payload: { body: "hi" } });
  MockWebSocket.current.receive({ ref: null, topic: "room:1", event: "other", payload: {} });
  MockWebSocket.current.receive({ ref: null, topic: "room:2", event: "new_msg", payload: {} }); // foreign topic
  await tick();

  assert.deepEqual(named, [{ body: "hi" }]);
  assert.deepEqual(everything, ["new_msg", "other"]);
  socket.disconnect();
});

test("unsubscribe stops delivery", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  const seen = [];
  const off = room.on("ping", (payload) => seen.push(payload));
  MockWebSocket.current.receive({ ref: null, topic: "room:1", event: "ping", payload: 1 });
  await tick();
  off();
  MockWebSocket.current.receive({ ref: null, topic: "room:1", event: "ping", payload: 2 });
  await tick();
  assert.deepEqual(seen, [1]);
  socket.disconnect();
});

test("fire-and-forget send() puts ref: null on the wire", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  room.send("typing", { on: true });
  const frame = JSON.parse(MockWebSocket.current.sent.at(-1));
  assert.deepEqual(frame, { ref: null, topic: "room:1", event: "typing", payload: { on: true } });
  socket.disconnect();
});

test("heartbeats flow on the control topic and ride flight:heartbeat", async () => {
  const socket = makeSocket({ heartbeatIntervalMs: 20 });
  await socket.connect();
  await sleep(70);

  const heartbeats = MockWebSocket.current.sent
    .map((text) => JSON.parse(text))
    .filter((frame) => frame.event === "flight:heartbeat");
  assert.ok(heartbeats.length >= 2, `expected ≥2 heartbeats, saw ${heartbeats.length}`);
  assert.ok(heartbeats.every((frame) => frame.topic === "flight" && frame.ref != null));
  assert.equal(socket.state, "connected");
  socket.disconnect();
});

test("an unanswered heartbeat closes the connection and re-dials", async () => {
  scriptServer();
  const original = MockWebSocket.onSend;
  let deafAfterConnect = false;
  MockWebSocket.onSend = (ws, text) => {
    const frame = JSON.parse(text);
    if (deafAfterConnect && frame.event === "flight:heartbeat") return; // server gone deaf
    original(ws, text);
  };
  const socket = makeSocket({ heartbeatIntervalMs: 20, reconnectDelayMs: () => 5 });
  await socket.connect();
  const first = MockWebSocket.current;
  deafAfterConnect = true;

  await sleep(100);
  deafAfterConnect = false;
  await sleep(60);

  assert.notEqual(MockWebSocket.current, first, "expected a re-dial");
  assert.equal(socket.state, "connected");
  socket.disconnect();
});

test("a dropped connection reconnects with backoff and rejoins", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:7");
  await room.join();

  const rejoins = [];
  room.on("flight:join", (state) => rejoins.push(state));
  const states = [];
  socket.onStateChange((state) => states.push(state));

  MockWebSocket.current.drop();
  await sleep(30);

  assert.equal(socket.state, "connected");
  assert.equal(room.joined, true);
  assert.deepEqual(rejoins, [{ count: 0 }]); // fresh state announced
  assert.ok(states.includes("disconnected") && states.includes("connecting"));
  assert.equal(MockWebSocket.instances.length, 2);

  // The rejoined channel works on the new connection.
  assert.deepEqual(await room.push("echo", { post: "reconnect" }), { post: "reconnect" });
  socket.disconnect();
});

test("in-flight pushes reject with DisconnectedError on a drop — never hang", async () => {
  const socket = makeSocket({ pushTimeoutMs: 5_000 });
  await socket.connect();
  const room = socket.channel("room:7");
  await room.join();

  const pending = room.push("silent");
  MockWebSocket.current.drop();
  await assert.rejects(pending, DisconnectedError);
  socket.disconnect();
});

test("failed re-dials keep backing off until one lands", async () => {
  const socket = makeSocket();
  await socket.connect();
  await socket.channel("room:7").join();

  MockWebSocket.nextBehaviors = ["refuse", "refuse", "accept"];
  MockWebSocket.current.drop();
  await sleep(80);

  assert.equal(socket.state, "connected");
  assert.equal(socket.channel("room:7").joined, true);
  // 1 initial + 2 refused + 1 success.
  assert.equal(MockWebSocket.instances.length, 4);
  socket.disconnect();
});

test("an exhausted reconnect policy closes the socket", async () => {
  const socket = makeSocket({
    reconnectDelayMs: exponentialBackoff({ initialMs: 5, maxAttempts: 2 }),
  });
  await socket.connect();
  MockWebSocket.nextBehaviors = ["refuse", "refuse", "refuse"];
  MockWebSocket.current.drop();
  await sleep(80);

  assert.equal(socket.state, "closed");
  assert.equal(MockWebSocket.instances.length, 3); // initial + 2 attempts
});

test("disconnect() is terminal and sends flight:close; connect() starts fresh", async () => {
  const socket = makeSocket();
  await socket.connect();
  const ws = MockWebSocket.current;
  socket.disconnect();
  await sleep(20);

  const closeFrame = JSON.parse(ws.sent.at(-1));
  assert.equal(closeFrame.event, "flight:close");
  assert.equal(socket.state, "closed");
  assert.equal(MockWebSocket.instances.length, 1); // no reconnect

  await socket.connect();
  assert.equal(socket.state, "connected");
  socket.disconnect();
});

test("a left channel is not rejoined after a drop", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:7");
  await room.join();
  await room.leave();

  MockWebSocket.current.drop();
  await sleep(30);

  assert.equal(socket.state, "connected");
  assert.equal(room.joined, false);
  const joinFrames = MockWebSocket.current.sent
    .map((text) => JSON.parse(text))
    .filter((frame) => frame.event === "flight:join");
  assert.equal(joinFrames.length, 0);
  socket.disconnect();
});

test("exponential backoff: doubling, capped, optionally bounded", () => {
  const delay = exponentialBackoff({ initialMs: 100, maxMs: 1_000, maxAttempts: 6 });
  assert.equal(delay(1), 100);
  assert.equal(delay(2), 200);
  assert.equal(delay(3), 400);
  assert.equal(delay(4), 800);
  assert.equal(delay(5), 1_000);
  assert.equal(delay(6), 1_000);
  assert.equal(delay(7), null);
});

test("undecodable server frames are dropped, not fatal", async () => {
  const socket = makeSocket();
  await socket.connect();
  const room = socket.channel("room:1");
  await room.join();

  MockWebSocket.current.receive("not json at all");
  await tick();
  assert.equal(socket.state, "connected");
  assert.deepEqual(await room.push("echo", { ok: 1 }), { ok: 1 });
  socket.disconnect();
});

test("server-initiated flight:close is a terminal, graceful teardown", async () => {
  const socket = makeSocket();
  await socket.connect();
  MockWebSocket.current.receive({ ref: null, topic: "flight", event: "flight:close", payload: {} });
  await sleep(20);

  assert.equal(socket.state, "closed");
  assert.equal(MockWebSocket.instances.length, 1); // no reconnect
});
