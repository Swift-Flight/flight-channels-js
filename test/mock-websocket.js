// A scriptable WebSocket double implementing the subset of the browser API
// the client uses (constructor, send, close, onopen/onmessage/onclose), plus
// a tiny in-memory Flight Channels server speaking the wire protocol —
// enough to test the client's protocol behavior without a network.

export class MockWebSocket {
  /** @param {string} url */
  constructor(url) {
    this.url = url;
    this.sent = [];
    this.readyState = 0; // CONNECTING
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    MockWebSocket.instances.push(this);
    const behavior = MockWebSocket.nextBehaviors.shift() ?? "accept";
    queueMicrotask(() => {
      if (behavior === "refuse") {
        this.readyState = 3;
        this.onclose?.({ code: 1006 });
      } else {
        this.readyState = 1;
        this.onopen?.({});
      }
    });
  }

  send(text) {
    if (this.readyState !== 1) throw new Error("send on non-open socket");
    this.sent.push(text);
    MockWebSocket.onSend?.(this, text);
  }

  close() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.onclose?.({ code: 1000 }));
  }

  /** Server side: deliver one frame to the client. */
  receive(envelope) {
    this.onmessage?.({ data: typeof envelope === "string" ? envelope : JSON.stringify(envelope) });
  }

  /** Server side: drop the connection abruptly (no client close()). */
  drop() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.onclose?.({ code: 1006 }));
  }

  static reset() {
    MockWebSocket.instances = [];
    MockWebSocket.nextBehaviors = [];
    MockWebSocket.onSend = null;
  }

  static get current() {
    return MockWebSocket.instances[MockWebSocket.instances.length - 1];
  }
}
MockWebSocket.instances = [];
MockWebSocket.nextBehaviors = [];
MockWebSocket.onSend = null;

/**
 * Wires MockWebSocket.onSend to a scripted server: joins succeed with
 * `initialState`, "echo" replies with its payload, "silent" never replies,
 * "fail" answers flight:error, heartbeats ack — mirroring the Swift test
 * fixtures so both clients are proven against the same server behavior.
 */
export function scriptServer({ initialState = { count: 0 }, rejectTopics = [] } = {}) {
  MockWebSocket.onSend = (ws, text) => {
    const { ref, topic, event, payload } = JSON.parse(text);
    const reply = (event, payload) =>
      queueMicrotask(() => ws.receive({ ref, topic, event, payload }));

    switch (event) {
      case "flight:join":
        if (rejectTopics.includes(topic)) reply("flight:error", { reason: "forbidden" });
        else reply("flight:reply", initialState);
        return;
      case "flight:leave":
      case "flight:heartbeat":
        if (ref != null) reply("flight:reply", {});
        return;
      case "flight:close":
        return;
      case "echo":
        if (ref != null) reply("flight:reply", payload);
        return;
      case "fail":
        reply("flight:error", { reason: "boom" });
        return;
      case "silent":
        return;
      default:
        return;
    }
  };
}
